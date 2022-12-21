import time
import cx_Oracle
import logging
from datetime import datetime, timedelta
from common.config import Config
from biz.wbxjob import WbxJob
from dao.wbxauditdbdao import wbxauditdbdao

logger = logging.getLogger("dbaudit")

class pgShareplexDelay2DepotJob(WbxJob):
    def start(self):
        config = Config()
        self.config = config
        self.depotdb_url = self.config.dbconnectionDict["depotdb_url"]
        self.depotdb_username = self.config.dbconnectionDict["depotdb_username"]
        self.depotdb_password = self.config.dbconnectionDict["depotdb_password"]
        self.auditdao = wbxauditdbdao(self.depot_connectionurl)
        self.influxdbip, self.influx_database, self._client = config.getInfluxDB_SJC_client()

        self.opdb_db_host_list = {}
        for i in range(3):
            self.main()
            time.sleep(15)
        self.updatewbxadbmon()

    # Take out the data those montime is more than 2 minutes, then update montime is sysdate
    def updatewbxadbmon(self):
        toUpdate = []
        wbxadbmonlist = []
        try:
            self.auditdao.connect()
            self.auditdao.startTransaction()
            wbxadbmonlist = self.auditdao.getwbxadbmonlist(2)
            self.auditdao.commit()
        except Exception as e:
            self.auditdao.rollback()
            print(e)
        finally:
            self.auditdao.close()

        for item in wbxadbmonlist:
            wbxadbmon = dict(item)
            print(wbxadbmon)
            toUpdate.append((wbxadbmon['src_host'], wbxadbmon['src_db'],wbxadbmon['port'],wbxadbmon['replication_to'], wbxadbmon['tgt_host'],wbxadbmon['tgt_db']))

        logger.info("update montime count:%s" % (len(toUpdate)))
        if len(toUpdate) > 0:
            try:
                constr = "%s/%s@%s" % (self.depotdb_username,self.depotdb_password,self.depotdb_url)
                srcconn = cx_Oracle.connect(constr)
                srccsr = srcconn.cursor()
                sql = "update wbxadbmon set montime=sysdate where src_host=:1 and src_db=:2 and port=:3 and replication_to=:4 and tgt_host=:5 and tgt_db =:6 "
                srccsr.executemany(sql, toUpdate)
                srcconn.commit()
            except Exception as e:
                logger.error(e)

    def main(self):
        sql = '''
                        select last(delayinsecond),*  from wbxdb_pg_shareplex_delay where time > now() - 2m group by src_db_name,tgt_db_name,queuename,port;
                        '''
        results = self._client.query(sql)
        logger.info("res:%s" %(len(results.raw['series'])))
        dataToUpdate = []
        for r1 in results.raw['series']:
            item = dict(r1)
            # print(item)
            queuename = item['tags']['queuename']
            splex_port = item['tags']['port']
            src_db = item['tags']['src_db_name']
            tgt_db = item['tags']['tgt_db_name']
            values = item['values']
            time = values[0][0]
            logtime = datetime.strptime(time, '%Y-%m-%dT%H:%M:%SZ')
            delayinsecond = int(values[0][5])
            accepttime = logtime + timedelta(seconds=delayinsecond)
            tgt_host = values[0][7]
            if ".webex.com" in tgt_host:
                tgt_host = str(tgt_host).split(".webex.com")[0]
            tgt_trim_host = tgt_host[0:len(tgt_host)-1]+"%"
            src_trim_host = values[0][9]
            if src_trim_host and ".webex.com" not in src_trim_host:
                # print(item)
                src_trim_host = src_trim_host+"%"
                dataToUpdate.append((logtime,accepttime, splex_port, src_db,tgt_trim_host,queuename, tgt_db,src_trim_host))

        if len(dataToUpdate)>0:
            # logger.info("dataToUpdate count:%s" %(len(dataToUpdate)))
            try:
                constr = "%s/%s@%s" % (self.depotdb_username,self.depotdb_password,self.depotdb_url)
                srcconn = cx_Oracle.connect(constr)
                srccsr = srcconn.cursor()
                sql = "update wbxadbmon set lastreptime=:1,montime=:2 where port=:3 and src_db=:4 and tgt_host like :5 and replication_to=:6 and tgt_db=:7 and src_host like :8 "
                # logger.info(sql)
                # print(sql)
                # print(dataToUpdate)
                srccsr.executemany(sql, dataToUpdate)
                srcconn.commit()
            except Exception as e:
                logger.error(e)
        else:
            logger.info("skip update")



if __name__ == '__main__':
    job = pgShareplexDelay2DepotJob()
    job.initialize()
    job.start()