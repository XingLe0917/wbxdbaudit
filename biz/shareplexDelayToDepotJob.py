import time
import cx_Oracle
import logging
from datetime import datetime, timedelta
from common.config import Config
from biz.wbxjob import WbxJob

logger = logging.getLogger("dbaudit")

class shareplexDelayToDepotJob(WbxJob):
    def start(self):
        config = Config()
        self.config = config
        self.depotdb_url = self.config.dbconnectionDict["depotdb_url"]
        self.depotdb_username = self.config.dbconnectionDict["depotdb_username"]
        self.depotdb_password = self.config.dbconnectionDict["depotdb_password"]
        self.influxdbip, self.influx_database, self._client = config.getInfluxDB_SJC_client()
        self.opdb_db_host_list = {}
        for i in range(3):
            self.main()
            time.sleep(15)

    def main(self):
        # sql ='''
        # select last(delayinsecond),*  from sp_delay_ora2pg where src_db_name='RACAAWEB' and port='60061'   and time > now() - 5m group by src_db_name,tgt_db_name,queuename,port;
        # '''
        # sql = '''
        #         select last(delayinsecond),*  from sp_delay where time > now() - 2m and src_db_name= 'RACDTWEB' and tgt_db_name = 'RACUWEB' group by src_db_name,tgt_db_name,direction,splex_port;
        #         '''
        sql = '''
                select last(delayinsecond),*  from wbxdb_monitor_sp_delay where time > now() - 2m group by src_db_name,tgt_db_name,direction,splex_port;
                '''
        results = self._client.query(sql)
        logger.info("res:%s" %(len(results.raw['series'])))
        dataToUpdate = []
        for r1 in results.raw['series']:
            item = dict(r1)
            print(item)
            direction = item['tags']['direction']
            splex_port = item['tags']['splex_port']
            src_db = item['tags']['src_db_name']
            tgt_db = item['tags']['tgt_db_name']
            values = item['values']
            time = values[0][0]
            logtime = datetime.strptime(time, '%Y-%m-%dT%H:%M:%SZ')
            delayinsecond = int(values[0][5])
            accepttime = logtime + + timedelta(seconds=delayinsecond)
            tgt_host = values[0][7]
            if ".webex.com" in tgt_host:
                tgt_host = str(tgt_host).split(".webex.com")[0]
            tgt_trim_host = tgt_host[0:len(tgt_host)-1]+"%"
            src_trim_host = values[0][9]
            if src_trim_host and ".webex.com" not in src_trim_host:
                # print(item)
                src_trim_host = src_trim_host+"%"
                dataToUpdate.append((logtime,accepttime, splex_port, src_db,tgt_trim_host,direction, tgt_db,src_trim_host))

        try:
            constr = "%s/%s@%s" % (self.depotdb_username,self.depotdb_password,self.depotdb_url)
            srcconn = cx_Oracle.connect(constr)
            srccsr = srcconn.cursor()
            sql = "update wbxadbmon set lastreptime=:1,montime=:2 where port=:3 and src_db=:4 and tgt_host like :5 and replication_to=:6 and tgt_db=:7 and src_host like :8 "
            logger.info(sql)
            # print(sql)
            # print(dataToUpdate)
            srccsr.executemany(sql, dataToUpdate)
            srcconn.commit()
        except Exception as e:
            logger.error(e)


if __name__ == '__main__':
    job = shareplexDelayToDepotJob()
    job.initialize()
    job.start()