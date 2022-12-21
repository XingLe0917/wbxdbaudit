
import logging
from common.config import Config
from concurrent.futures.thread import ThreadPoolExecutor

from common.cxOracle import CXOracle

PASSWORD_EXTRA_DB=['RACAFWEB','TTA35','TTA136','RACAM1MMP','RACFRRPT']


from biz.wbxjob import WbxJob
from dao.wbxauditdbdao import wbxauditdbdao

logger = logging.getLogger("dbaudit")


class ShareplexAdbmonNew(WbxJob):
    def start(self):
        self.src_dbs = ""
        self.tgt_dbs = ""
        self.type = self.args[0]
        if len(self.args) > 1:
            self.src_dbs = self.args[1]
        if len(self.args) > 2:
            self.tgt_dbs = self.args[2]
        logger.info("input src_db:{0}".format(self.src_dbs))
        logger.info("input tgt_db:{0}".format(self.tgt_dbs))
        logger.info("input type:{0}".format(self.type))
        self.auditdao = wbxauditdbdao(self.depot_connectionurl)
        config = Config()
        self._client = config.getInfluxDBclientNew()
        self.wbxadbmon = {}
        self.all_splexinfo = {}
        self.depotdb_url = config.getDepotConnectionurl()
        self.main()

    def main(self):
        all_db_tns = []
        shareplexinfos = []
        wbxadbmons = []
        pwds = []
        try:
            self.auditdao.connect()
            self.auditdao.startTransaction()
            all_db_tns = self.auditdao.getDBTnsInfoForCI()
            shareplexinfos = self.auditdao.getShareplexinfos(self.src_dbs, self.tgt_dbs)
            wbxadbmons = self.auditdao.getWbxadbmons()
            pwds = self.auditdao.getDBPwdInfo()
            self.auditdao.commit()
        except Exception as e:
            self.auditdao.rollback()
            logger.error(e)
        finally:
            self.auditdao.close()

        tns_vo = {}
        for vo in all_db_tns:
            item = dict(vo)
            key = str(item['db_name'] + "_" + item['trim_host'])
            value = self.db_connect_str(item['listener_port'],item['service_name'],item['scan_ip1'],item['scan_ip2'],item['scan_ip3'])
            tns_vo[key] = value
        print("tns_vo:{0}" .format(len(tns_vo)))
        source_db_infos = {}
        target_db_infos = {}

        for channel_data in shareplexinfos:
            source_db_key = str(channel_data["src_db"]) +"_"+str(channel_data["src_trim_host"])
            source_db_value = dict(channel_data)
            schema = "SPLEX"+str(channel_data["port"])
            table_name = "splex_monitor_adb"
            src_sql = "update %s.%s SET logtime=sysdate WHERE direction='%s' and src_db='%s' and src_host='%s' " % (
            schema, table_name, channel_data["replication_to"], channel_data["src_db"],channel_data["src_trim_host"])
            source_db_value['username'] = schema
            source_db_value['connstr'] = tns_vo[source_db_key]
            source_db_value['sql'] = src_sql
            if source_db_infos.get(source_db_key, None) is None:
                source_db_infos[source_db_key] = []
            source_db_infos[source_db_key].append(source_db_value)

            target_db_key = str(channel_data["tgt_db"])+"_"+channel_data["tgt_trim_host"]
            target_db_value = dict(channel_data)
            tgt_sql = "select direction,src_host,src_db,logtime,port_number from %s.%s where src_host='%s' and port_number = %s and direction ='%s' " % (
                schema, table_name, channel_data["src_trim_host"], channel_data["port"],
                channel_data["replication_to"])
            target_db_value['username'] = schema
            target_db_value['connstr'] = tns_vo[target_db_key]
            target_db_value['sql'] = tgt_sql
            if target_db_infos.get(target_db_key, None) is None:
                target_db_infos[target_db_key] = []
            target_db_infos[target_db_key].append(target_db_value)

        print("source_db_channel_infos:%s" %(len(source_db_infos)))
        print("target_db_channel_infos:%s" % (len(target_db_infos)))

        # update src db
        for (db, value) in source_db_infos.items():
            self.update_srcdb(db,value)

        # query tgt db
        query_result = []
        for (db, value) in target_db_infos.items():
            query_result = self.query_tgt_logintime_job(db, value)

        # update abdmon
        for item in query_result:
            print(item)



    def update_srcdb(self,db,datas):
        print(db)
        username = "system"
        password = "sysnotallow"
        connstr = datas[0]['connstr']
        for data in datas:
            sql = data['sql']
            cx_Oracle = CXOracle(username,password,connstr)
            result = cx_Oracle.execute(sql)
            print(result)

    def query_tgt_logintime_job(self,db, datas):
        print(db)
        query_result = []
        username = "system"
        password = "sysnotallow"
        connstr = datas[0]['connstr']
        for data in datas:
            sql = data['sql']
            cx_Oracle = CXOracle(username, password, connstr)
            result = cx_Oracle.select(sql)
            query_result.append(result)
        return query_result

    def db_connect_str(self,listener_port,service_name,scan_ip1,scan_ip2,scan_ip3):
        service_name = "%s.webex.com" % service_name
        value = '(DESCRIPTION ='
        if scan_ip1:
            value = '%s (ADDRESS = (PROTOCOL = TCP)(HOST = %s)(PORT = %s))' % (value, scan_ip1, listener_port)
        if scan_ip2:
            value = '%s (ADDRESS = (PROTOCOL = TCP)(HOST = %s)(PORT = %s))' % (value, scan_ip2, listener_port)
        if scan_ip3:
            value = '%s (ADDRESS = (PROTOCOL = TCP)(HOST = %s)(PORT = %s))' % (value, scan_ip3, listener_port)
        value = '%s (LOAD_BALANCE = yes) (CONNECT_DATA = (SERVER = DEDICATED)(SERVICE_NAME = %s)(FAILOVER_MODE =(TYPE = SELECT)(METHOD = BASIC)(RETRIES = 3)(DELAY = 5))))' % (
        value, service_name)
        return value

    def test(self):
        self.auditdao = wbxauditdbdao(self.depot_connectionurl)
        self.src_dbs = "IDPRDTX"
        self.tgt_dbs = ""
        self.type = "update"
        config = Config()
        self.depotdb_url = config.getDepotConnectionurl()
        self.main()

if __name__ == '__main__':
    job = ShareplexAdbmonNew()
    job.initialize()
    job.test()