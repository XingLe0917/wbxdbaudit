import datetime
import logging
import time

from cx_Oracle import DatabaseError
from dateutil.parser import parse
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool

from common.wbxutil import wbxutil
from biz.wbxjob import WbxJob
from dao.wbxauditdbdao import wbxauditdbdao

logger = logging.getLogger("dbaudit")

#shareplex monitor adbmoncheck
class ShareplexAdbmonCheck(WbxJob):
    def start(self):
        self.auditdao = wbxauditdbdao(self.depot_connectionurl)
        if len(self.args) == 1:
            args = str(self.args[0])
            params = args.split("-")
            self.src_db = params[0]
            self.tgt_db = params[1]
            self.port = params[2]
            self.src_host = params[3]
            self.tgt_host = params[4]
            self.replication_to = params[5]
            self.main()
        else:
            logger.info("args invalid")

    def main(self):
        res = self.adbmoncheck(self.port,self.src_db,self.src_host,self.tgt_db,self.tgt_host,self.replication_to)
        logger.info("============ check result: ============")
        logger.info(res)
        return res

    # 1. Test channel status
    # 2. If channel status is ok, get src and tgt abdmon data, otherwise ignore
    # 3. According to the above 4 cases
    #    case 1 : src True,  tgt False  -> sync data from sourcedb
    #    case 2 : src True,  tgt True   -> sync data from sourcedb
    #    case 3 : src False, tgt True   -> delete tgt data, go to case 4
    #    case 4 : src False, tgt False  -> insert data into sourcedb ; query from targetdb; update wbxadbmon table in depotdb
    def adbmoncheck(self,port, src_db, src_host, tgt_db, tgt_host, replication_to):
        res = {"status": "SUCCESS", "errormsg": "", "data": None}
        logger.info(
            "adbmoncheck, port={0},src_db={1},src_host={2},tgt_db={3},tgt_host={4},replication_to={5}".format(
                port, src_db,
                src_host,
                tgt_db,
                tgt_host,
                replication_to))
        # test channel status
        try:
            channel_status = self.test_channel(port, src_db, src_host, tgt_db, tgt_host, replication_to)
            logger.info("====== Test channel status done, channel_status={0} ====== ".format(channel_status))
        except Exception as e:
            res["status"] = "FAILED"
            res["errormsg"] = str(e)
            logger.error(str(e), exc_info=e, stack_info=True)
            return res

        if not channel_status:
            res = self.get_adbMonOne(port, src_db, src_host, tgt_db, tgt_host, replication_to, None)
            return res

        trim_host_src = ''
        trim_host_tgt = ''
        try:
            self.auditdao.connect()
            self.auditdao.startTransaction()
            ls = self.auditdao.getTrimhost(src_host, tgt_host)
            qnames = self.auditdao.getQnames(port, src_db, src_host, tgt_db, tgt_host)
            self.auditdao.commit()
            qname = ""
            for vo in qnames:
                qname = vo['qname']
                if vo['replication_to'] == replication_to:
                    qname = vo['qname']
                    break

            for vo in ls:
                item = dict(vo)
                if (item['host_name'] == src_host):
                    trim_host_src = item['trim_host']
                if (item['host_name'] == tgt_host):
                    trim_host_tgt = item['trim_host']
        except Exception as e:
            self.auditdao.rollback()
            res["status"] = "FAILED"
            res["errormsg"] = "find trim host fail"
            logger.error("find trim host fail", exc_info=e, stack_info=True)
            return res

        logger.info("trim_host_src:" + trim_host_src)
        logger.info("trim_host_tgt:" + trim_host_tgt)
        schemaname = "splex" + str(port)
        table_name = "splex_monitor_adb"
        if qname:
            table_name += "_" + qname

        src_table_name = table_name
        # hardcode target table is splex_monitor_adb_tgt for port 24531
        if str(port) == "24531" and src_db == "RACGDIAG" and tgt_db == "RACGDIAG":
            tgt_table_name = "splex_monitor_adb_tgt"
        else:
            tgt_table_name = table_name
        logger.info("src_table_name:" + src_table_name)
        logger.info("tgt_table_name:" + tgt_table_name)
        src_dbid = "%s" % (src_db)
        tgt_dbid = "%s" % (tgt_db)
        src_schema_password_info = self.auditdao.get_schema_password(schemaname, src_dbid, trim_host_src)
        src_schema_password = ''
        if len(src_schema_password_info) > 0:
            src_schema_password = src_schema_password_info[0]['password']

        tgt_schema_password_info = self.auditdao.get_schema_password(schemaname, tgt_dbid, trim_host_tgt)
        self.auditdao.commit()
        tgt_schema_password = ''
        if len(tgt_schema_password_info) > 0:
            tgt_schema_password = tgt_schema_password_info[0]['password']
        src_vo = self.opt_splex_monitor_abd("select", src_dbid, port, schemaname, src_schema_password, src_table_name,
                                       replication_to, src_db, trim_host_src, None)
        tgt_vo = self.opt_splex_monitor_abd("select", tgt_dbid, port, schemaname, tgt_schema_password, tgt_table_name,
                                       replication_to, src_db, trim_host_src, None)
        logger.info("====== The source and target query results are: src={0},tgt={1} ======".format(len(src_vo), len(tgt_vo)))

        logtime = None
        try:
            if len(src_vo) == 1 and len(tgt_vo) == 0:
                item = dict(src_vo[0])
                logtime = item['logtime']
                self.opt_splex_monitor_abd("insert", tgt_dbid, port, schemaname, tgt_schema_password, tgt_table_name,
                                      replication_to, src_db, trim_host_src, logtime)
            if len(src_vo) == 1 and len(tgt_vo) == 1:
                item = dict(src_vo[0])
                # logtime = item['logtime']
                logtime = wbxutil.gettimestr()
                self.opt_splex_monitor_abd("update", src_dbid, port, schemaname, src_schema_password, src_table_name,
                                      replication_to, src_db, trim_host_src, logtime)
                self.opt_splex_monitor_abd("update", tgt_dbid, port, schemaname, tgt_schema_password, tgt_table_name,
                                      replication_to, src_db, trim_host_src, logtime)
            if len(src_vo) == 0 and len(tgt_vo) == 1:
                self.opt_splex_monitor_abd("delete", tgt_dbid, port, schemaname, tgt_schema_password, tgt_table_name,
                                      replication_to, src_db, trim_host_src, None)
                self.opt_splex_monitor_abd("insert", src_dbid, port, schemaname, src_schema_password, src_table_name,
                                      replication_to, src_db, trim_host_src,
                                      None)
                tgt = self.opt_splex_monitor_abd("select", tgt_dbid, port, schemaname, tgt_schema_password, tgt_table_name,
                                            replication_to, src_db, trim_host_src, None)
                if tgt:
                    logtime = dict(tgt[0])['logtime']
                else:
                    logtime = datetime.datetime.strptime("1970-01-01", '%Y-%m-%d')
            if len(src_vo) == 0 and len(tgt_vo) == 0:
                self.opt_splex_monitor_abd("insert", src_dbid, port, schemaname, src_schema_password, src_table_name,
                                      replication_to, src_db, trim_host_src, None)
                tgt = self.opt_splex_monitor_abd("select", tgt_dbid, port, schemaname, tgt_schema_password, tgt_table_name,
                                            replication_to, src_db, trim_host_src, None)
                if tgt:
                    logtime = dict(tgt[0])['logtime']
                else:
                    logtime = datetime.datetime.strptime("1970-01-01", '%Y-%m-%d')
        except Exception as e:
            res["status"] = "FAILED"
            res["errormsg"] = "check fail, e={0}".format(e)
            logger.error("check fail", exc_info=e, stack_info=True)
            return res
        res = self.get_adbMonOne(port, src_db, src_host, tgt_db, tgt_host, replication_to, logtime)
        return res

    # opt: select/insert/update/delete
    def opt_splex_monitor_abd(self,opt, dbid, port, schemaname, schemapwd, table_name, direction, src_db, trim_host_src,
                              logtime):
        logger.info(
            "opt={0},dbid={1},port={2},schemaname={3},schemapwd={4},table_name={5},direction={6},src_db={7},trim_host_src={8},logtime={9}".format(
                opt, dbid, port, schemaname, schemapwd, table_name, direction, src_db, trim_host_src, logtime))

        count = None
        res = None
        connectionurl = ""
        db_tns_info = self.get_db_tns(dbid)
        if db_tns_info:
            connectionurl = db_tns_info['data']
        try:
            engine = create_engine('oracle+cx_oracle://%s:%s@%s' % (schemaname, schemapwd, connectionurl),
                                   poolclass=NullPool, echo=False)
            connect = engine.connect()
            sql = ""
            if "delete" == opt:
                sql = '''
                       delete from %s.%s WHERE direction='%s' and src_db='%s' and src_host='%s'
                       ''' % (schemaname, table_name, direction, src_db, trim_host_src)
            if "insert" == opt:
                if logtime:
                    sql = "insert into %s.%s (direction,src_host,src_db,logtime,port_number) values ('%s','%s','%s',to_date('%s','YYYY-MM-DD hh24:mi:ss'),%s)" % (
                        schemaname, table_name, direction, trim_host_src, src_db, logtime, port)
                else:
                    sql = "insert into %s.%s (direction,src_host,src_db,logtime,port_number) values ('%s','%s','%s',sysdate,%s)" % (
                        schemaname, table_name, direction, trim_host_src, src_db, port)
            if "update" == opt:
                if logtime:
                    sql = "UPDATE %s.%s SET logtime=to_date('%s','YYYY-MM-DD hh24:mi:ss') WHERE direction='%s' and src_db='%s' and src_host='%s'" % (
                        schemaname, table_name, logtime, direction, src_db, trim_host_src)
                else:
                    sql = "UPDATE %s.%s SET logtime=sysdate WHERE direction='%s' and src_db='%s' and src_host='%s'" % (
                        schemaname, table_name, direction, src_db, trim_host_src)
            if "select" == opt:
                sql = "select * from %s.%s where direction='%s' and src_db='%s' and src_host='%s' " % (
                    schemaname, table_name, direction, src_db, trim_host_src)
            logger.info(sql)
            cursor = connect.execute(sql)
            if "select" == opt:
                res = cursor.fetchall()
                logger.info("res={0}".format(res))
            connect.connection.commit()
        except DatabaseError as e:
            if connect is not None:
                connect.connection.rollback()
            logger.error(e, exc_info=e, stack_info=True)
            raise e
        return res

    def get_adbMonOne(self,port, src_db, src_host, tgt_db, tgt_host, replication_to, logtime):
        res = {"status": "SUCCESS", "errormsg": "", "data": None}
        # daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
        # daoManager = daoManagerFactory.getDefaultDaoManager()
        try:
            # daoManager.startTransaction()
            # depotdbDao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
            self.auditdao.connect()
            self.auditdao.startTransaction()
            self.auditdao.updatewbxadbmon(port, src_db, src_host, tgt_db, tgt_host, replication_to, logtime)
            self.auditdao.commit()
            ls = self.auditdao.getadbmonOne(port, src_db, src_host, tgt_db, tgt_host, replication_to)
            self.auditdao.commit()
            item = dict(ls[0])
            lastreptime = parse(item['lastreptime'])
            montime = parse(item['montime'])
            times = (montime - lastreptime).seconds
            m, s = divmod(times, 60)
            h, m = divmod(m, 60)
            d = (montime - lastreptime).days
            item['lag_by'] = str(d) + ":" + str(h) + ":" + str(m)
            res["data"] = item
        except Exception as e:
            self.auditdao.rollback()
            res["status"] = "FAILED"
            res["errormsg"] = str(e)
            logger.error(str(e), exc_info=e, stack_info=True)
        return res

    def test_channel(self,port, src_db, src_host, tgt_db, tgt_host, replication_to):
        logger.info("====== test_channel ====== ")
        flag = False
        # daoManagerFactory = wbxdaomanagerfactory.getDaoManagerFactory()
        # daoManager = daoManagerFactory.getDefaultDaoManager()
        trim_host_src = ''
        trim_host_tgt = ''
        qname = ""
        try:
            # daoManager.startTransaction()
            # depotdbDao = daoManager.getDao(DaoKeys.DAO_DEPOTDBDAO)
            self.auditdao.connect()
            self.auditdao.startTransaction()
            ls = self.auditdao.getTrimhost(src_host, tgt_host)
            qnames = self.auditdao.getQnames(port, src_db, src_host, tgt_db, tgt_host)
            for vo in qnames:
                qname = vo['qname']
                if vo['replication_to'] == replication_to:
                    qname = vo['qname']
                    break
            self.auditdao.commit()
            for vo in ls:
                item = dict(vo)
                if (item['host_name'] == src_host):
                    trim_host_src = item['trim_host']
                if (item['host_name'] == tgt_host):
                    trim_host_tgt = item['trim_host']
        except Exception as e:
            self.auditdao.rollback()
            logger.error(e, exc_info=e, stack_info=True)
            raise e

        schemaname = "splex" + str(port)
        table_name = "splex_monitor_adb"
        if qname:
            table_name += "_" + qname
        src_table_name = table_name
        # hardcode target table is splex_monitor_adb_tgt for port 24531
        if str(port)=="24531" and src_db == "RACGDIAG" and tgt_db =="RACGDIAG":
            tgt_table_name = "splex_monitor_adb_tgt"
        else:
            tgt_table_name = table_name
        src_dbid = "%s" % (src_db)
        tgt_dbid = "%s" % (tgt_db)
        direction = "checktest"
        src_schema_password_info = self.auditdao.get_schema_password(schemaname, src_dbid, trim_host_src)
        src_schema_password = ''
        if len(src_schema_password_info) > 0:
            src_schema_password = src_schema_password_info[0]['password']
        tgt_schema_password_info = self.auditdao.get_schema_password(schemaname, tgt_dbid, trim_host_tgt)
        tgt_schema_password = ''
        if len(tgt_schema_password_info) > 0:
            tgt_schema_password = tgt_schema_password_info[0]['password']
        try:
            self.opt_splex_monitor_abd("delete", src_dbid, port, schemaname, src_schema_password, src_table_name, direction,
                                  src_db, trim_host_src, None)
            time.sleep(1)
            self.opt_splex_monitor_abd("insert", src_dbid, port, schemaname, src_schema_password, src_table_name, direction,
                                  src_db, trim_host_src, None)
            res = self.opt_splex_monitor_abd("select", tgt_dbid, port, schemaname, tgt_schema_password,tgt_table_name,
                                             direction, src_db, trim_host_src, None)
            num1 = 0
            while len(res) == 0 and num1 < 5:
                time.sleep(1)
                now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                logger.info("Try to query data on target, time={0}".format(now))
                res = self.opt_splex_monitor_abd("select", tgt_dbid, port, schemaname, tgt_schema_password, tgt_table_name,
                                            direction, src_db, trim_host_src, None)
                num1 += 1
            num2 = 0
            while len(res) == 0 and num2 < 5:
                time.sleep(3)
                now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                logger.info("Try to query data on target, time={0}".format(now))
                res = self.opt_splex_monitor_abd("select", tgt_dbid, port, schemaname, tgt_schema_password, tgt_table_name,
                                            direction, src_db, trim_host_src, None)
                num2 += 1
            if len(res) > 0:
                flag = True

            logger.info("====== Clean source test data ======")
            self.opt_splex_monitor_abd("delete", src_dbid, port, schemaname, src_schema_password, src_table_name, direction,
                                  src_db, trim_host_src, None)
            logger.info("====== Clean target test data ======")
            # hardcode target table is splex_monitor_adb_tgt for port 24531
            self.opt_splex_monitor_abd("delete", tgt_dbid, port, schemaname, tgt_schema_password, tgt_table_name, direction,
                                       src_db, trim_host_src, None)
        except Exception as e:
            logger.error(e, exc_info=e, stack_info=True)
            raise e
        return flag

    def get_db_tns(self,db_name):
        retn = {"status": "SUCCESS", "errormsg": "", "data": None}
        try:
            self.auditdao.connect()
            self.auditdao.startTransaction()
            rows = self.auditdao.getOneDBTnsInfo(db_name)
            self.auditdao.commit()
            if len(rows) == 0:
                retn['status'] = 'FAILED'
                retn['errormsg'] = " %s is invalid due to no tns info" % (db_name)
                return retn
            item = rows[0]
            # trim_host = item['trim_host']
            listener_port = item['listener_port']
            service_name = "%s.webex.com" % item['service_name']
            value = '(DESCRIPTION ='
            if item['scan_ip1']:
                value = '%s (ADDRESS = (PROTOCOL = TCP)(HOST = %s)(PORT = %s))' % (
                    value, item['scan_ip1'], listener_port)
            if item['scan_ip2']:
                value = '%s (ADDRESS = (PROTOCOL = TCP)(HOST = %s)(PORT = %s))' % (
                    value, item['scan_ip2'], listener_port)
            if item['scan_ip3']:
                value = '%s (ADDRESS = (PROTOCOL = TCP)(HOST = %s)(PORT = %s))' % (
                    value, item['scan_ip3'], listener_port)
            value = '%s (LOAD_BALANCE = yes) (CONNECT_DATA = (SERVER = DEDICATED)(SERVICE_NAME = %s)(FAILOVER_MODE =(TYPE = SELECT)(METHOD = BASIC)(RETRIES = 3)(DELAY = 5))))' % (
                value, service_name)
            retn['data'] = value
            # retn['trim_host'] = trim_host
            return retn

        except Exception as e:
            self.auditdao.rollback()
            logger.error("get_db_tns error occurred", exc_info=e, stack_info=True)
            retn['status'] = 'FAILED'
            retn['message'] = str(e)
            return retn
        finally:
            self.auditdao.close()

    def test(self):
        self.auditdao = wbxauditdbdao(self.depot_connectionurl)
        self.src_db = "RACGDIAG"
        self.tgt_db = "RACGDIAG"
        self.port = "24531"
        self.src_host = "txdbormt0112"
        self.tgt_host = "txdbormt0112"
        self.replication_to = ""
        self.main()

if __name__ == '__main__':
    job = ShareplexAdbmonCheck()
    job.initialize()
    job.test()