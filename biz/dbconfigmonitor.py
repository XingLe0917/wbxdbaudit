import logging
import cx_Oracle
from biz.wbxjob import WbxJob
from dao.wbxauditdbdao import wbxauditdbdao
from concurrent.futures.thread import ThreadPoolExecutor
from concurrent.futures import as_completed

logger = logging.getLogger("dbaudit")


class DBConfigMonitor(WbxJob):

    def start(self):
        self.DBFactory = []
        self.auditdao = wbxauditdbdao(self.depot_connectionurl)
        try:
            self.auditdao.connect()
            self.auditdao.startTransaction()
            self.DBFactory = self.auditdao.getDBConnectionList("ALL", "ALL", "ALL", "ALL", "SYSTEM")
            self.auditdao.commit()
        except Exception as e:
            logger.error("DBConfigMonitor met error with %s", str(e))
            self.auditdao.rollback()
        finally:
            self.auditdao.close()

        flist = []
        executor = ThreadPoolExecutor(max_workers=10)
        for dbitem in self.DBFactory:
            # if dbitem.getDBName() == "RACGTWEB":
            flist.append(executor.submit(self.jobHandler, dbitem))
        executor.shutdown(wait=True)

    def jobHandler(self, dbinfo):
        db_name = dbinfo.getDBName()
        trim_host = dbinfo.getTrimHost()
        appln_support_code = dbinfo.getApplnSupportCode()
        print("Start to collect info from %s\n" % db_name)
        try:
            depotconn = cx_Oracle.connect("depot/depot@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=10.252.8.105)(PORT=1701))(ADDRESS=(PROTOCOL=TCP)(HOST=10.252.8.106)(PORT=1701))(ADDRESS=(PROTOCOL=TCP)(HOST=10.252.8.107)(PORT=1701))(LOAD_BALANCE=yes)(FAILOVER=on)(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=auditdbha.webex.com)(FAILOVER_MODE=(TYPE=SELECT)(METHOD=BASIC)(RETRIES=3)(DELAY=5))))")
            depotcsr = depotconn.cursor()
            conn = cx_Oracle.connect(dbinfo.getConnectionurl(), mode=cx_Oracle.SYSDBA)
            csr = conn.cursor()
            self.getLogFileInfo(depotcsr, csr, trim_host, db_name)
            self.getTablespaceInfo(depotcsr, csr, trim_host, db_name)
            self.get_option(depotcsr, csr, trim_host, db_name)
            self.get_database(depotcsr, csr, trim_host, db_name)
            self.get_dba_users(depotcsr, csr, trim_host, db_name)
            self.get_parameter(depotcsr, csr, trim_host, db_name,appln_support_code)
            self.get_dba_profiles(depotcsr, csr, trim_host, db_name)
            self.getLogFileInfo(depotcsr, csr, trim_host, db_name)
            self.getTablespaceInfo(depotcsr, csr, trim_host, db_name)
            self.getDbaStmtAuditOptsInfo(depotcsr, csr, trim_host, db_name)
            self.getDbaAutotaskClientInfo(depotcsr, csr, trim_host, db_name)
            self.getVersionInfo(depotcsr, csr, trim_host, db_name)
            self.getDbaRegistryHistoryInfo(depotcsr, csr, trim_host, db_name)
            self.getDbaRegistryInfo(depotcsr, csr, trim_host, db_name)
            self.getDbaHistWrControlInfo(depotcsr, csr, trim_host, db_name)
            conn.commit()
            depotconn.commit()
        except Exception as e:
            conn.rollback()
            depotconn.rollback()
            logger.error("Execution on db %s fail with error %s" % (dbinfo.getDBName(), str(e)))
        finally:
            csr.close()
            conn.close()
            depotcsr.close()
            depotconn.close()

    def getLogFileInfo(self, depotcsr, csr, trim_host, db_name):
        SQL= "select * from v$logfile"
        csr.execute(SQL)
        _datalist = []
        for row in csr.fetchall():
            _data = [trim_host, db_name]
            _data.extend(row)
            _datalist.append(_data)
        SQL = "INSERT INTO pccp_logfile(trim_host, db_name, group#, status, type, member, is_recovery_dest_file) VALUES (:1,:2,:3,:4,:5,:6,:7)"
        depotcsr.executemany(SQL, _datalist)

        SQL = "select GROUP#,THREAD#,SEQUENCE#,BYTES,BLOCKSIZE,MEMBERS,ARCHIVED,STATUS,FIRST_CHANGE#,FIRST_TIME,NEXT_CHANGE#,NEXT_TIME from v$log"
        csr.execute(SQL)
        _datalist = []
        for row in csr.fetchall():
            _data = [trim_host, db_name]
            _data.extend(row)
            _datalist.append(_data)
        SQL = "INSERT INTO pccp_log(trim_host, db_name, GROUP#,THREAD#,SEQUENCE#,BYTES,BLOCKSIZE,MEMBERS,ARCHIVED,STATUS,FIRST_CHANGE#,FIRST_TIME,NEXT_CHANGE#,NEXT_TIME) VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14)"
        depotcsr.executemany(SQL, _datalist)

    def getTablespaceInfo(self,depotcsr, csr, trim_host, db_name):
        SQL = "select tablespace_name , count(*) as count_num from dba_rollback_segs group by tablespace_name"
        csr.execute(SQL)
        _datalist = []
        for row in csr.fetchall():
            _data = [trim_host, db_name]
            _data.extend(row)
            _datalist.append(_data)
        SQL = "INSERT INTO pccp_dba_rollback_segs(trim_host, db_name, tablespace_name, count_num) VALUES (:1,:2,:3,:4)"
        depotcsr.executemany(SQL, _datalist)

    def getDbaStmtAuditOptsInfo(self, depotcsr, csr, trim_host, db_name):
        SQL= "select * from dba_stmt_audit_opts"
        csr.execute(SQL)
        _datalist = []
        for row in csr.fetchall():
            _data = [trim_host, db_name]
            _data.extend(row)
            _datalist.append(_data)
        SQL = "INSERT INTO pccp_dba_stmt_audit_opts(trim_host, db_name, user_name, proxy_name, audit_option, success, failure) VALUES (:1,:2,:3,:4,:5,:6,:7)"
        depotcsr.executemany(SQL, _datalist)

    def getDbaAutotaskClientInfo(self,depotcsr, csr, trim_host, db_name):
        SQL = "select client_name,status from dba_autotask_client"
        csr.execute(SQL)
        _datalist = []
        for row in csr.fetchall():
            _data = [trim_host, db_name]
            _data.extend(row)
            _datalist.append(_data)
        SQL = "INSERT INTO pccp_dba_autotask_client(trim_host, db_name, client_name,status) VALUES (:1,:2,:3,:4)"
        depotcsr.executemany(SQL, _datalist)

    def getVersionInfo(self,depotcsr, csr, trim_host, db_name):
        SQL = "select * from v$version"
        csr.execute(SQL)
        _datalist = []
        for row in csr.fetchall():
            _data = [trim_host, db_name]
            _data.extend(row)
            _datalist.append(_data)
        SQL = "INSERT INTO pccp_version(trim_host, db_name, banner) VALUES (:1,:2,:3)"
        depotcsr.executemany(SQL, _datalist)

    def getDbaRegistryHistoryInfo(self,depotcsr, csr, trim_host, db_name):
        SQL = "select * from dba_registry_history"
        csr.execute(SQL)
        _datalist = []
        for row in csr.fetchall():
            _data = [trim_host, db_name]
            _data.extend(row)
            _datalist.append(_data)
        SQL = "INSERT INTO pccp_dba_registry_history(trim_host, db_name, action_time, action, namespace, version, id, bundle_series, comments) VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9)"
        depotcsr.executemany(SQL, _datalist)

    def getDbaRegistryInfo(self,depotcsr, csr, trim_host, db_name):
        SQL = "select comp_name, status, substr(version,1,10) as version from dba_registry"
        csr.execute(SQL)
        _datalist = []
        for row in csr.fetchall():
            _data = [trim_host, db_name]
            _data.extend(row)
            _datalist.append(_data)
        # print(_datalist)
        SQL = "INSERT INTO pccp_dba_registry(trim_host, db_name, comp_name, status, version) VALUES (:1,:2,:3,:4,:5)"
        depotcsr.executemany(SQL, _datalist)

    def getDbaHistWrControlInfo(self,depotcsr, csr, trim_host, db_name):
        SQL = "select * from dba_hist_wr_control"
        csr.execute(SQL)
        _datalist = []
        for row in csr.fetchall():
            _data = [trim_host, db_name]
            _data.extend(row)
            _datalist.append(_data)
        # print(_datalist)
        SQL = "INSERT INTO pccp_dba_hist_wr_control(trim_host, db_name, dbid, snap_interval, retention, topnsql) VALUES (:1,:2,:3,:4,:5,:6)"
        depotcsr.executemany(SQL, _datalist)

    def get_option(self,depotcsr, csr, trim_host, db_name):
        SQL = "select parameter,value from v$option where parameter in ('OLAP','Data Mining','Real Application Testing')"
        csr.execute(SQL)
        _datalist = []
        for row in csr.fetchall():
            _data = [trim_host, db_name]
            _data.extend(row)
            _datalist.append(_data)
        SQL = "INSERT INTO pccp_option(trim_host, db_name, parameter,value) VALUES (:1,:2,:3,:4)"
        depotcsr.executemany(SQL, _datalist)

    def get_database(self,depotcsr, csr, trim_host, db_name):
        SQL = '''
        select inst_id,SUPPLEMENTAL_LOG_DATA_MIN,SUPPLEMENTAL_LOG_DATA_PK,SUPPLEMENTAL_LOG_DATA_UI,SUPPLEMENTAL_LOG_DATA_FK,SUPPLEMENTAL_LOG_DATA_ALL 
        from gv$database
        '''
        csr.execute(SQL)
        _datalist = []
        for row in csr.fetchall():
            _data = [trim_host, db_name]
            _data.extend(row)
            _datalist.append(_data)
        SQL = "INSERT INTO pccp_database(trim_host, db_name, inst_id,SUPPLEMENTAL_LOG_DATA_MIN,SUPPLEMENTAL_LOG_DATA_PK,SUPPLEMENTAL_LOG_DATA_UI,SUPPLEMENTAL_LOG_DATA_FK,SUPPLEMENTAL_LOG_DATA_ALL) VALUES (:1,:2,:3,:4,:5,:6,:7,:8)"
        depotcsr.executemany(SQL, _datalist)

    def get_dba_users(self,depotcsr, csr, trim_host, db_name):
        SQL = '''
        select username,user_id,password,account_status,lock_date,expiry_date,default_tablespace,temporary_tablespace,created,profile,
               initial_rsrc_consumer_group,external_name,password_versions,editions_enabled,authentication_type from dba_users 
        where username not like 'SPLEX%' and user_id > 50 and account_status='OPEN'
        '''
        csr.execute(SQL)
        _datalist = []
        for row in csr.fetchall():
            _data = [trim_host, db_name]
            _data.extend(row)
            _datalist.append(_data)
        SQL = '''
        INSERT INTO pccp_dba_users(trim_host, db_name,username,user_id,password,account_status,lock_date,expiry_date,
        default_tablespace,temporary_tablespace,created,profile,initial_rsrc_consumer_group,external_name,
        password_versions,editions_enabled,authentication_type)  
        VALUES (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17)
        '''
        depotcsr.executemany(SQL, _datalist)

    def get_parameter(self,depotcsr, csr, trim_host, db_name,appln_support_code):
        SQL = '''
        select inst_id, name, value
          from gv$parameter
         where isdefault = 'FALSE'
            or name in ('undo_management','undo_retention')
         order by name
        '''
        csr.execute(SQL)
        _datalist = []
        for row in csr.fetchall():
            _data = [trim_host, db_name]
            _data.extend(row)
            _datalist.append(_data)
        SQL = '''
        INSERT INTO pccp_parameter(trim_host, db_name,inst_id, name, value)  
        VALUES (:1,:2,:3,:4,:5)
        '''
        depotcsr.executemany(SQL, _datalist)

    def get_dba_profiles(self,depotcsr, csr, trim_host, db_name):
        SQL = '''select profile,resource_name,resource_type,limit from dba_profiles 
                 where profile in (
                    select profile 
                    from dba_users 
                    where username not like 'SPLEX%' 
                    and user_id > 50 
                    and account_status='OPEN') '''
        csr.execute(SQL)
        _datalist = []
        for row in csr.fetchall():
            _data = [trim_host, db_name]
            _data.extend(row)
            _datalist.append(_data)
        SQL = '''
         INSERT INTO pccp_dba_profiles(trim_host, db_name,profile,resource_name,resource_type,limit)  
         VALUES (:1,:2,:3,:4,:5,:6)
        '''
        depotcsr.executemany(SQL, _datalist)

#     def test(self):
#         self.auditdao = wbxauditdbdao(self.depot_connectionurl)
#         try:
#             self.auditdao.connect()
#             self.auditdao.startTransaction()
#             self.DBFactory = self.auditdao.getDBConnectionList("ALL", "ALL", "ALL", "ALL", "SYSTEM")
#             self.auditdao.commit()
#         except Exception as e:
#             logger.error("DBConfigMonitor met error with %s", str(e))
#             self.auditdao.rollback()
#         finally:
#             self.auditdao.close()
#
#         flist = []
#         executor = ThreadPoolExecutor(max_workers=10)
#         for dbitem in self.DBFactory:
#             # if dbitem.getDBName() == "RACGTWEB":
#             flist.append(executor.submit(self.jobHandler, dbitem))
#         executor.shutdown(wait=True)
# #
# if __name__ == '__main__':
#     job = DBConfigMonitor()
#     job.initialize()
#     job.test()
