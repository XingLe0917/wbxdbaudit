import logging
import threading
import socket
from db.wbxdbconnection import wbxdbconnection
from common.wbxexception import wbxConnectionException
from common.wbxutil import wbxutil
from common.config import Config
from common.singleton import Singleton

logger = logging.getLogger("dbprocess")

@Singleton
class wbxdbmanager:
    _instance_lock = threading.Lock()

    # singleton model also execute __init__ function each object
    def __init__(self):
        self._dbpool = {}

    def getDBPool(self):
        return self._dbpool

    def getDefaultDatabase(self):
        return self.getDatabase("DEFAULT")

    def getDatabase(self, db_name):
        if db_name in self._dbpool:
            return self._dbpool[db_name]
        else:
            return self.newDatabase(db_name)

    def newDatabase(self, db_name):
        jobmanagerconfig = Config()
        logger.info("jobmanagerconfig id=%s" % id(jobmanagerconfig))
        if db_name == "DEFAULT":
            if db_name not in self._dbpool:
                username, pwd, connectionurl = jobmanagerconfig.getDepotConnectionurl()
                depotdb = DepotDB(username, pwd, connectionurl)
                self._dbpool["DEFAULT"] = depotdb
        else:
            if db_name not in self._dbpool:
                logging.info("Get DB connection info for db_name=%s" % db_name)
                depotdb = self._dbpool["DEFAULT"]
                conn = csr = None
                try:
                    conn,csr = depotdb.getConnnection()
                    depotdb.startTransaction(conn)
                    row = depotdb.getDBConnectionInfo(csr, db_name, jobmanagerconfig.getShareplexMonitorEnv())
                    connectionurl = row[5]
                    db_env = row[3]
                    appln_support_code = row[2]
                    application_type = row[4]

                    if connectionurl is None:
                        raise wbxdbconnection("The db %s does not exist with db_type=%s" % (db_name, jobmanagerconfig.getShareplexMonitorEnv()))
                    db = BusinessDB(db_name, db_env, appln_support_code, application_type, "system", "sysnotallow", connectionurl)
                    self._dbpool[db_name] = db
                    depotdb.commit(conn)
                except Exception as e:
                    depotdb.rollback(conn)
                    raise e
                finally:
                    depotdb.close(conn, csr)
        return self._dbpool[db_name]

    def getDBInfoList(self):
        dbinfoList = []
        for db_name, db in self._dbpool.items():
            if db_name != "DEFAULT":
                dbinfoList.append(db.getDBInfo())
        return dbinfoList


class DepotDB(wbxdbconnection):
    def __init__(self, username, pwd, connectionurl):
        super(DepotDB, self).__init__(username, pwd, connectionurl)

    def getDatabaseList(self, host_name):
        conn = csr = None
        try:
            conn, csr = self.getConnnection()
            self.startTransaction(conn)
            SQL = '''
            select di.db_vendor, di.db_type, di.appln_support_code, di.db_name,hi.site_code
            from instance_info ii, database_info di, host_info hi
            where ii.host_name=:host_name
            and ii.trim_host=di.trim_host
            and ii.db_name=di.db_name
            and hi.host_name=ii.host_name
            and di.db_type in ('PROD','BTS_PROD')
            '''
            paramdict = {"host_name": host_name}
            rows = self.queryAll(csr, SQL, paramdict)
            self.commit(conn)
            return rows
        except Exception as e:
            self.rollback(conn)
            logger.error("deleteJobInstance failed", exc_info=e)
        finally:
            self.close(conn, csr)
        return []

    def getDBConnectionInfo(self, csr, db_name, db_type):
        SQL= '''
select distinct db.trim_host, db.db_name, db.appln_support_code,db.db_type, db.application_type,       
 '(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST='||hi.scan_ip1||')(PORT='||db.listener_port||                      
 '))(ADDRESS=(PROTOCOL=TCP)(HOST='||hi.scan_ip2||')(PORT='||db.listener_port||                                 
 '))(ADDRESS=(PROTOCOL=TCP)(HOST='||hi.scan_ip3||')(PORT='||db.listener_port||                                 
 '))(LOAD_BALANCE=yes)(FAILOVER=on)(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME='||db.service_name||          
 '.webex.com)(FAILOVER_MODE=(TYPE=SELECT)(METHOD=BASIC)(RETRIES=3)(DELAY=5))))' as connectionstring            
from database_info db, instance_info ii, host_info hi                                                                            
where db.trim_host=ii.trim_host  
AND db.db_name=ii.db_name
AND ii.trim_host=hi.trim_host
AND ii.host_name=hi.host_name
AND db.db_name = '%s' 
AND db.db_type = '%s'
AND upper(db.db_vendor)='ORACLE'
and db.db_type<> 'DECOM'
        ''' % (db_name, db_type)
        res = self.queryOne(csr, SQL)
        # Can only get by index, can not by column name directly
        # return res["connectionstring"]
        return res

    def getShareplexPortList(self, csr, db_name, db_type):
        SQL = '''select si.src_host,si.src_db,sdi.appln_support_code,si.tgt_host,si.tgt_db,tdi.appln_support_code , si.port,si.qname
from shareplex_info si, instance_info sii, database_info sdi, instance_info tii, database_info tdi
where si.src_host=sii.host_name
and si.src_db=sii.db_name
and si.tgt_host=tii.host_name
and si.tgt_db=tii.db_name
and sii.trim_host=sdi.trim_host
and sii.db_name=sdi.db_name
and tii.trim_host=tdi.trim_host
and tii.db_name=tdi.db_name
and tdi.db_type=:db_type
and sdi.db_type=:db_type
and (si.src_db=:db_name or si.tgt_db=:db_name)'''
        paramdict = {"db_type": db_type,"db_name":db_name}
        resPortList = self.queryAll(csr, SQL, paramdict)
        return resPortList

    def updatejobinstance(self, jobparam):
        conn = csr = None
        try:
            conn, csr = self.getConnnection()
            self.startTransaction(conn)
            SQL = "SELECT jobid, templateid, host_name, jobname, commandstr, jobruntime, status, errormsg FROM wbxjobinstance WHERE host_name=:host_name AND commandstr=:commandstr"
            paramdict = {"host_name": jobparam["host_name"], "commandstr": jobparam["commandstr"]}
            res = self.queryOneAsDict(csr, SQL, paramdict)
            if res is None:
                insertSQL = "INSERT INTO wbxjobinstance(jobid, templateid, host_name, jobname, job_type, job_level, commandstr, jobruntime, status,errormsg) VALUES(SYS_GUID(), :templateid, :host_name, :jobname, :job_type, :job_level, :commandstr, :jobruntime, :status, :errormsg)"
                self.insertOne(csr, insertSQL, jobparam)
            self.commit(conn)
        except Exception as e:
            self.rollback(conn)
            logger.error("deleteJobInstance failed", exc_info=e)
        finally:
            self.close(conn, csr)

    def startJobInstance(self, jobid, next_run_time):
        conn = csr = None
        try:
            conn, csr = self.getConnnection()
            self.startTransaction(conn)
            SQL = "UPDATE wbxjobinstance SET status='RUNNING', last_run_time=:last_run_time, next_run_time=:next_run_time WHERE jobid=:jobid"
            paramdict = {"jobid": jobid, "last_run_time": wbxutil.getcurrenttime(), "next_run_time":next_run_time}
            self.update(csr, SQL, paramdict)
            self.commit(conn)
        except Exception as e:
            self.rollback(conn)
            logger.error("deleteJobInstance failed", exc_info=e)
        finally:
            self.close(conn, csr)

    def updatejobinstancestatus(self,jobid, status, errormsg):
        conn = csr = None
        try:
            conn, csr = self.getConnnection()
            self.startTransaction(conn)
            SQL = "UPDATE wbxjobinstance SET status=:status, errormsg=:errormsg WHERE jobid=:jobid and status not in ('PAUSE','DELETED')"
            paramdict = {"status": status, "jobid": jobid, "errormsg": errormsg}
            self.update(csr, SQL, paramdict)
            self.commit(conn)
        except Exception as e:
            self.rollback(conn)
            logger.error("updatejobinstancestatus failed", exc_info=e)
        finally:
            self.close(conn, csr)

class BusinessDB(wbxdbconnection):
    def __init__(self, db_name, db_env, appln_support_code,application_type, username, pwd, connectionurl):
        super(BusinessDB, self).__init__(username, pwd, connectionurl)
        self._db_name = db_name
        self._host_name = socket.gethostname().split(".")[0]
        self._db_env = db_env
        self._appln_support_code = appln_support_code
        self._application_type = application_type

    def getDBInfo(self):
        return {"DB_NAME":self._db_name,"DB_ENV":self._db_env,"APPLN_SUPPORT_CODE":self._appln_support_code,"APPLICATION_TYPE": self._application_type,
                "USERNAME": self._userame,"PASSWORD":self._pwd,"CONNECTIONFINO":self._connectionurl,"DB_HOST":self._host_name}

    #This is database level job
    def getMetricDatainQuater(self, csr):
        sampleTime = wbxutil.getCurrentTimeAsStringForKibana()
        SQL = '''select ta.tablespace_name, round(ta.totalsize/1024/1024/1024,2) as totalsize, round(tb.usedsize/1024/1024/1024,2) as usedsize,round(tb.usedsize/ta.totalsize*100) usageratio
from (
select tablespace_name, sum(decode(maxbytes,0,user_bytes, maxbytes)) totalsize
from dba_data_files
group by tablespace_name
) ta,
(select tablespace_name, sum(bytes) usedsize
from dba_segments
group by tablespace_name ) tb
where ta.tablespace_name=tb.tablespace_name'''
        rows = self.queryAll(csr, SQL, None)
        tblist = []
        for row in rows:
            tblist.append({"DB_TABLESPACE_NAME":row[0],"DB_TABLESPACE_TOTALSIZE":row[1],
                           "DB_TABLESPACE_USEDSIZE":row[2],"DB_TABLESPACE_USEDRATIO":row[3],
                           "DB_HOST":self._host_name,"DB_METRICS_TYPE":"DB-TABLESPACE","DB_ENV":self._db_env,
                           "DB_NAME":self._db_name,"DB_SAMPLE_DATE":sampleTime})
        return tblist

    # This is instance level job
    def getMetricDatainMinute(self, csr):
        sampleTime = wbxutil.getCurrentTimeAsStringForKibana()
        SQL = '''select count(1) total_count, sum(decode(status,'ACTIVE',1,0)) active_count from v$session where type != 'BACKGROUND' '''

        rows = self.queryAll(csr, SQL, None)
        tblist = []
        for row in rows:
            tblist.append(
                {"DB_SESSION_COUNT": row[0], "DB_ACTIVE_SESSION_COUNT": row[1],
                 "DB_HOST":self._host_name,"DB_METRICS_TYPE":"DB-USERSESSION","DB_ENV":self._db_env,
                 "DB_NAME":self._db_name,"DB_SAMPLE_DATE": sampleTime})

        SQL = '''select 'DB_METRIC_'||upper(replace(stat_name,' ','_')), value from v$sys_time_model'''
        rows = self.queryAll(csr, SQL, None)
        metricdict = {"DB_HOST":self._host_name,"DB_METRICS_TYPE":"DB_SYS_TIME_MODEL","DB_ENV":self._db_env,"DB_NAME":self._db_name,"DB_SAMPLE_DATE": wbxutil.getCurrentTimeAsStringForKibana()}
        for row in rows:
            metricdict[row[0]] = row[1]
        tblist.append(metricdict)

        SQL = '''select begin_time, 'DB_METRIC_'||upper(replace(metric_name,' ','_')),round(value,2) from v$sysmetric where begin_time > sysdate-1/60/24'''
        rows = self.queryAll(csr, SQL, None)
        metricdict = {"DB_HOST": self._host_name, "DB_METRICS_TYPE": "DB_SYS_METRIC", "DB_ENV": self._db_env,"DB_NAME": self._db_name}
        if len(rows) > 0:
            metricdict["DB_SAMPLE_DATE"] = wbxutil.convertDateTimeToStringForES(rows[0][0])
        for row in rows:
            metricdict[row[1]] = row[2]
        tblist.append(metricdict)

        return tblist

    # This is database level job
    def getMetricDatainHour(self,csr):
        SQL = '''select start_time, end_time, trunc((end_time - start_time)*24*60*60) cost_time_in_second, status, input_bytes, output_bytes, 
                        round(input_bytes/trunc((end_time - start_time)*24*60*60)) backup_speed, input_type
                 from v$rman_backup_job_details 
                 where start_time between trunc(sysdate-1/24,'hh24') and trunc(sysdate,'hh24') 
                 and status in ('COMPLETED','FAILED','COMPLETED WITH WARNINGS','COMPLETED WITH ERRORS') '''

        rows = self.queryAll(csr, SQL, None)
        tblist = []
        for row in rows:
            tblist.append(
                {"DB_RMAN_COST_TIME_IN_SECOND": row[2], "DB_RMAN_STATUS": row[3],"DB_RMAN_INPUT_BYTES":row[4],
                 "DB_RMAN_OUTPUT_BYTES":row[5],"DB_RMAN_BACKUP_SPEED":row[6],"DB_RMAN_INPUT_TYPE":row[7],
                 "DB_HOST": self._host_name, "DB_METRICS_TYPE": "DB-RMANBACKUP", "DB_ENV": self._db_env,
                 "DB_NAME": self._db_name, "DB_SAMPLE_DATE": row[0]})

        SQL = ''' select substr(inst.host_name,0,instr(inst.host_name,'.')-1),trunc(first_time, 'hh24') as logtime, count(*), round(sum(blocks * block_size) / 1024 / 1024) mbsize
                from v$archived_log a, gv$instance inst
                where a.FIRST_TIME between trunc(sysdate-1/24,'hh24') and trunc(sysdate,'hh24')
                and a.thread#=inst.instance_number
                group by inst.host_name,trunc(first_time, 'hh24')
                order by 1 '''
        rows = self.queryAll(csr, SQL, None)

        for row in rows:
            metricdict = {"DB_HOST": row[0], "DB_METRICS_TYPE": "DB_ARCHIVELOG", "DB_ENV": self._db_env,
                          "DB_NAME": self._db_name, "DB_SAMPLE_DATE": wbxutil.convertDateTimeToStringForES(row[1])}
            tblist.append(metricdict)

        return tblist

    # This is database level job
    def getMetricDataDaily(self, csr):
        tblist = []
        resdict = {"DB_HOST": self._host_name, "DB_METRICS_TYPE": "DB_DAILYDATA", "DB_ENV": self._db_env,
                 "DB_NAME": self._db_name, "DB_SAMPLE_DATE": wbxutil.convertDateTimeToStringForES()}
        if self._appln_support_code == "WEB":
            SQL = '''select count(1), sum(case when s.active=1 and usr.reglevel=1 then 1 else 0 end)
                    from test.wbxuser usr, test.wbxsite s
                    where s.siteid=usr.siteid '''
            userRow = self.queryOne(csr, SQL, None)
            resdict["DB_TOTAL_USER_COUNT"] = userRow[0]
            resdict["DB_ACTIVE_USER_COUNT"] = userRow[1]
            SQL=''' select count(1), sum(case when s.active=1 then 1 else 0 end) 
                    from test.wbxsite s, test.wbxsitewebdomain swd, test.wbxdatabaseversion ver
                    where swd.domainid=ver.webdomainid
                    and swd.siteid=s.siteid '''
            siteRow = self.queryOne(csr, SQL, None)
            resdict["DB_TOTAL_SITE_COUNT"] = siteRow[0]
            resdict["DB_ACTIVE_SITE_COUNT"] = siteRow[1]

            SQL = '''select count(1), sum(case when s.active=1 then 1 else 0 end)
                    from test.mtgconference mtg, test.wbxsite s
                    where mtg.siteid=s.siteid '''
            mtgRow = self.queryOne(csr, SQL, None)
            resdict["DB_TOTAL_MEETING_COUNT"] = mtgRow[0]
            resdict["DB_ACTIVE_MEETING_COUNT"] = mtgRow[1]

        SQL=''' select trunc(last_start_date), job_name,extract(day from last_run_duration)*24 * 60 * 60 + extract(hour from last_run_duration)* 60 * 60 + extract(minute from last_run_duration)* 60 + round(extract(second from last_run_duration),2) duration_in_second 
                from dba_scheduler_jobs 
                where job_name like 'GATHER_STATS%'
                and last_start_date between trunc(sysdate-1) and trunc(sysdate) '''
        statjobList = self.queryAll(csr, SQL, None)
        for statjob in statjobList:
            resdict[statjob[1]] = statjob[2]

        tblist.append(resdict)
        return tblist


