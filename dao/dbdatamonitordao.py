from dao.wbxdao import wbxdao
import math
class dbdatamonitordao(wbxdao):

    def __init__(self, connectionurl):
        super(dbdatamonitordao, self).__init__(connectionurl)

    def getPasscodeAllocationLog(self):
        vSQL = "SELECT count(1) as errorcount FROM WBXALLOCATEPASSCODEMONITOR  WHERE ACTION IN ('MonitorPasscodeError','AllocatePasscodeError','AllocateRangeJobError') AND CREATETIME BETWEEN SYSDATE-7 AND SYSDATE"
        row = self.session.execute(vSQL).first()
        return row.errorcount

    def getMeetingUUIDDataWithDifferentConfID(self):
        SQL = "select count(1) as mtgcount from test.wbxmeetinguuidmap where confid<>joinconfid or joinconfid is null"
        row = self.session.execute(SQL).first()
        case5 = row.mtgcount

        SQL = " select count(1) as mtgcount " \
              " from test.wbxcalendar a," \
              "      test.wbxmeetinguuidmap b," \
              "      test.wbxsite c," \
              "      test.wbxsitewebdomain d," \
              "      test.wbxdatabaseversion e" \
              " where a.siteid=b.siteid(+) and a.eventid=b.confid(+)" \
              " and  a.eventtype=0" \
              " and a.siteid=c.siteid and c.active=1 and c.siteid not in (512665,573002,314178,12351279,12351263,12351259,12352979,12358687)" \
              " and c.siteid=d.siteid and d.domainid=e.webdomainid" \
              " and ((a.serviceid in (6,7,9) and b.confid IS NULL) or (a.serviceid = 1 and ( a.mtguuid<>b.mtguuid or b.mtguuid is null)))"
        row = self.session.execute(SQL).first()
        case1 = row.mtgcount

        SQL = " select count(1) as mtgcount " \
              " from test.mtgconference a," \
              "      test.wbxmeetinguuidmap b," \
              "      test.wbxsite c," \
              "      test.wbxsitewebdomain d," \
              "      test.wbxdatabaseversion e" \
              " where a.siteid=b.siteid(+) and a.confid=b.confid(+)" \
              " and a.siteid=c.siteid and c.active=1 and c.siteid not in (512665,573002,314178,12351279,12351263,12351259,12352979,12358687)" \
              " and c.siteid=d.siteid and d.domainid=e.webdomainid" \
              " and ((a.companyid in (6,7,9) and b.confid IS NULL) or (a.companyid = 1 and ( a.mtguuid<>b.mtguuid or b.mtguuid is null)))"
        row = self.session.execute(SQL).first()
        case2 = row.mtgcount

        SQL = " select count(1) as mtgcount " \
              " from test.mtgconference a, " \
              "      test.wbxsite c," \
              "      test.wbxsitewebdomain d," \
              "      test.wbxdatabaseversion e" \
              " where a.companyid=1" \
              " and a.siteid=c.siteid and c.active=1 and c.siteid not in (512665,573002,314178,12351279,12351263)" \
              " and c.siteid=d.siteid and d.domainid=e.webdomainid" \
              " and a.mtguuid is null"
        row = self.session.execute(SQL).first()
        case3 = row.mtgcount

        return (case1, case2, case3, 0, case5)

    def getEDRLog(self):
        SQL= """
        select to_char(tc.starttime,'YYYY-MM-DD hh24:mi:ss'), ta.pkseq, ta.mydbschema, ta.mydbname, ta.mydbip, ta.active, tc.status,tc.description, round((sysdate-tc.starttime)*24*60*60) duration
        from (
        select pkseq, mydbschema,mydbname,mydbip,active, starttime, rowid from (
        SELECT c.scriptseq as pkseq, mydbschema,mydbname,mydbip, a.active, c.starttime, c.rowid, row_number() over (partition by c.scriptseq order by c.starttime desc) rownumb
        FROM test.wbxschscripts a,test.wbxschmysqldb b, test.wbxschlog c
        WHERE c.scriptseq = a.pkseq
        and c.starttime >= sysdate - 1/24
        AND a.mydbid=b.mydbid AND name='SchIntfMO' AND b.active=1
        order by c.scriptseq, c.starttime desc
        ) where rownumb=1) ta, test.wbxschlog tc
        where ta.rowid=tc.rowid
        and (nvl(tc.endtime, tc.starttime) < sysdate - 10/60/24)
        and (sysdate - tc.modifytime >= 10/1440)
        """
        # SQL = "select * from test.wbxschlog where scriptseq=2005901 and starttime>=sysdate-1/24 order by starttime desc"
        rows = self.session.execute(SQL).fetchall()
        result = []
        for row in rows:
             result.append({
                 "starttime": row[0],
                 "duration(M)": math.ceil(row[8]/ 60),
                 "pkseq": row[1],
                 "mydbschema": row[2],
                 "mydbname": row[3],
                 "mydbip": row[4],
                 "active": int(row[5]) if row[5].isdigit() else row[5],
                 "status": row[6],
                 "description": row[7].replace("\n", " ") if row[7] else row[7]
             })
        return result

    def getEDRErrorLog(self):
        SQL= """
         select to_char(tc.starttime,'YYYY-MM-DD hh24:mi:ss'), ta.pkseq, ta.mydbschema, ta.mydbname, ta.mydbip, tc.status,tc.description, round((sysdate-tc.starttime)*24*60*60) duration
        from (
        select pkseq, mydbschema,mydbname,mydbip, starttime, rowid from (
        SELECT c.scriptseq as pkseq, mydbschema,mydbname,mydbip, c.starttime, c.rowid, row_number() over (partition by c.scriptseq order by c.starttime desc) rownumb
        FROM test.wbxschscripts a,test.wbxschmysqldb b, test.wbxschlog c
        WHERE c.scriptseq = a.pkseq
        and c.starttime >= sysdate - 1/24
        AND a.mydbid=b.mydbid AND name='SchIntfMO' AND b.active=1
        order by c.scriptseq, c.starttime desc
        ) where rownumb=1) ta, test.wbxschlog tc
        where ta.rowid=tc.rowid
        and (tc.status='Terminated' or (tc.status='MySQL' and description like '%Communications link failure%'))
        """
        # SQL = "select * from test.wbxschlog where scriptseq=2005901 and starttime>=sysdate-1/24 order by starttime desc"
        rows = self.session.execute(SQL).fetchall()
        result = []
        for row in rows:
             result.append({
                 "starttime": row[0],
                 "duration(M)": math.ceil(row[7]/ 60),
                 "pkseq": row[1],
                 "mydbschema": row[2],
                 "mydbname": row[3],
                 "mydbip": row[4],
                 "status": row[5],
                 "description": row[6].replace("\n", " ") if row[6] else row[6]
             })
        return result

    def getDBWaitEvent(self,db_name,cur):
        vSQL = """
         select '%s' as db_name,
            a.INST_ID,
            a.event,
            a.osuser,
            a.machine,
            a.sid,
            b.program,
            a.username,
            nvl(a.sql_id,0) as sql_id,
            a.SQL_EXEC_START,                 
            sysdate as monitor_time,
            round(sysdate - nvl(a.SQL_EXEC_START,sysdate),4) * 24 * 60 as duration
        from gv$session a, gv$process b
        where a.paddr = b.addr
        and a.INST_ID = b.INST_ID
        and (a.TYPE <> 'BACKGROUND')
        and a.username not in ('SYS','SYSTEM')
        and event='SQL*Net break/reset to client'
        and a.STATUS = 'ACTIVE'
        --and (sysdate-a.SQL_EXEC_START)*24 * 60 >= 0
        and (sysdate-a.SQL_EXEC_START)*24 * 60 >= 5
        """ %(db_name)
        rows = cur.execute(vSQL).fetchall()
        return rows

    def getDBExcuteTimeOneHourMore(self, cur, db_name):
        vSQL = """
        select distinct '%s' as db_name,a.sid ,a.sql_id,a.username, a.machine,a.osuser,trunc((sysdate-a.SQL_EXEC_START)*60*24) as duration,
    c.sql_text, sysdate as monitortime
        from gv$session a, gv$process b, gv$sqlarea c
        where a.paddr = b.addr
        and a.INST_ID = b.INST_ID
        and (a.TYPE <> 'BACKGROUND')
        and a.username not in ('SYS','SYSTEM')
        and a.STATUS = 'ACTIVE'
        and a.inst_id=c.inst_id
        and a.sql_id=c.sql_id
        and trunc((sysdate-a.SQL_EXEC_START)* 24) > 1
        and c.sql_text not like '%%PKGDELOBSOLETEMTG%%'
        and replace(a.machine,'.webex.com','') not in ('gsftx1rpl001','gsftx1rpl002','gsftx1rpl003','gsftx1rpl004','gsbtx1rs05','gsbtx1rs03','gsbtx1rs04','gsbtx1rs06','gsbam1rs03','gsbam1rs07','gsbam1rs04','asj1rpl015','asj1rpl014','sfsj1rpl005','sfsj1rpl006','asj1rpl005','asj1rpl006','asj1rpl007','asj1rpl001','asj1rpl002','asj1rpl003','asj1rpl004','asj1rpl008','sfsj1rpl001','sfsj1rpl002','gsbta1bk06','gsbta1bk05','gsbta1rs15','gsbta1rs14','gsbto1bk03','gsblhrbk01','gsbsg1bk08','gsbsg1bk05','gsbsy1rs01','gsbsy1rs02','abt2bk001','gabt2bk001')
        order by a.username,a.sid,a.sql_id
        """ % (db_name)
        rows = cur.execute(vSQL).fetchall()

        return rows

    def wbxschmysqldb(self,locations=None):
        SQL = "select mydbip,mydbport,mydbuser,mydbpassword,mydbschema,location,mydbname from TEST.WBXSCHMYSQLDB where active=1 and not regexp_like(mydbschema,'af|df') "
        SQL = SQL +"and location in (%s) "%locations if locations else SQL
        rows = self.session.execute(SQL).fetchall()
        res = [dict(zip(row.keys(), row)) for row in rows]
        return res

    def wbxgatherdbbackupinfo(self):
        SQL = """
            SELECT distinct  a.SET_STAMP,a.recid,
             DECODE (B.INCREMENTAL_LEVEL,
                     '', DECODE (BACKUP_TYPE, 'L', 'Archivelog', 'Full'),
                     1, 'Incr-1',
                     0, 'Incr-0',
                     B.INCREMENTAL_LEVEL) BACKUP_TYPE,        
             A.START_TIME START_TIME,
             A.COMPLETION_TIME end_time,
             A.ELAPSED_SECONDS,
             A.BYTES,
             A.HANDLE path
        FROM GV$BACKUP_PIECE A, GV$BACKUP_SET B WHERE A.SET_STAMP = B.SET_STAMP AND A.DELETED = 'NO' and  a.set_count = b.set_count
        ORDER BY A.SET_STAMP desc,A.START_TIME DESC,A.HANDLE
        """
        rows = self.session.execute(SQL).fetchall()
        res = [dict(zip(row.keys(), row)) for row in rows]
        return res
# (699499146, None, datetime.datetime(2020, 6, 23, 8, 46, 6), datetime.datetime(2020, 6, 23, 8, 46, 23), 2005741, datetime.datetime(2020, 6, 23, 8, 46, 6), datetime.datetime(2020, 6, 23, 8, 46, 23), 4855, 4855, 0, 'Good', None)
# (699498762, None, datetime.datetime(2020, 6, 23, 8, 45, 6), datetime.datetime(2020, 6, 23, 8, 45, 26), 2005741, datetime.datetime(2020, 6, 23, 8, 45, 6), datetime.datetime(2020, 6, 23, 8, 45, 26), 6548, 6548, 0, 'Good', None)
# (699498988, None, datetime.datetime(2020, 6, 23, 8, 44, 6), datetime.datetime(2020, 6, 23, 8, 44, 23), 2005741, datetime.datetime(2020, 6, 23, 8, 44, 6), datetime.datetime(2020, 6, 23, 8, 44, 23), 3935, 3935, 0, 'Good', None)
# (699498898, None, datetime.datetime(2020, 6, 23, 8, 43, 6), datetime.datetime(2020, 6, 23, 8, 43, 23), 2005741, datetime.datetime(2020, 6, 23, 8, 43, 6), datetime.datetime(2020, 6, 23, 8, 43, 23), 4410, 4410, 0, 'Good', None)

    def getwbxcrlognum(self, splex_schema):
        SQL = "select count(1) as crlogcount from %s.wbxcrlog where CONFLICTTIME between trunc(sysdate-1) and trunc(sysdate) " % splex_schema
        row = self.session.execute(SQL).first()
        return row.crlogcount

    def getgatherstats(self):
        SQL = " select job_name, job_action, last_start_date, last_run_duration, repeat_interval from dba_scheduler_jobs where job_name like 'GATHER%' "
        rows = self.session.execute(SQL).fetchall()
        res = [dict(zip(row.keys(), row)) for row in rows]
        return res

    def updateexcutionplanmonitortime(self):
        SQL = ''' update wbxdba.SQL_MULTI_PLAN set monitor_time = sysdate '''
        self.session.execute(SQL)

    def getsqlmultiplanAlert(self):
        SQL = ''' select db_name,sql_id,sql_plan_id,cost_time,time_increase,create_time,modify_time
from wbxdba.sql_multi_plan t
where (db_name, sql_id) in
(select db_name, sql_id
from wbxdba.sql_multi_plan t
WHERE t.problem_label = 'Y' and t.fix_label='N' and t.modify_time > sysdate-7 and t.create_time !=t.modify_time  and t.executions_delta >30)
and t.MONITOR_TIME < t.modify_time
order by db_name, sql_id , cost_time'''
        rows = self.session.execute(SQL).fetchall()
        # res = [dict(zip(row.keys(), row)) for row in rows]
        return rows

    def getsqlmultiplan(self):
        SQL = ''' select *
from wbxdba.sql_multi_plan t
where (db_name, sql_id) in
(select db_name, sql_id
from wbxdba.sql_multi_plan t
WHERE t.problem_label = 'Y' and t.fix_label='N')
order by db_name, sql_id , cost_time '''
        rows = self.session.execute(SQL).fetchall()
        res = [dict(zip(row.keys(), row)) for row in rows]
        return res

    def getstapchangelist(self, oncall_starttime, oncall_endtime):
        SQL = '''
select substr(schstartdate,instr(schstartdate,' ')+1) || ' : ' || changeid || '->|| Failover:[' || failover || ']->|| ' || implementer || '->|| ' || summary from stapuser.STAP_CHANGEDBINFO 
where implementergroup='Production DBA' 
and to_date(schstartdate, 'YYYY-MM-DD hh24:mi:ss') >= to_date('{0}', 'YYYY-MM-DD hh24:mi:ss') and to_date(schstartdate, 'YYYY-MM-DD hh24:mi:ss') <= to_date('{1}', 'YYYY-MM-DD hh24:mi:ss')
and summary like '%RLSE%'
order by schstartdate,changeid'''.format(oncall_starttime, oncall_endtime)
        rows = self.session.execute(SQL).fetchall()
        return [ row[0] for row in rows ]

    def getstapchangereport(self, oncall_starttime, oncall_endtime):
        SQL = '''
select changeid || '->|| ' || implementer || '->|| ' || summary || '->|| ' || statecddisp from stapuser.STAP_CHANGEDBINFO 
where implementergroup='Production DBA' 
and to_date(schstartdate, 'YYYY-MM-DD hh24:mi:ss') >= to_date('{0}', 'YYYY-MM-DD hh24:mi:ss') and to_date(schstartdate, 'YYYY-MM-DD hh24:mi:ss') <= to_date('{1}', 'YYYY-MM-DD hh24:mi:ss')
and summary like '%RLSE%'
order by schstartdate,changeid'''.format(oncall_starttime, oncall_endtime)
        rows = self.session.execute(SQL).fetchall()
        return [ row[0] for row in rows ]
