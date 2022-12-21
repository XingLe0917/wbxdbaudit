from sqlalchemy import func, and_

from common.wbxssh import wbxssh
from dao.vo.auditdbvo import wbxdatabase
from dao.vo.dbdatamonitorvo import WebDomainDataMonitorVO, MeetingDataMonitorVO
from dao.vo.dbpatchdeploymonitorvo import DBPatchReleaseVO, DBPatchDeploymentVO
from dao.vo.depotdbvo import DBLinkBaselineVO, DBLinkMonitorResultVO
from dao.vo.wbxcronjobMonitor import wbxcronjobmonitor
from dao.vo.splexparamdetailvo import SplexparamdetailVo
from dao.vo.wbxcrmonitorvo import WbxcrlogVo,WbxcrmonitordetailVo
from dao.wbxdao import wbxdao

class wbxauditdbdao(wbxdao):

    def __init__(self, connectionurl):
        super(wbxauditdbdao, self).__init__(connectionurl)

    def getDBConnectionList(self, db_type, appln_support_code, apptype, dcname, schema_type):
        vSQL = (
                " select distinct ta.trim_host, ta.db_name, ta.appln_support_code,ta.db_type, ta.application_type, ta.wbx_cluster, decode('" + schema_type + "','SYSTEM','SYSTEM:sysnotallow',tb.schema||':'||f_get_deencrypt(password))||'@'||ta.connectionstring as connectinfo  "
                                                                                                                                                             " from (                                                                              "
                                                                                                                                                             " select distinct db.trim_host, db.db_name, db.appln_support_code,db.db_type, db.application_type,db.wbx_cluster,       "
                                                                                                                                                             "  '(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST='||hi.scan_ip1||')(PORT='||db.listener_port||                      "
                                                                                                                                                             "  '))(ADDRESS=(PROTOCOL=TCP)(HOST='||hi.scan_ip2||')(PORT='||db.listener_port||                                 "
                                                                                                                                                             "  '))(ADDRESS=(PROTOCOL=TCP)(HOST='||hi.scan_ip3||')(PORT='||db.listener_port||                                 "
                                                                                                                                                             "  '))(LOAD_BALANCE=yes)(FAILOVER=on)(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME='||db.service_name||          "
                                                                                                                                                             "  '.webex.com)(FAILOVER_MODE=(TYPE=SELECT)(METHOD=BASIC)(RETRIES=3)(DELAY=5))))' as connectionstring            "
                                                                                                                                                             " from database_info db, instance_info ii, host_info hi                                                                            "
                                                                                                                                                             " where db.trim_host=ii.trim_host  "
                                                                                                                                                             " AND db.db_name=ii.db_name"
                                                                                                                                                             " AND ii.trim_host=hi.trim_host"
                                                                                                                                                             " AND ii.host_name=hi.host_name"
                                                                                                                                                             # " AND ii.host_name=hi.host_name AND db.db_name in ('RACGTWEB','TSJ9','RACLTWEB','TSJCOMB1') "
                                                                                                                                                             " AND db.db_name not in ('RACAFWEB','RACFWEB','TTA35','TSJ35','RACINTH','RACFTMMP','TTA136','TSJ136','IDPRDFTA','RACINFRG','IDPRDFSJ','RACINFRA') "
                                                                                                                                                             " AND hi.host_name not in ('tadborbf06','sjdborbf06','tadborbf07','sjdborbf07','tadbth351','tadbth352','sjdbwbf1','sjdbwbf2','sjdbth351','sjdbth352')"
                                                                                                                                                             " AND upper(db.db_vendor)='ORACLE'"
                                                                                                                                                             " and db.db_type<> 'DECOM'")

        if db_type != 'ALL':
            vSQL = vSQL + " AND db.db_type in ('" + db_type + "')"

        if appln_support_code != "ALL":
            vSQL = vSQL + " AND db.appln_support_code in ('" + appln_support_code + "') "

        if apptype != "ALL":
            vSQL = vSQL + " AND db.application_type in ('" + apptype + "') "

        if dcname != "ALL":
            vSQL = vSQL + " AND upper(hi.site_code) like '%" + dcname + "%'"

        vSQL = vSQL + (
            " ) ta, appln_pool_info tb                                                                                       "
            " where ta.trim_host=tb.trim_host                                                                                "
            " and ta.db_name=tb.db_name    "
            " AND tb.schema != 'stap_ro'                                                                                  "
            " and upper(tb.appln_support_code)=decode(upper(ta.appln_support_code),'RPT','TEO',upper(ta.appln_support_code)) "
        )
        if schema_type != "SYSTEM":
            vSQL = vSQL + " AND tb.schematype='" + schema_type + "'"

        rows = self.session.execute(vSQL).fetchall()
        dbvolist = []
        for row in rows:
            dbvo = wbxdatabase(row[0], row[1], row[2], row[3], row[4], row[5], row[6])
            dbvolist.append(dbvo)
        return dbvolist

    def getDBSchemaList(self):
        vSQL = """select ai.trim_host||'_'||ai.db_name, listagg(ai.schema||'_'||upper(ai.appln_support_code)||'_'||ai.schematype||'_'||nvl(mi.mapping_name,nvl(di.wbx_cluster, di.db_name)),',') within group (order by ai.schema||'_'||upper(ai.appln_support_code)||'_'||nvl(mi.mapping_name,nvl(di.wbx_cluster, di.db_name)))
from appln_pool_info ai, database_info di, appln_mapping_info mi
where ai.schematype in ('test', 'app', 'glookup', 'xxrpth','collabmed')
and ai.trim_host=di.trim_host
and ai.db_name=di.db_name
and di.db_type in ('PROD','BTS_PROD')
and di.db_vendor='Oracle'
and di.appln_support_code in ('CONFIG','WEB','TEL','TEO','OPDB','MEDIATE','RPT','LOOKUP')
and di.db_name not in ('RACFWEB','RACAFWEB','TSJ35','TTA35','RACINTH')
and ai.trim_host=mi.trim_host(+)
and ai.db_name = mi.db_name(+)
and ai.schema=mi.schema(+)
group by ai.trim_host||'_'||ai.db_name"""
        schemaList = self.session.execute(vSQL).fetchall()
        schemadict = {}
        for schema in schemaList:
            schemadict[schema[0]] = schema[1]
        return schemadict

    def getWebdomainDataMonitorVO(self, clustername, itemname):
        monitorvo = self.session.query(WebDomainDataMonitorVO).filter(
            and_(WebDomainDataMonitorVO.clustername == clustername,
                 WebDomainDataMonitorVO.itemname == itemname)).first()
        return monitorvo

    def newWebDomainDataMonitorVO(self, cfgvo):
        self.session.add(cfgvo)

    def getMeetingDataMonitorVO(self, trim_host, db_name):
        meetingDatavo = self.session.query(MeetingDataMonitorVO).filter(
            and_(MeetingDataMonitorVO.db_name == db_name, MeetingDataMonitorVO.trim_host == trim_host)).first()
        return meetingDatavo

    def newMeetingDataMonitorVO(self, meetingDatavo):
        self.session.add(meetingDatavo)

    def isDBPatchReleaseExist(self, releasenumber):
        row = self.session.query(func.count(DBPatchReleaseVO.releasenumber)). \
            filter(DBPatchReleaseVO.releasenumber == releasenumber).first()
        return True if row[0] > 0 else False

    def getDBPatchDeploymentvo(self, releasenumber, trim_host, db_name, schemaname):
        deploymentvo = self.session.query(DBPatchDeploymentVO). \
            filter(and_(DBPatchDeploymentVO.releasenumber == releasenumber, DBPatchDeploymentVO.trim_host == trim_host,
                        DBPatchDeploymentVO.db_name == db_name, DBPatchDeploymentVO.schemaname == schemaname)).first()
        return deploymentvo

    def getpreviousRelease(self, appln_support_code, schematype):
        SQL = " select releasenumber " \
              " from (" \
              "     select releasenumber " \
              "     from (" \
              "         select releasenumber, lpad(major_number,4, '0')||lpad(minor_number,4, '0') as curver, createtime,  max(lpad(major_number,4, '0')||lpad(minor_number,4, '0'))  over ()  as maxver " \
              "         from wbxdbpatchrelease " \
              "         where appln_support_code='%s' and schematype='%s'" \
              "         and releasenumber!=15896 " \
              "     ) where curver=maxver" \
              "      order by createtime desc" \
              " ) where rownum=1" % (appln_support_code, schematype)
        row = self.session.execute(SQL).fetchone()
        if row is None:
            return -1
        else:
            return row[0]

    def mergeShareplexBaseline(self, prevreleasenumber, curreleasenumber, src_appln_support_code, src_schematype):
        SQL = " insert into wbxshareplexbaseline(releasenumber, SRC_APPLN_SUPPORT_CODE, src_schematype, SRC_TABLENAME, TGT_APPLN_SUPPORT_CODE, tgt_schematype, TGT_TABLENAME, " \
              "   TABLESTATUS, SPECIFIEDKEY, COLUMNFILTER, SPECIFIEDCOLUMN, CHANGERELEASE, TGT_APPLICATION_TYPE)" \
              " select %s,SRC_APPLN_SUPPORT_CODE, src_schematype, SRC_TABLENAME, TGT_APPLN_SUPPORT_CODE, tgt_schematype, TGT_TABLENAME, " \
              "    TABLESTATUS, SPECIFIEDKEY, COLUMNFILTER, SPECIFIEDCOLUMN, CHANGERELEASE, TGT_APPLICATION_TYPE" \
              " from wbxshareplexbaseline" \
              " where releasenumber=%s" \
              " and SRC_APPLN_SUPPORT_CODE='%s'" \
              " and SRC_SCHEMATYPE='%s'" \
              " and (releasenumber,SRC_APPLN_SUPPORT_CODE,TGT_APPLN_SUPPORT_CODE,SRC_TABLENAME,TGT_TABLENAME) " \
              "     not in (" \
              "          select %s,SRC_APPLN_SUPPORT_CODE,TGT_APPLN_SUPPORT_CODE,SRC_TABLENAME,TGT_TABLENAME" \
              "          from wbxshareplexbaseline" \
              "          where releasenumber=%s" \
              "          and SRC_APPLN_SUPPORT_CODE='%s' " \
              "          and SRC_SCHEMATYPE='%s' " \
              "      )" % (
              curreleasenumber, prevreleasenumber, src_appln_support_code, src_schematype, prevreleasenumber,
              curreleasenumber, src_appln_support_code, src_schematype)
        self.session.execute(SQL)

    def addDBPatchDeployment(self, dbpatchDeploymentvo):
        self.session.add(dbpatchDeploymentvo)

    def addDBPatchRelease(self, dbpatchReleasevo):
        self.session.add(dbpatchReleasevo)

    def addDBPatchSPChange(self, spvo):
        self.session.add(spvo)

    def listScheduledChange(self, trim_host, db_name, starttime):
        deploylist = self.session.query(DBPatchDeploymentVO). \
            filter(and_(DBPatchDeploymentVO.db_name == db_name, DBPatchDeploymentVO.trim_host == trim_host,
                        DBPatchDeploymentVO.change_completed_date.is_(None),
                        DBPatchDeploymentVO.createtime > starttime)). \
            all()
        return deploylist

    def listShutdownJobManagerInstance(self):
        SQL = "select host_name, status, lastupdatetime from wbxjobmanagerinstance where lastupdatetime < sysdate - 3/24/60"
        rows = self.session.execute(SQL).fetchall()
        return rows

    def listFailedJob(self):
        SQL = ''' select jb.host_name, jb.commandstr, jb.last_run_time, jb.next_run_time,jb.jobruntime, sysdate as currenttime, jb.status from wbxjobinstance jb, wbxjobmanagerinstance jbm
                where jb.host_name = jbm.host_name
                and  jbm.status='RUNNING'
                and jbm.lastupdatetime > sysdate-3/60/24
                and (jb.status ='FAILED' or (jb.status ='SUCCEED' and jb.next_run_time < sysdate-5/60/24)) '''
        rows = self.session.execute(SQL).fetchall()
        return rows

    '''
    if justFirstNode is True, then only return first node in the cluster; otherwise return all nodes in the cluster
    '''

    def listOracleServer(self, dc_name="ALL", db_name="ALL", appln_support_code="ALL", justFirstNode=False,
                         db_type="ALL"):
        rn = 1 if justFirstNode else 1000

        SQL = '''
            select distinct ta.host_name, f_get_deencrypt(tb.pwd) pwd from
            (
                select distinct ii.host_name, row_number() over (partition by hi.scan_name order by hi.host_name) rn
                from database_info di, instance_info ii, host_info hi
                where di.trim_host=ii.trim_host
                and di.db_name=ii.db_name
                and di.db_name not in ('RACPSYT')
                and ii.host_name not in ('tadborbf06','sjdborbf06','tadborbf07','sjdborbf07','tadbth351','tadbth352','sjdbwbf1','sjdbwbf2','sjdbth351','sjdbth352')
                and ii.host_name=hi.host_name
                and ii.trim_host=hi.trim_host'''

        if db_type != "ALL":
            SQL = "%s AND di.db_type in (%s)" % (SQL, db_type)
        else:
            SQL = "%s AND di.db_type in ('BTS_PROD','PROD')" % SQL

        if dc_name != "ALL":
            SQL = "%s AND hi.site_code= '%s'" % (SQL, dc_name)
        if db_name != "ALL":
            SQL = "%s AND di.db_name= '%s'" % (SQL, db_name)
        if appln_support_code != "ALL":
            SQL = "%s AND di.appln_support_code= '%s'" % (SQL, appln_support_code)

        SQL = '''%s) ta, host_user_info tb
        where ta.host_name = tb.host_name(+)
        and ta.rn <=  %s
        order by 1
        ''' % (SQL, rn)

        rows = self.session.execute(SQL).fetchall()
        return rows

    def getDbInfo(self):
        SQL = '''
         select distinct hi.host_name,hi.site_code,ui.username, f_get_deencrypt(ui.pwd),decode(w.host_name,NULL,'0','1') is_exist_wbxjobinstance 
          from database_info di, instance_info ii, host_info hi, host_user_info ui,(select distinct host_name from  wbxjobmanagerinstance WHERE status='RUNNING') w 
          where di.db_type in ('PROD','BTS_PROD') 
          and ii.host_name not in ('tadborbf06','sjdborbf06','tadborbf07','sjdborbf07','tadbth351','tadbth352','sjdbwbf1','sjdbwbf2','sjdbth351','sjdbth352','tadbwbf1','tadbwbf2','sjdbormtf03','sjdbormtf04','tadbormtf03','tadbormtf04') 
          and ii.host_name not in ('sjdbormt065','sjdbormt066','iddbormt01','iddbormt02')
          and di.db_name=ii.db_name 
          and di.trim_host=ii.trim_host
          and ii.host_name=hi.host_name 
          and ii.host_name=ui.host_name(+) 
          and ii.host_name = w.host_name(+)
          and di.catalog_db!='FEDERAMP'
        order by hi.site_code 
        '''
        rows = self.session.execute(SQL).fetchall()
        return rows

    def getCronJobStatus(self, host_name):
        SQL = "select count(1) as mtgcount from CRONJOBSTATUS where host_name = '%s'" % (host_name)
        row = self.session.execute(SQL).first()
        if row.mtgcount > 0:
            return True
        else:
            return False

    def insertCronJobStatus(self, host_name, status, is_exist_wbxjobinstance):
        SQL = "insert into cronjobstatus(host_name,status,db_agent_exist) values('%s','%s','%s') " % (
        host_name, status, is_exist_wbxjobinstance)
        self.session.execute(SQL)

    def updateCronJobStatus(self, host_name, status, is_exist_wbxjobinstance):
        SQL = "update cronjobstatus set status = '%s',db_agent_exist= '%s',monitor_time= systimestamp where host_name = '%s' " % (
        status, is_exist_wbxjobinstance, host_name)
        self.session.execute(SQL)

    def deleteInvalidCronJobStatus(self):
        SQL = "delete from CRONJOBSTATUS  where to_date(monitor_time) < sysdate-1 "
        self.session.execute(SQL)

    def getAlertCronbList(self):
        SQL = '''
        select host_name,status,db_agent_exist,to_char(monitor_time,'YYYY-MM-DD hh24:mi:ss') monitor_time 
        from CRONJOBSTATUS 
        --where ((instr(status, 'running') > 0 and db_agent_exist = '1')or(instr(status, 'stopped') > 0 and db_agent_exist = '0'))
        where (instr(status, 'stopped') > 0 or db_agent_exist = '0')
        '''
        rows = self.session.execute(SQL).fetchall()
        return rows

    def getDBTnsInfo(self):
        sql = '''
        with tmp as (
        select distinct di.db_name, di.service_name,di.listener_port,ii.trim_host,hi.scan_ip1,hi.scan_ip2,hi.scan_ip3,hi.host_ip
        from database_info di ,instance_info ii,host_info hi
        where di.db_name= ii.db_name
        and di.trim_host = ii.trim_host
        and ii.host_name = hi.host_name
        and (di.db_type in ('PROD') or di.db_name in ('RACBTW6','BGSBWEB'))
        and hi.scan_ip1 is not null
        order by di.db_name,trim_host
        )
        select * from (
        select tmp.*, row_number() over(PARTITION BY db_name,trim_host order by db_name,trim_host ) as rn from  tmp
        ) where rn = 1 order by db_name
        '''
        rows = self.session.execute(sql).fetchall()
        return rows

    def getDBTnsInfo_2(self):
        sql = '''
        with tmp as (
        select distinct di.db_name, di.service_name,di.listener_port,ii.trim_host,hi.scan_ip1,hi.scan_ip2,hi.scan_ip3
        from database_info di ,instance_info ii,host_info hi
        where di.db_name= ii.db_name
        and di.trim_host = ii.trim_host
        and ii.host_name = hi.host_name
        and (di.db_type in ('PROD') or di.db_name in ('RACBTW6','BGSBWEB'))
        and hi.scan_ip1 is not null
        --and di.db_name not in ('RACFRRPT')
        order by di.db_name,trim_host
        )
        select * from (
        select tmp.*, row_number() over(PARTITION BY db_name,trim_host order by db_name,trim_host ) as rn from  tmp
        ) where rn = 1 order by db_name
        '''
        rows = self.session.execute(sql).fetchall()
        return rows

    def getDBTnsInfoForCI(self):
        sql = '''
                with tmp as (
                select distinct di.db_name, di.service_name,di.listener_port,ii.trim_host,hi.scan_ip1,hi.scan_ip2,hi.scan_ip3
                from database_info di ,instance_info ii,host_info hi
                where di.db_name= ii.db_name
                and di.trim_host = ii.trim_host
                and ii.host_name = hi.host_name
                and (di.db_type in ('PROD') or di.db_name in ('RACBTW6','BGSBWEB'))
                and hi.scan_ip1 is not null
                and di.appln_support_code = 'CI' 
                order by di.db_name,trim_host
                )
                select * from (
                select tmp.*, row_number() over(PARTITION BY db_name,trim_host order by db_name,trim_host ) as rn from  tmp
                ) where rn = 1 order by db_name
                '''
        rows = self.session.execute(sql).fetchall()
        return rows


    def getDBTnsInfo_2_bts(self):
        sql = '''
        with tmp as (
        select distinct di.db_name, di.service_name,di.listener_port,ii.trim_host,hi.scan_ip1,hi.scan_ip2,hi.scan_ip3
        from database_info di ,instance_info ii,host_info hi
        where di.db_name= ii.db_name
        and di.trim_host = ii.trim_host
        and ii.host_name = hi.host_name
        and (di.db_type in ('BTS_PROD') or di.db_name in ('RACBTW6','BGSBWEB','AUDITDB'))
        and hi.scan_ip1 is not null
        order by di.db_name,trim_host
        )
        select * from (
        select tmp.*, row_number() over(PARTITION BY db_name,trim_host order by db_name,trim_host ) as rn from  tmp
        ) where rn = 1 order by db_name
        '''
        rows = self.session.execute(sql).fetchall()
        return rows

    def getDBTnsInfoByDcaas(self):
        sql = '''
        with tmp as (
        select distinct di.db_name, di.service_name,di.listener_port,ii.trim_host,hi.scan_ip1,hi.scan_ip2,hi.scan_ip3
        from database_info di ,instance_info ii,host_info hi
        where di.db_name= ii.db_name
        and di.trim_host = ii.trim_host
        and ii.host_name = hi.host_name
        --and di.db_type in ('PROD')
        and hi.scan_ip1 is not null
        order by di.db_name,trim_host
        )
        select * from (
        select tmp.*, row_number() over(PARTITION BY db_name,trim_host order by db_name,trim_host ) as rn from  tmp
        ) where rn = 1 order by db_name
        '''
        rows = self.session.execute(sql).fetchall()
        return rows

    def getDBPwdInfo(self):
        sql = '''
        select distinct ai.db_name,ai.trim_host, ai.schema, f_get_deencrypt(password) as pwd
        from appln_pool_info ai  where  ai.schema like 'splex%'
        '''
        rows = self.session.execute(sql).fetchall()
        return rows

    def getShareplexinfos(self, src_db, tgt_db):
        sql = '''
        select distinct si.src_db,si.src_host,si.port,si.qname,si.replication_to||nvl2(si.qname,'_'||si.qname, '') replication_to,si.tgt_db,si.tgt_host ,sii.trim_host src_trim_host,tii.trim_host tgt_trim_host
        from shareplex_info si,instance_info sii,instance_info tii,database_info sdi,database_info tdi
        where si.src_db=sii.db_name
        and si.src_host=sii.host_name
        and si.tgt_db= tii.db_name
        and si.tgt_host=tii.host_name
        and sii.db_name=sdi.db_name
        and sii.trim_host=sdi.trim_host
        and tdi.db_name=tii.db_name
        and tdi.trim_host=tii.trim_host
        
        and sdi.db_type in ('PROD')
        and tdi.db_type in ('PROD')
        
        -- and (sdi.db_type in ('PROD') or sdi.db_name='RACBTW6' or sdi.db_name='BGSBWEB')
        -- and (tdi.db_type in ('PROD') or tdi.db_name='BGSBWEB' or tdi.db_name='RACBTW6')
        and si.tgt_db not in ('RACFRRPT')
        and si.port !=24531
        and nvl(length(si.qname),0) <=12
        
        '''

        if src_db:
            src_dbs = src_db.split("_")
            sql += " and si.src_db in ("
            for index, src in enumerate(src_dbs):
                sql += "'" + src + "'"
                if index != len(src_dbs) - 1:
                    sql += ","
            sql += ")"

        if tgt_db:
            tgt_dbs = tgt_db.split("_")
            sql += " and si.tgt_db in ("
            for index, tgt in enumerate(tgt_dbs):
                sql += "'" + tgt + "'"
                if index != len(tgt_dbs) - 1:
                    sql += ","
            sql += ")"
        sql += " order by src_db,port "
        rows = self.session.execute(sql).fetchall()
        return rows

    def getShareplexinfos_bts(self, src_db, tgt_db):
        sql = '''
        select distinct si.src_db,si.src_host,si.port,si.qname,si.replication_to||nvl2(si.qname,'_'||si.qname, '') replication_to,si.tgt_db,si.tgt_host ,sii.trim_host src_trim_host,tii.trim_host tgt_trim_host
        from shareplex_info si,instance_info sii,instance_info tii,database_info sdi,database_info tdi
        where si.src_db=sii.db_name
        and si.src_host=sii.host_name
        and si.tgt_db= tii.db_name
        and si.tgt_host=tii.host_name
        and sii.db_name=sdi.db_name
        and sii.trim_host=sdi.trim_host
        and tdi.db_name=tii.db_name
        and tdi.trim_host=tii.trim_host

        --and sdi.db_type in ('PROD')
        --and tdi.db_type in ('PROD')

        and (sdi.db_type in ('BTS_PROD') or sdi.db_name='RACBTW6' or sdi.db_name='BGSBWEB')
        and (tdi.db_type in ('BTS_PROD') or tdi.db_name='BGSBWEB' or tdi.db_name='RACBTW6')
        and si.port not in (50001)
        and nvl(length(si.qname),0) <=12
        -- sjgrcabt102:
        --and si.src_db not in('IDPRDFSJ','RACAFWEB','IDPRDFTA','RACINFRG','RACAFCSP','RACFTMMP','RACFCSP','RACFMMP','RACFWEB','RACINFRA','TSJ136','TSJ35','TTA35')
        --and si.tgt_db not in('TTA35','TSJ35','RACAFWEB','RACFWEB')
        --sjdbormt0110  
        --and si.src_db not in ('RACAFWEB','TTA35')
        --and si.tgt_db not in('RACAFWEB','TTA35')
        -- order by src_db,port 
        --and si.src_db not in ('RACAFCSP','RACFTMMP','RACAFWEB')
        --and si.tgt_db not in ('RACAFCSP','RACFTMMP','RACAFWEB')
        '''

        if src_db:
            src_dbs = src_db.split("_")
            sql += " and si.src_db in ("
            for index, src in enumerate(src_dbs):
                sql += "'" + src + "'"
                if index != len(src_dbs) - 1:
                    sql += ","
            sql += ")"

        if tgt_db:
            tgt_dbs = tgt_db.split("_")
            sql += " and si.tgt_db in ("
            for index, tgt in enumerate(tgt_dbs):
                sql += "'" + tgt + "'"
                if index != len(tgt_dbs) - 1:
                    sql += ","
            sql += ")"
        sql += " order by src_db,port "
        rows = self.session.execute(sql).fetchall()
        return rows

    def getShareplexinfosByDCaas(self, src_db, tgt_db):
        sql = '''
                select distinct si.src_db,si.src_host,si.port,si.qname,si.replication_to||nvl2(si.qname,'_'||si.qname, '') replication_to,si.tgt_db,si.tgt_host ,sii.trim_host src_trim_host,tii.trim_host tgt_trim_host
                from shareplex_info si,instance_info sii,instance_info tii,database_info sdi,database_info tdi
                where si.src_db=sii.db_name
                and si.src_host=sii.host_name
                and si.tgt_db= tii.db_name
                and si.tgt_host=tii.host_name
                and sii.db_name=sdi.db_name
                and sii.trim_host=sdi.trim_host
                and tdi.db_name=tii.db_name
                and tdi.trim_host=tii.trim_host
                --and sdi.db_type in ('PROD')
                --and tdi.db_type in ('PROD')
                and nvl(length(si.qname),0) <=12
                '''

        if src_db:
            src_dbs = src_db.split("_")
            sql += " and si.src_db in ("
            for index, src in enumerate(src_dbs):
                sql += "'" + src + "'"
                if index != len(src_dbs) - 1:
                    sql += ","
            sql += ")"

        if tgt_db:
            tgt_dbs = tgt_db.split("_")
            sql += " and si.tgt_db in ("
            for index, tgt in enumerate(tgt_dbs):
                sql += "'" + tgt + "'"
                if index != len(tgt_dbs) - 1:
                    sql += ","
            sql += ")"
        sql += " order by src_db,port "
        rows = self.session.execute(sql).fetchall()
        return rows

    def getWbxadbmons(self):
        sql = '''
        select src_db,src_host,port,replication_to,tgt_db,tgt_host,to_char(lastreptime,'yyyy-mm-dd hh24:mi:ss') lastreptime,to_char(montime,'yyyy-mm-dd hh24:mi:ss') montime,src_trim_host,tgt_trim_host from (
            select distinct ta.*,sii.trim_host src_trim_host,tii.trim_host tgt_trim_host
            from wbxadbmon ta, shareplex_info si, database_info sdi, instance_info sii, database_info tdi, instance_info tii
            where ta.src_host=si.src_host
            and ta.src_db=si.src_db
            and ta.tgt_host=si.tgt_host
            and ta.tgt_db=si.tgt_db
            and ta.port=si.port
            and ta.replication_to = si.replication_to||nvl2(qname,'_'||qname, '')
            and si.src_host=sii.host_name
            and si.src_db=sii.db_name
            and sii.db_name=sdi.db_name
            and sii.trim_host=sdi.trim_host
            and si.tgt_host=tii.host_name
            and si.tgt_db=tii.db_name
            and tdi.db_name=tii.db_name
            and tdi.trim_host=tii.trim_host
            
            --and tdi.db_type in ('PROD')
            --and sdi.db_type in ('PROD')
            
            --and (sdi.db_type in ('PROD') or sdi.db_name='RACBTW2')
            --and (tdi.db_type in ('PROD') or tdi.db_name='BGSBWEB' )
            
            and (sdi.db_type in ('PROD') or sdi.db_name='RACBTW6' or sdi.db_name='BGSBWEB')
            and (tdi.db_type in ('PROD') or tdi.db_name='BGSBWEB' or tdi.db_name='RACBTW6')
            
            ) 
            order by src_db ,port
        '''
        rows = self.session.execute(sql).fetchall()
        return rows

    def getWbxadbmons_bts(self):
        sql = '''
        select src_db,src_host,port,replication_to,tgt_db,tgt_host,to_char(lastreptime,'yyyy-mm-dd hh24:mi:ss') lastreptime,to_char(montime,'yyyy-mm-dd hh24:mi:ss') montime,src_trim_host,tgt_trim_host from (
            select distinct ta.*,sii.trim_host src_trim_host,tii.trim_host tgt_trim_host
            from wbxadbmon ta, shareplex_info si, database_info sdi, instance_info sii, database_info tdi, instance_info tii
            where ta.src_host=si.src_host
            and ta.src_db=si.src_db
            and ta.tgt_host=si.tgt_host
            and ta.tgt_db=si.tgt_db
            and ta.port=si.port
            and ta.replication_to = si.replication_to||nvl2(qname,'_'||qname, '')
            and si.src_host=sii.host_name
            and si.src_db=sii.db_name
            and sii.db_name=sdi.db_name
            and sii.trim_host=sdi.trim_host
            and si.tgt_host=tii.host_name
            and si.tgt_db=tii.db_name
            and tdi.db_name=tii.db_name
            and tdi.trim_host=tii.trim_host

            --and tdi.db_type in ('PROD')
            --and sdi.db_type in ('PROD')

            --and (sdi.db_type in ('PROD') or sdi.db_name='RACBTW2')
            --and (tdi.db_type in ('PROD') or tdi.db_name='BGSBWEB' )

            and (sdi.db_type in ('BTS_PROD') or sdi.db_name='RACBTW6' or sdi.db_name='BGSBWEB')
            and (tdi.db_type in ('BTS_PROD') or tdi.db_name='BGSBWEB' or tdi.db_name='RACBTW6')
            ) 
            order by src_db ,port
        '''
        rows = self.session.execute(sql).fetchall()
        return rows

    def getValid_wbxadbmon(self):
        sql = '''
        select src_host,src_db,port,replication_to,tgt_host,tgt_db,
            to_char(lastreptime,'yyyy-mm-dd hh24:mi:ss') lastreptime,
             to_char(montime,'yyyy-mm-dd hh24:mi:ss') montime,
             diff_secend,diff_day,diff_hour,diff_min,(diff_day||':'||diff_hour||':'||diff_min) lag_by,nvl(wbl.lag_by,30) alert_mins,case when temp.diff_secend>nvl(wbl.lag_by,30)*60  then '1' else '0' end alert_flag
             from ( select distinct ta.*,ROUND((ta.montime-ta.lastreptime)*24*60*60) diff_secend,
                        ROUND(TO_NUMBER(ta.montime - ta.lastreptime)) diff_day,
                        trunc(TO_NUMBER(ta.montime - ta.lastreptime)*24)- trunc(TO_NUMBER(ta.montime - ta.lastreptime))*24 diff_hour,
                        trunc(TO_NUMBER(ta.montime - ta.lastreptime)*24*60)-trunc(TO_NUMBER(ta.montime - ta.lastreptime)*24)*60 diff_min,sdi.appln_support_code src_appln_support_code,tdi.appln_support_code tgt_appln_support_code
                        from wbxadbmon ta, shareplex_info si, database_info sdi, instance_info sii, database_info tdi, instance_info tii
                        where ta.src_host=si.src_host
                        and ta.src_db=si.src_db
                        and ta.tgt_host=si.tgt_host
                        and ta.tgt_db=si.tgt_db
                        and ta.port=si.port
                        and ta.replication_to = si.replication_to||nvl2(qname,'_'||qname, '')
                        and si.src_host=sii.host_name
                        and si.src_db=sii.db_name
                        and sii.db_name=sdi.db_name
                        and sii.trim_host=sdi.trim_host
                        and si.tgt_host=tii.host_name
                        and si.tgt_db=tii.db_name
                        and tdi.db_name=tii.db_name
                        and tdi.trim_host=tii.trim_host
                        and tdi.db_type in ('PROD')
                        and sdi.db_type in ('PROD')) temp,wbxadbmonlagby wbl 
             where temp.src_appln_support_code= wbl.src_appln_support_code(+)
              and temp.tgt_appln_support_code = wbl.tgt_appln_support_code(+)
              order by src_db 
        '''
        rows = self.session.execute(sql).fetchall()
        return rows

    def getDBSchemaPwdInfo(self):
        sql = '''
            select distinct di.db_name,di.trim_host ,ai.schema,f_get_deencrypt(password) pwd
            from database_info di,appln_pool_info ai
            where di.db_name = ai.db_name
            and di.db_type in ('PROD')
            and di.db_vendor='Oracle'
            --and di.appln_support_code='WEB'
            and di.db_name not in ('RACAFWEB','RACFWEB','IDPRDFSJ','RACAFWEB','IDPRDFTA','RACINFRG','RACAFCSP','RACFTMMP','RACFCSP',
            'RACFMMP','RACFWEB','RACINFRA','TSJ136','TSJ35','TTA35','FSTAPDB','SJFTOOLS')
            --and di.db_name in ('RACASWEB')
            order by di.db_name
        '''
        rows = self.session.execute(sql).fetchall()
        return rows

    def getWEBDBTnsInfo(self):
        sql = '''
        with tmp as (
        select di.db_name,di.trim_host,di.service_name,hi.host_name,ii.instance_name, di.listener_port ,hi.scan_ip1,hi.scan_ip2,hi.scan_ip3
        from database_info di,instance_info ii,host_info hi
        where di.db_name= ii.db_name
        and di.trim_host = ii.trim_host
        and ii.host_name = hi.host_name
        and hi.scan_ip1 is not null
        and di.db_type in ('PROD')
        and di.appln_support_code='WEB'
        --and di.db_name = 'RACTARPT'
        )
        select * from (
        select tmp.*, row_number() over(PARTITION BY db_name,trim_host order by db_name,trim_host ) as rn from  tmp
        ) where rn = 1 
         and db_name not in ('RACAFWEB','RACFWEB','RACGUWEB','FSTAPDB','RACBGWEB','RACHUWEB','RACAGWEB')
         order by db_name
        '''
        rows = self.session.execute(sql).fetchall()
        return rows

    def getOracleUserPwdByHostname(self, host_name):
        SQL = "select f_get_deencrypt(pwd) from HOST_USER_INFO where host_name='%s' and username='oracle'" % host_name
        row = self.session.execute(SQL).fetchone()
        if row is None:
            return None
        return row[0]

    def inserttelegrafstatus(self, host_name, status, errortime, errorlog):
        SQL = '''
        insert into telegraf_performance_monitor(host_name, status, logrecordtime, logmsg)values('%s', %s, to_date('%s','YYYY-MM-DD HH24:mi:ss'), '%s')
        ''' % (host_name, status, errortime, errorlog.replace("\'", "\""))
        print(SQL)
        self.session.execute(SQL)

    def listWEBDBShareplexPort(self):
        SQL = '''select distinct hi.site_code, si.tgt_host, si.port, si.tgt_splex_sid, f_get_deencrypt(ui.pwd)
    from shareplex_info si, database_info di, instance_info ii, host_info hi, host_user_info ui
    where di.appln_support_code='WEB'
    and di.trim_host=ii.trim_host
    and di.db_name=ii.db_name
    and ii.db_name=si.tgt_db
    and ii.host_name=si.tgt_host
    and di.db_name not in ('RACFWEB','RACAFWEB')
    and ii.host_name=hi.host_name
    and hi.host_name=ui.host_name
    and ui.username='oracle'
    order by 1
            '''
        rows = self.session.execute(SQL).fetchall()
        return rows

    def jobInit(self, jobname):
        hasError = False
        jobmapping = {"send_alert_email": "/u00/app/admin/dbarea/bin/alert_mail_11g.sh",
                      "splex_restart_process": "/u00/app/admin/dbarea/bin/splex_restart_proc.sh",
                      "kill_session": "/u00/app/admin/dbarea/bin/kill_session.sh",
                      "shareplex_monitor_configfile": "/u00/app/admin/dbarea/bin/shareplex_monitor_configfile.sh",
                      "shareplex_monitor_configfile_full": "/u00/app/admin/dbarea/bin/shareplex_monitor_configfile.sh"
                      }

        if jobname in ("send_alert_email", "kill_session"):
            SQL = '''
            begin
              delete from wbxcronjobmonitor where jobname = '%s';
              insert into wbxcronjobmonitor(JOBNAME,JOB_LEVEL,HOST_NAME,COMMANDSTR,params,STATUS,STARTTIME,UPDATETIME)
              select distinct '%s','INSTANCE',ii.host_name,'%s',ii.instance_name,
                 'INITIAL',sysdate,sysdate
                from instance_info ii, database_info di,wbxjobmanagerinstance msg
                where ii.trim_host=di.trim_host and ii.db_name=di.db_name and di.db_type !='DECOM'
                and ii.host_name= msg.host_name
                and not exists (select 1 from wbxjobmonitordbblacklist blk where di.db_name=blk.db_name and di.trim_host=blk.trim_host)
                order by ii.host_name;
            EXCEPTION when others then rollback ;
            end;
            ''' % (jobname, jobname, jobmapping[jobname])
        elif jobname == "splex_restart_process":
            SQL = '''
            begin
              delete from wbxcronjobmonitor where jobname = '%s';
              insert into wbxcronjobmonitor(JOBNAME,JOB_LEVEL,HOST_NAME,COMMANDSTR,params,STATUS,STARTTIME,UPDATETIME)
              select distinct '%s','SHAREPLEXPORT',shx.src_host,'%s',shx.port,
                 'INITIAL',sysdate,sysdate
                from shareplex_info shx,wbxjobmanagerinstance msg
                where shx.src_host=msg.host_name
                and not exists (select 1 from wbxjobmonitorspblacklist blk where shx.port=blk.port and shx.src_host=blk.host_name)
                order by shx.src_host;
            EXCEPTION when others then rollback ;
            end;
            ''' % (jobname, jobname, jobmapping[jobname])
        elif jobname == "shareplex_monitor_configfile":
            SQL = '''
            begin
              delete from wbxcronjobmonitor where jobname = '%s';
              insert into wbxcronjobmonitor(JOBNAME,JOB_LEVEL,HOST_NAME,COMMANDSTR,params,STATUS,STARTTIME,UPDATETIME)
              select distinct '%s','INSTANCE',ii.host_name,'%s',lower(di.db_name)||' INCREMENTAL',
                 'INITIAL',sysdate,sysdate
                from instance_info ii, database_info di,wbxjobmanagerinstance msg
                where ii.trim_host=di.trim_host and ii.db_name=di.db_name and di.db_type !='DECOM'
                and ii.host_name= msg.host_name and di.appln_support_code in ('WEB','TEL','CONFIG','OPDB','MEDIATE')
                and not exists (select 1 from wbxjobmonitordbblacklist blk where di.db_name=blk.db_name and di.trim_host=blk.trim_host)
                order by ii.host_name;
            EXCEPTION when others then rollback ;
            end;
            ''' % (jobname, jobname, jobmapping[jobname])
        elif jobname == "shareplex_monitor_configfile_full":
            SQL = '''
            begin
              delete from wbxcronjobmonitor where jobname = '%s';
              insert into wbxcronjobmonitor(JOBNAME,JOB_LEVEL,HOST_NAME,COMMANDSTR,params,STATUS,STARTTIME,UPDATETIME)
              select distinct '%s','INSTANCE',ii.host_name,'%s',lower(di.db_name)||' FULL',
                 'INITIAL',sysdate,sysdate
                from instance_info ii, database_info di,wbxjobmanagerinstance msg
                where ii.trim_host=di.trim_host and ii.db_name=di.db_name and di.db_type !='DECOM'
                and ii.host_name= msg.host_name and di.appln_support_code in ('WEB','TEL','CONFIG','OPDB','MEDIATE')
                and not exists (select 1 from wbxjobmonitordbblacklist blk where di.db_name=blk.db_name and di.trim_host=blk.trim_host)
                order by ii.host_name;
            EXCEPTION when others then rollback ;
            end;
            ''' % (jobname, jobname, jobmapping[jobname])
        try:
            self.session.execute(SQL)
        except Exception as e:
            print(e)
            hasError = True
        return hasError

    def getCronjobhostbyjob(self, jobname):
        hasError = self.jobInit(jobname)
        hostvolist = []
        if not hasError:
            SQL = '''
            SELECT wbxjob.host_name,max(f_get_deencrypt(pwd)) pwd,commandstr,to_char(listagg(params,',')) params
            FROM wbxcronjobmonitor wbxjob,host_user_info host
            where wbxjob.host_name=host.host_name and jobname = '%s'
            and status not in ('RUNNING','OBSOLETE')
            group by wbxjob.host_name,commandstr
            ''' % (jobname)
            rows = self.session.execute(SQL).fetchall()
            hostvolist = []
            for row in rows:
                dbvo = wbxcronjobmonitor(row[0], row[1], row[2], row[3])
                hostvolist.append(dbvo)
        return hostvolist

    # def getCronjobhostbyjob_tmp(self, jobname):
    #     hasError = self.jobInit(jobname)
    #     hostvolist = []
    #     if not hasError:
    #         SQL = '''
    #         SELECT wbxjob.host_name,max(f_get_deencrypt(pwd)) pwd,commandstr,to_char(listagg(params,',')) params
    #         FROM wbxcronjobmonitor wbxjob,host_user_info host,database_info di
    #         where wbxjob.host_name=host.host_name
    #         and jobname = '%s'
    #         and host.trim_host=di.trim_host
    #         and di.appln_support_code='CONFIG'
    #         and status not in ('RUNNING','OBSOLETE')
    #         group by wbxjob.host_name,commandstr
    #         ''' % (jobname)
    #         rows = self.session.execute(SQL).fetchall()
    #         hostvolist = []
    #         for row in rows:
    #             dbvo = wbxcronjobmonitor(row[0], row[1], row[2], row[3])
    #             hostvolist.append(dbvo)
    #     return hostvolist

    def getRunMyDBInfo(self):
        SQL = """
        select dbname,to_char(listagg(''''||dcname||'''',',')) from wbxmydbcfg
        where paramname = 'WbxMyDBLogPurge' and status = 'VAILD' 
          and key = to_char(sysdate,'hh24') 
         group by dbname
        """
        rows = self.session.execute(SQL).fetchall()
        return rows

    def updateWbxCronJobMonStatus(self, *args):
        SQL = "update wbxcronjobmonitor set status = '%s' ,updatetime = sysdate where jobname = '%s' and host_name = '%s' and params = '%s'"
        SQL = SQL % (args[3], args[0], args[1], args[2])
        self.session.execute(SQL)

    def InitWbxMySqlDBPurgeStatus(self, **kargs):
        SQL = '''
        begin
            delete from wbxmysqldbpurge where mydbname = '%s' and mydbschema = '%s';
            insert into wbxmysqldbpurge(dcname,mydbname,mydbschema,mydbip,status,duration,drows,errormsg) values
            ('%s','%s','%s','%s','%s',%s,%s,'%s');
        EXCEPTION when others then rollback ;
        end;
        ''' % (
        kargs["dbname"], kargs["dbschema"], kargs["dcname"], kargs["dbname"], kargs["dbschema"], kargs["hostname"],
        kargs["status"], kargs["Duration"], kargs["drows"], kargs["errormsg"])
        self.session.execute(SQL)

    def UpdWbxMySqlDBPurgeStatus(self, **kargs):
        SQL = '''
        begin
            UPDATE wbxmysqldbpurge SET status='%s',duration=%s,drows=%s,dnum=%s,errormsg='%s',endtime=to_date('%s','yyyy-MM-dd HH24:mi:ss')
            where mydbname='%s' and mydbschema='%s';
            insert into wbxmysqldbpurge_his(JOBID,DCNAME,MYDBNAME,MYDBSCHEMA,MYDBIP,STATUS,DURATION,DROWS,DNUM,ERRORMSG,STARTTIME,ENDTIME)  
            select JOBID,DCNAME,MYDBNAME,MYDBSCHEMA,MYDBIP,STATUS,DURATION,DROWS,DNUM,ERRORMSG,STARTTIME,ENDTIME 
            from wbxmysqldbpurge where mydbname='%s' and mydbschema = '%s';
        EXCEPTION when others then rollback ;
        end;
        ''' % (kargs["status"], kargs["Duration"], kargs["drows"], kargs["dnum"], kargs["errormsg"].replace('\'', ''),
               kargs["endtime"], kargs["dbname"], kargs["dbschema"], kargs["dbname"], kargs["dbschema"])
        self.session.execute(SQL)

    def getHostListbyAPPCode(self, appln_support_code):
        SQL = '''
            select distinct hi.host_name, f_get_deencrypt(ui.pwd), hi.site_code, db.db_type from host_info hi, instance_info ii, database_info db, host_user_info ui 
            where db.trim_host=ii.trim_host
            AND db.db_name=ii.db_name
            AND ii.trim_host=hi.trim_host
            AND ii.host_name=hi.host_name
            AND upper(db.db_vendor)='ORACLE'
            and db.db_type<> 'DECOM'
            and db.appln_support_code='%s'
            AND db.db_name not in ('RACAFWEB','RACFWEB','TTA35','TSJ35','RACINTH','RACFTMMP','TTA136','TSJ136','IDPRDFTA','RACINFRG','IDPRDFSJ','RACINFRA')
            AND hi.host_name not in ('tadborbf06','sjdborbf06','tadborbf07','sjdborbf07','tadbth351','tadbth352','sjdbwbf1','sjdbwbf2','sjdbth351','sjdbth352')
            and ui.host_name=hi.host_name
            and ui.trim_host=hi.trim_host
            and ui.username='oracle'
                ''' % (appln_support_code.upper())
        rows = self.session.execute(SQL).fetchall()
        hostvolist = []
        for row in rows:
            hostvo = wbxssh(row[0], 22, "oracle", row[1])
            hostvo.site_code = row[2]
            hostvo.db_type = row[3]
            hostvolist.append(hostvo)
        return hostvolist

    def getMonitorDBInfo(self):
        sql = '''
        select distinct a.db_name, 
        lower(b.instance_name)||'('||c.host_name||'.'||c.domain ||')' instance_name
        from database_info a,instance_info b ,host_info c
              where a.db_name=b.db_name  
             and a.trim_host=b.trim_host 
              and b.trim_host=c.trim_host
              and b.host_name=c.host_name
              and a.appln_support_code in ('CONFIG','WEB','OPDB','MEDIATE','TEO','TEL','LOOKUP','DIAGS','CI','CSP','TRANSCRIPT','CALENDAR','MMP')
              and a.db_type in ('PROD','BTS')
              and c.host_name not in ('tadborbf06','sjdborbf06','tadborbf07','sjdborbf07','tadbth351','tadbth352','sjdbwbf1','sjdbwbf2','tadbwbf1','tadbwbf2','sjdbth351','sjdbth352','sjdbormtf03','sjdbormtf04','tadbormtf04','tadbormtf03','sjdborbf08','sjdborbf09')
              and a.catalog_db!='FEDERAMP'
              and c.site_code not in ('BOM01')
              and a.db_name not in ('IDPRDTA1')
              order by db_name,instance_name
        '''
        rows = self.session.execute(sql).fetchall()
        return rows

    def getDropPartitionError(self):
        sql = '''
SELECT db_name,
       trim_host,
       host_name,
       to_char(starttime, 'yyyy-mm-dd hh24:mi:ss'),
       to_char(endtime, 'yyyy-mm-dd hh24:mi:ss'),
       duration,
       status,
       errormsg
  from (
SELECT di.db_name,
               di.trim_host,
               di.appln_support_code,
               dp.host_name,
               dp.starttime,
               dp.endtime,
               ceil(((dp.endtime - dp.starttime)) * 24 * 60) duration,
               NVL(dp.status, 'NOTRUN') as status,
               dp.errormsg
          FROM database_info di, wbxdroppartjob dp
         WHERE di.trim_host = dp.trim_host(+)
           AND di.db_name = dp.db_name(+)
           AND di.appln_support_code in ('CONFIG','WEB','TEL','TEO','OPDB','MEDIATE','CI',
                                         'CSP','DIAGS','CALENDAR','TRANS','LOOKUP','MCT',
                                         'MMP','TOOLS','DIAGNS','AVWATCH','DEPOT')
           and di.catalog_db = 'COMMERCIAL'
           and di.db_type in ('BTS_PROD', 'PROD')
           and NOT EXISTS (SELECT db_name FROM wbxjobmonitordbblacklist wb WHERE wb.trim_host = di.trim_host and wb.db_name=di.db_name )
)
 WHERE starttime < sysdate - 31
    or (status = 'RUNNING' and starttime < sysdate - 1)
    or status in ('NOTRUN', 'FAILED')
'''
        rows = self.session.execute(sql).fetchall()
        rst = []
        for row in rows:
            rst.append({
                "db_name": row[0],
                "trim_host": row[1],
                "host_name": row[2],
                "starttime": row[3],
                "endtime": row[4],
                "duration": row[5],
                "status": row[6],
                "errormsg": row[7]
            })
        return rst

    def getMonitorDBIist(self):
        sql = '''
                select a.db_name, '(DESCRIPTION = (ADDRESS = (PROTOCOL = TCP)(HOST = '|| c.scan_ip1 ||')(PORT = '|| a.listener_port ||'))(ADDRESS = (PROTOCOL = TCP)(HOST = '|| c.scan_ip2 ||')(PORT = '|| a.listener_port ||'))(ADDRESS = (PROTOCOL = TCP)(HOST = '|| c.scan_ip3 ||')(PORT = '|| a.listener_port ||'))(LOAD_BALANCE = yes)(FAILOVER = on)(CONNECT_DATA =(SERVER = DEDICATED)(SERVICE_NAME = '|| a.service_name ||'.webex.com)(FAILOVER_MODE =(TYPE = SELECT)(METHOD = BASIC)(RETRIES = 3)(DELAY = 5)))' connectionurl
                from database_info a,instance_info b ,host_info c 
                where a.appln_support_code in ('CONFIG','WEB','OPDB','MEDIATE','TEO','TEL','LOOKUP','MMP','SYSTOOL','DIAGS','CI','CSP')
                and a.db_name=b.db_name  
                and a.trim_host=b.trim_host 
                and b.trim_host=c.trim_host
                and b.host_name=c.host_name
                and a.db_type in ('PROD','BTS')
                and c.host_name not in ('tadborbf06','sjdborbf06','tadborbf07','sjdborbf07','tadbth351','tadbth352','sjdbwbf1','sjdbwbf2','tadbwbf1','tadbwbf2','sjdbth351','sjdbth352')
                '''
        rows = self.session.execute(sql).fetchall()
        rst = []
        for row in rows:
            rst.append({
                "db_name": row[0],
                "connectionurl": row[1]
            })
        return rst

    def getDBconnectionlist(self, db_type, appln_support_code, apptype, dcname, schema_type):
        vSQL = (
                " select distinct ta.trim_host, ta.db_name, ta.appln_support_code,ta.db_type, ta.application_type, ta.wbx_cluster, decode('" + schema_type + "','SYSTEM','SYSTEM/sysnotallow',tb.schema||':'||f_get_deencrypt(password))||'@'||ta.connectionstring as connectinfo  "
                                                                                                                                                             " from (                                                                              "
                                                                                                                                                             " select distinct db.trim_host, db.db_name, db.appln_support_code,db.db_type, db.application_type,db.wbx_cluster,       "
                                                                                                                                                             "  '(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST='||hi.scan_ip1||')(PORT='||db.listener_port||                      "
                                                                                                                                                             "  '))(ADDRESS=(PROTOCOL=TCP)(HOST='||hi.scan_ip2||')(PORT='||db.listener_port||                                 "
                                                                                                                                                             "  '))(ADDRESS=(PROTOCOL=TCP)(HOST='||hi.scan_ip3||')(PORT='||db.listener_port||                                 "
                                                                                                                                                             "  '))(LOAD_BALANCE=yes)(FAILOVER=on)(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME='||db.service_name||          "
                                                                                                                                                             "  '.webex.com)(FAILOVER_MODE=(TYPE=SELECT)(METHOD=BASIC)(RETRIES=3)(DELAY=5))))' as connectionstring            "
                                                                                                                                                             " from database_info db, instance_info ii, host_info hi                                                                            "
                                                                                                                                                             " where db.trim_host=ii.trim_host  "
                                                                                                                                                             " AND db.db_name=ii.db_name"
                                                                                                                                                             " AND ii.trim_host=hi.trim_host"
                                                                                                                                                             " AND ii.host_name=hi.host_name"
                                                                                                                                                             # " AND ii.host_name=hi.host_name AND db.db_name in ('RACGTWEB','TSJ9','RACLTWEB','TSJCOMB1') "
                                                                                                                                                             " AND db.db_name not in ('RACAFWEB','RACFWEB','TTA35','TSJ35','RACINTH','RACFTMMP','TTA136','TSJ136','IDPRDFTA','RACINFRG','IDPRDFSJ','RACINFRA') "
                                                                                                                                                             " AND hi.host_name not in ('tadborbf06','sjdborbf06','tadborbf07','sjdborbf07','tadbth351','tadbth352','sjdbwbf1','sjdbwbf2','sjdbth351','sjdbth352')"
                                                                                                                                                             " AND upper(db.db_vendor)='ORACLE'"
                                                                                                                                                             " and db.db_type<> 'DECOM'")

        if db_type != 'ALL':
            vSQL = vSQL + " AND db.db_type in ('" + db_type + "')"

        if appln_support_code != "ALL":
            vSQL = vSQL + " AND db.appln_support_code in ('" + appln_support_code + "') "

        if apptype != "ALL":
            vSQL = vSQL + " AND db.application_type in ('" + apptype + "') "

        if dcname != "ALL":
            vSQL = vSQL + " AND upper(hi.site_code) like '%" + dcname + "%'"

        vSQL = vSQL + (
            " ) ta, appln_pool_info tb                                                                                       "
            " where ta.trim_host=tb.trim_host                                                                                "
            " and ta.db_name=tb.db_name    "
            " AND tb.schema != 'stap_ro'                                                                                  "
            " and upper(tb.appln_support_code)=decode(upper(ta.appln_support_code),'RPT','TEO',upper(ta.appln_support_code)) "
        )
        if schema_type != "SYSTEM":
            vSQL = vSQL + " AND tb.schematype='" + schema_type + "'"
        print(vSQL)
        rows = self.session.execute(vSQL).fetchall()
        dbvolist = []
        for row in rows:
            dbvo = wbxdatabase(row[0], row[1], row[2], row[3], row[4], row[5], row[6])
            dbvolist.append(dbvo)
        return dbvolist

    def insert_oracle_sga_pga_info(self, instance_name, db_name, sga_target, sga_max_size, pga_aggregate_target):
        SQL = """
        MERGE INTO wbxoracledbinfo de
        USING (select '%s' instance_name,'%s' db_name,'%s' sga_target,'%s' sga_max_size,'%s' pga_aggregate_target from dual) db
        ON (de.instance_name = db.instance_name and de.db_name = db.db_name)
        when matched then update set de.sga_target= db.sga_target,de.sga_max_size=db.sga_max_size,de.pga_aggregate_target=db.pga_aggregate_target
        when not matched then INSERT (instance_name,db_name,sga_target,sga_max_size,pga_aggregate_target) VALUES (db.instance_name,db.db_name,db.sga_target,db.sga_max_size,db.pga_aggregate_target)
        """ % ( instance_name, db_name, sga_target, sga_max_size, pga_aggregate_target)
        self.session.execute(SQL)

    # def getConfigdbToWebdbShplexList(self, csr, db_name, db_type):
    def getConfigdbToWebdbShplexList(self,delay_min):
            SQL = '''with ta as (
         select src_host,src_db,port,replication_to,tgt_host,tgt_db,src_site_code,tgt_site_code,
         temp.src_splex_sid, temp.tgt_splex_sid,
            to_char(lastreptime,'yyyy-mm-dd hh24:mi:ss') lastreptime,
             to_char(montime,'yyyy-mm-dd hh24:mi:ss') montime,
             diff_secend,diff_day,diff_hour,diff_min,(diff_day||':'||diff_hour||':'||diff_min) lag_by,
             case when temp.diff_secend>nvl(wbl.lag_by,%s)*60  then '1' else '0' end alert_flag
             from ( select distinct ta.*,ROUND((ta.montime-ta.lastreptime)*24*60*60) diff_secend,
                        trunc(TO_NUMBER(ta.montime - ta.lastreptime)) diff_day,
                        trunc(TO_NUMBER(ta.montime - ta.lastreptime)*24)- trunc(TO_NUMBER(ta.montime - ta.lastreptime))*24 diff_hour,
                        trunc(TO_NUMBER(ta.montime - ta.lastreptime)*24*60)-trunc(TO_NUMBER(ta.montime - ta.lastreptime)*24)*60 diff_min,
                        sdi.appln_support_code src_appln_support_code,tdi.appln_support_code tgt_appln_support_code,si.src_splex_sid, si.tgt_splex_sid,
                        shi.site_code src_site_code,thi.site_code tgt_site_code
                        from wbxadbmon ta, shareplex_info si, database_info sdi, instance_info sii, database_info tdi, instance_info tii,host_info shi,host_info thi
                        where ta.src_host=si.src_host
                        and ta.src_db=si.src_db
                        and ta.tgt_host=si.tgt_host
                        and ta.tgt_db=si.tgt_db
                        and ta.port=si.port
                        and ta.replication_to = si.replication_to||nvl2(qname,'_'||qname, '')
                        and si.src_host=sii.host_name
                        and si.src_db=sii.db_name
                        and sii.db_name=sdi.db_name
                        and sii.trim_host=sdi.trim_host
                        and si.tgt_host=tii.host_name
                        and si.tgt_db=tii.db_name
                        and tdi.db_name=tii.db_name
                        and tdi.trim_host=tii.trim_host
                        and sii.host_name = shi.host_name
                        and tii.host_name = thi.host_name
                        and tdi.db_type in ('PROD')
                        and sdi.db_type in ('PROD')
                        and sdi.appln_support_code in ('CONFIG','WEB') 
                        
                        ) temp,wbxadbmonlagby wbl 
             where temp.src_appln_support_code= wbl.src_appln_support_code(+)
              and temp.tgt_appln_support_code = wbl.tgt_appln_support_code(+)
              order by temp.diff_secend desc
        ) 
        select 
        ta.port,ta.src_db,ta.src_host,ta.replication_to,ta.tgt_db,ta.tgt_host,ta.lag_by,ta.lastreptime,ta.montime
        from ta
        where ta.alert_flag = '1'     
        and ta.src_db in ('CONFIGDB','GCFGDB')
        and ta.tgt_db in (select db_name from database_info where appln_support_code = 'WEB')
        and ta.tgt_db not in ('CONFIGDB','GCFGDB','RACAFWEB','RACFWEB')'''% (delay_min)
            rows = self.session.execute(SQL).fetchall()
            return rows

    # def getWebdbToWebdbShplexList(self, csr, db_name, db_type):
    def getWebdbToWebdbShplexList(self, delay_min):
        SQL = '''with ta as (
         select src_host,src_db,port,replication_to,tgt_host,tgt_db,src_site_code,tgt_site_code,
         temp.src_splex_sid, temp.tgt_splex_sid,
            to_char(lastreptime,'yyyy-mm-dd hh24:mi:ss') lastreptime,
             to_char(montime,'yyyy-mm-dd hh24:mi:ss') montime,
             diff_secend,diff_day,diff_hour,diff_min,(diff_day||':'||diff_hour||':'||diff_min) lag_by,
             case when temp.diff_secend>nvl(wbl.lag_by,%s)*60  then '1' else '0' end alert_flag
             from ( select distinct ta.*,ROUND((ta.montime-ta.lastreptime)*24*60*60) diff_secend,
                        trunc(TO_NUMBER(ta.montime - ta.lastreptime)) diff_day,
                        trunc(TO_NUMBER(ta.montime - ta.lastreptime)*24)- trunc(TO_NUMBER(ta.montime - ta.lastreptime))*24 diff_hour,
                        trunc(TO_NUMBER(ta.montime - ta.lastreptime)*24*60)-trunc(TO_NUMBER(ta.montime - ta.lastreptime)*24)*60 diff_min,
                        sdi.appln_support_code src_appln_support_code,tdi.appln_support_code tgt_appln_support_code,si.src_splex_sid, si.tgt_splex_sid,
                        shi.site_code src_site_code,thi.site_code tgt_site_code
                        from wbxadbmon ta, shareplex_info si, database_info sdi, instance_info sii, database_info tdi, instance_info tii,host_info shi,host_info thi
                        where ta.src_host=si.src_host
                        and ta.src_db=si.src_db
                        and ta.tgt_host=si.tgt_host
                        and ta.tgt_db=si.tgt_db
                        and ta.port=si.port
                        and ta.replication_to = si.replication_to||nvl2(qname,'_'||qname, '')
                        and si.src_host=sii.host_name
                        and si.src_db=sii.db_name
                        and sii.db_name=sdi.db_name
                        and sii.trim_host=sdi.trim_host
                        and si.tgt_host=tii.host_name
                        and si.tgt_db=tii.db_name
                        and tdi.db_name=tii.db_name
                        and tdi.trim_host=tii.trim_host
                        and sii.host_name = shi.host_name
                        and tii.host_name = thi.host_name
                        and tdi.db_type in ('PROD')
                        and sdi.db_type in ('PROD')
                        and sdi.appln_support_code in ('CONFIG','WEB')

                        
                        ) temp,wbxadbmonlagby wbl 
             where temp.src_appln_support_code= wbl.src_appln_support_code(+)
              and temp.tgt_appln_support_code = wbl.tgt_appln_support_code(+)
              order by temp.diff_secend desc
        ) 
        select 
        ta.port,ta.src_db,ta.src_host,ta.replication_to,ta.tgt_db,ta.tgt_host,ta.lag_by,ta.lastreptime,ta.montime
        from ta
        where ta.alert_flag = '1'     
        and ta.tgt_db in (select db_name from database_info where appln_support_code = 'WEB')
        and ta.tgt_db not in ('CONFIGDB','GCFGDB','RACAFWEB','RACFWEB')
        and ta.src_db in (select db_name from database_info where appln_support_code = 'WEB')
        and ta.src_db not in ('CONFIGDB','GCFGDB','RACAFWEB','RACFWEB')''' % (delay_min)
        rows = self.session.execute(SQL).fetchall()
        return rows

    # def getWebdbToWebdbShplexList(self, csr, db_name, db_type):
    def getAlldbShplexList(self, delay_min):
        SQL = ''' with ta as (
         select src_host,src_db,port,replication_to,tgt_host,tgt_db,temp.src_splex_sid, temp.tgt_splex_sid,
            to_char(lastreptime,'yyyy-mm-dd hh24:mi:ss') lastreptime,
             to_char(montime,'yyyy-mm-dd hh24:mi:ss') montime,
             diff_secend,diff_day,diff_hour,diff_min,(diff_day||':'||diff_hour||':'||diff_min) lag_by,nvl(wbl.lag_by,10) alert_mins,case when temp.diff_secend>%s*60  then '1' else '0' end alert_flag
             from ( select distinct ta.*,ROUND((ta.montime-ta.lastreptime)*24*60*60) diff_secend,
                        trunc(TO_NUMBER(ta.montime - ta.lastreptime)) diff_day,
                        trunc(TO_NUMBER(ta.montime - ta.lastreptime)*24)- trunc(TO_NUMBER(ta.montime - ta.lastreptime))*24 diff_hour,
                        trunc(TO_NUMBER(ta.montime - ta.lastreptime)*24*60)-trunc(TO_NUMBER(ta.montime - ta.lastreptime)*24)*60 diff_min,sdi.appln_support_code src_appln_support_code,tdi.appln_support_code tgt_appln_support_code,si.src_splex_sid, si.tgt_splex_sid 
                         from wbxadbmon ta, shareplex_info si, database_info sdi, instance_info sii, database_info tdi, instance_info tii
                        where ta.src_host=si.src_host
                        and ta.src_db=si.src_db
                        and ta.tgt_host=si.tgt_host
                        and ta.tgt_db=si.tgt_db
                        and ta.port=si.port
                        and ta.replication_to = si.replication_to||nvl2(qname,'_'||qname, '')
                        and si.src_host=sii.host_name
                        and si.src_db=sii.db_name
                        and sii.db_name=sdi.db_name
                        and sii.trim_host=sdi.trim_host
                        and si.tgt_host=tii.host_name
                        and si.tgt_db=tii.db_name
                        and tdi.db_name=tii.db_name
                        and tdi.trim_host=tii.trim_host
                        --and tdi.db_type in ('PROD')
                        --and sdi.db_type in ('PROD')
                       -- and (tdi.db_type in ('PROD') or (ta.src_db='RACBTW2' ))
                       -- and (sdi.db_type in ('PROD') or (ta.tgt_db='BGSBWEB' ))
                       and (tdi.db_type in ('PROD') or (ta.src_db='RACBTW6' ) or  (ta.src_db='BGSBWEB' ))
                       and (sdi.db_type in ('PROD') or (ta.tgt_db='BGSBWEB' ) or  ((ta.tgt_db='RACBTW6' )))
                       
                        ) temp,wbxadbmonlagby wbl 
             where temp.src_appln_support_code= wbl.src_appln_support_code(+)
              and temp.tgt_appln_support_code = wbl.tgt_appln_support_code(+)
              order by temp.diff_secend desc
        ) 
        select ta.port,ta.src_db,ta.src_host,ta.replication_to,ta.tgt_db,ta.tgt_host,ta.lag_by,ta.lastreptime,ta.montime from ta  where ta.alert_flag = '1' ''' % (delay_min)
        rows = self.session.execute(SQL).fetchall()
        return rows

    def getADBMonDataForKafka(self):
        vSQL = """ select si.src_host, si.src_db, si.port, mon.replication_to, si.tgt_db, si.tgt_host, 
                   to_char(mon.montime,'YYYY-MM-DD hh24:mi:ss'), to_char(mon.lastreptime,'YYYY-MM-DD hh24:mi:ss'), 
                   trunc((mon.montime - mon.lastreptime) * 24 * 60) lag_by
                   from shareplex_info si, wbxadbmon mon
                   where si.src_db=mon.src_db
                   and si.port = mon.port
                   and si.src_host=mon.src_host
                   and si.tgt_db=mon.tgt_db
                   and si.replication_to||nvl2(qname,'_'||qname,qname)=mon.replication_to
                   and si.tgt_splex_sid='KAFKA' 
                   and trunc((mon.montime - mon.lastreptime) * 24 * 60) > 10 """
        rows = self.session.execute(vSQL).fetchall()
        return rows

    def getedrpasswordbyhostname(self, host_name):
        vSQL = """
        select distinct f_get_deencrypt(pwd) from host_user_info where host_name='%s' and username='edrsvr'""" % host_name
        rows = self.session.execute(vSQL).fetchall()
        if not rows:
            raise Exception("cannot find edrsvr password for %s from depot.host_user_info" % host_name)
        return rows[0][0]

    def get_edr_host_map(self):
        vSQL = """
        select distinct host_name, active, alert_db_appln_support_code || '_' || alert_db_db_type db_type from wbx_edr_mapping"""
        rows = self.session.execute(vSQL).fetchall()
        if not rows:
            raise Exception("cannot find edr host map from depot.wbx_edr_mapping")
        result_map = {}
        for row in rows:
            if row[2] not in result_map.keys():
                result_map.update({
                    row[2]: {
                        row[1]: row[0]
                    }
                })
            else:
                if row[1] not in result_map[row[2]].keys():
                    result_map[row[2]].update({
                        row[1]: row[0]
                    })
                else:
                    result_map[row[2]][row[1]] = row[0]
        return result_map

    def get_wbxmonitoralert2(self):
        sql = '''
        select t1.alertid,t1.alerttitle,t1.autotaskid,t1.host_name,t1.db_name,t1.splex_port,t1.instance_name,
        t1.alert_type,t1.job_name,t1.parameter,t1.first_alert_time,t1.last_alert_time,t1.alert_count,t1.attemptcount,t1.fixtime
        from wbxmonitoralert2 t1 
        where status = 'NEW'
        and t1.alert_type = 'INFLUXDB_ISSUE_TASK'
        and t1.first_alert_time > sysdate-7
        '''
        rows = self.session.execute(sql).fetchall()
        return rows

    def addWbxAutoTask(self, taskvo):
        self.session.add(taskvo)

    def addAutoTaskJob(self, jobvo):
        self.session.add(jobvo)

    def updateWbxmonitoralert2(self, alertid, taskid):
        sql = '''
        update wbxmonitoralert2 set status = 'PENDING',autotaskid = '%s' where alertid = '%s'
        ''' % (taskid, alertid)
        self.session.execute(sql)

    def getShplexMonitorServerList(self):
        sql = '''
              select distinct host_name, username, pwd, listagg(port,',') within group (order by 1) over(partition by host_name, username, pwd) as port_list from (
        select distinct ii.host_name,si.port, ui.username, f_get_deencrypt(ui.pwd) as pwd
                            from shareplex_info si, instance_info ii, database_info di, host_user_info ui
                            where ((si.tgt_host=ii.host_name and si.tgt_db=ii.db_name) or (si.src_host=ii.host_name and si.src_db=ii.db_name))
                            and ii.db_name=di.db_name
                            and ii.trim_host=di.trim_host
                            and di.db_type in ('BTS_PROD','PROD')
                            and ii.host_name=ui.host_name
                            and ui.username='oracle'
                            and di.catalog_db='COMMERCIAL' order by 1
        ) order by 1
                        '''
        rows = self.session.execute(sql).fetchall()
        rst = []
        for row in rows:
            rst.append({
                "host_name": row[0],
                "user_name": row[1],
                "password": row[2],
                "port_list": row[3],
            })
        return rst

    def getProblemShplexDenyMon(self):
        sql = ''' select to_char(check_time,'YYYY-MM-DD hh24:mi:ss') check_time,host_name,port_number,check_stat,user_name,userid_db,userid_sp,comments from SP_OCT_DENIED_USERID_LOG where check_stat = 'N' and  check_time > sysdate-1 order by 2 '''
        rows = self.session.execute(sql).fetchall()
        return rows

    def getProblemShplexDisableObjectMon(self):
        sql = ''' select to_char(check_time,'YYYY-MM-DD hh24:mi:ss') check_time,host_name,port_number,check_stat,queue_name,para_value from SP_OPO_DISABLE_OBJECT_NUM_LOG where check_stat = 'N' order by 2 '''
        rows = self.session.execute(sql).fetchall()
        return rows

    def getProblemShplexDenyMonForAutoFix(self):
        sql = ''' select distinct so.check_time,so.host_name,so.port_number,so.check_stat,so.user_name,so.userid_db,so.userid_sp,so.comments, si.src_splex_sid, f_get_deencrypt(ui.pwd)
    from SP_OCT_DENIED_USERID_LOG so, shareplex_info si, host_user_info ui
    where so.host_name=si.src_host
    and so.port_number=si.port
    and so.check_stat='N' 
    and ui.username='oracle' 
    and si.src_host=ui.host_name order by 2 '''
        rows = self.session.execute(sql).fetchall()
        return rows

    def insert_wbxmonitoralertdetail(self,wbxmonitoralertdetailVo):
        self.session.add(wbxmonitoralertdetailVo)

    def insert_wbxmonitoralertdetail_all(self,wbxmonitoralertdetailVo_all):
        self.session.add_all(wbxmonitoralertdetailVo_all)

    def delete_splexparamdetail(self,hostname, splex_port):
        if splex_port is not None:
            self.session.query(SplexparamdetailVo).filter(SplexparamdetailVo.host_name == hostname).filter(SplexparamdetailVo.port_number == splex_port).delete(synchronize_session=False)
        else:
            self.session.query(SplexparamdetailVo).filter(SplexparamdetailVo.host_name == hostname).delete(synchronize_session=False)

    def delete_splexparamdetail_byparamname(self,splexdetailVo,param_name):
        self.session.query(SplexparamdetailVo).filter(SplexparamdetailVo.host_name == splexdetailVo.host_name).filter(SplexparamdetailVo.port_number == splexdetailVo.port_number).filter(SplexparamdetailVo.param_name == splexdetailVo.param_name).delete(synchronize_session=False)

    def add_splexparamdetail_one(self,splexdetailVo):
        self.session.add(splexdetailVo)

    def add_splexparamdetail(self,splexdetailVo_list):
        self.session.add_all(splexdetailVo_list)

    def get_splexparamdetail(self,host_name,port,param_name):
        SQL = '''select host_name,port_number,param_category,param_name,queue_name,actual_value,default_value,to_char(collect_time,'YYYY-MM-DD hh24:mi:ss') collect_time from splex_param_detail where 1=1'''

        if host_name != "":
            SQL = "%s and host_name = '%s'" % (SQL, host_name)

        if port != "":
            SQL = "%s and port_number = '%s'" % (SQL, port)

        if param_name != "":
            SQL = "%s and param_name = '%s'" % (SQL, param_name)

        rows = self.session.execute(SQL).fetchall()
        return rows


    def deleteDBLinkMonitorResult(self,trim_host, db_name):
        sql = "DELETE FROM wbxdblinkmonitordetail WHERE trim_host='%s' and db_name='%s'" % (trim_host, db_name)
        self.session.execute(sql)

    def listDBLinkBaseline(self):
        dblinkbaselinelist = self.session.query(DBLinkBaselineVO).filter(
            and_(DBLinkBaselineVO.status == 1)).all()
        return dblinkbaselinelist

    def getDBLinkMonitorResult(self,trim_host, db_name, schema_name, dblink_name):
        dblinkMonitorResultVO = self.session.query(DBLinkMonitorResultVO).filter(
            and_(DBLinkMonitorResultVO.trim_host == trim_host, DBLinkMonitorResultVO.db_name == db_name,
                 DBLinkMonitorResultVO.schema_name == schema_name,
                 DBLinkMonitorResultVO.dblink_name == dblink_name)).first()
        return dblinkMonitorResultVO

    def insertDBLinkMonitorResult(self, vo):
        self.session.add(vo)

    def getSchemaBySchemaType(self):
        sql = '''
        select ai.trim_host,ai.db_name,ai.schematype,ai.appln_support_code, ai.schema,f_get_deencrypt(ai.password) password 
        from appln_pool_info ai,database_info di
        where ai.trim_host=di.trim_host
        and ai.db_name=di.db_name
        and di.appln_support_code in ('CONFIG','WEB','TEL','TEO','OPDB','MEDIATE','RPT','LOOKUP','DIAGNS','CSP','CI','TRANS')
        and di.db_type in ('PROD')
       order by  ai.trim_host,ai.db_name,ai.schematype
        '''
        rows = self.session.execute(sql).fetchall()
        return [dict(row) for row in rows]

    def shareplexchannellist(self):
        sql = '''
        select src_host,src_db,port,replication_to,tgt_host,tgt_db,qname,src_splex_sid,tgt_splex_sid,src_schema,tgt_schema 
        from shareplex_info 
        '''
        rows = self.session.execute(sql).fetchall()
        return [dict(row) for row in rows]

    def getFailoverdbFromShareplex(self):
        sql = '''
           select si.src_host,si.src_db,si.port,si.tgt_host,si.tgt_db,si.src_splex_sid,si.tgt_splex_sid
           from shareplex_info si, instance_info sii, database_info sdi, instance_info tii, database_info tdi
           where si.src_host=sii.host_name
           and si.src_db=sii.db_name
           and sii.db_name=sdi.db_name
           and sii.trim_host=sdi.trim_host
           and si.tgt_host=tii.host_name
           and si.tgt_db=tii.db_name
           and tii.db_name=tdi.db_name
           and tii.trim_host=tdi.trim_host
           and sdi.appln_support_code=tdi.appln_support_code
           '''
        rows = self.session.execute(sql).fetchall()
        return [dict(row) for row in rows]

    def get_all_dblink_monitor_list(self,db_name):
        sql = '''
        select db_name,trim_host
        from database_info where DB_TYPE = 'PROD' 
        and appln_support_code in ('TEL','WEB','TEO','CONFIG','OPDB','LOOKUP','DIAGNS','CSP','CI','TRANS') 
        --and appln_support_code in ('TRANS')
        and db_name not in ('TTA136','RACFWEB')
        and db_type = 'PROD'
        '''
        if db_name:
            sql += " and db_name='%s'" %(db_name)
        sql += "order by db_name"
        rows = self.session.execute(sql).fetchall()
        return [dict(row) for row in rows]

    def all_db_dbcache_list(self):
        sql = '''
                select db_name,trim_host,db_type,application_type,appln_support_code,web_domain
                 from database_info where DB_TYPE = 'PROD'
                '''
        rows = self.session.execute(sql).fetchall()
        return [dict(row) for row in rows]

    def getWEBDBShareplexPort(self,host_name,splex_port):
        sql = '''
                select distinct si.tgt_host, di.trim_host, to_char(si.port) port, si.tgt_splex_sid, di.db_name, f_get_deencrypt(ui.pwd)
    from shareplex_info si, database_info di, instance_info ii, host_info hi, host_user_info ui
    where di.appln_support_code in ('WEB','CSP')
    and di.trim_host=ii.trim_host
    and di.db_name=ii.db_name
    and ii.db_name=si.tgt_db
    and ii.host_name=si.tgt_host
    and di.db_name not in ('RACFWEB','RACAFWEB')
    and di.db_type<> 'DECOM'
    and ii.host_name=hi.host_name
    and hi.host_name=ui.host_name
    and ui.username='oracle'
                '''
        if host_name:
            sql += " and hi.host_name='%s'" %(host_name)
        if splex_port:
            sql += " and si.port='%s'" %(splex_port)
        sql += "order by 1"
        rows = self.session.execute(sql).fetchall()
        rst = []
        for row in rows:
            rst.append({
                "host_name": row[0],
                "trim_host":row[1],
                "splex_port":row[2],
                "splex_sid":row[3],
                "db_name":row[4],
                "password":row[5],
            })
        return rst

    def getShplexParamsServerList(self):
        sql = '''
               select distinct host_name, username, pwd, listagg(port,',') within group (order by 1) over(partition by host_name, username, pwd) as port_list from (
select distinct ii.host_name,si.port, ui.username, f_get_deencrypt(ui.pwd) as pwd
                    from shareplex_info si, instance_info ii, database_info di, host_user_info ui
                    where ((si.tgt_host=ii.host_name and si.tgt_db=ii.db_name) or (si.src_host=ii.host_name and si.src_db=ii.db_name))
                    and ii.db_name=di.db_name
                    and ii.trim_host=di.trim_host
                    and di.db_type in ('BTS_PROD','PROD')
                    and ii.host_name=ui.host_name
                    and ui.username='oracle'
                    and di.catalog_db='COMMERCIAL' order by 1
) order by 1
                '''
        rows = self.session.execute(sql).fetchall()
        rst = []
        for row in rows:
            rst.append({
                "host_name": row[0],
                "user_name":row[1],
                "password":row[2],
                "port_list": row[3],
            })
        return rst

    def getCustomizeParamsList(self):
        sql = '''
               select distinct param_category,param_name from splex_param_detail where host_name='0' '''
        rows = self.session.execute(sql).fetchall()
        rst = []
        for row in rows:
            rst.append({
                "param_category": row[0],
                "param_name": row[1],
            })
        return rows

    def getSplexParamsList(self, host_name, port, ismodified, param_name):
        rows = {}

        SQL = """
select host_name,param_name,port_number, queue_name,param_category,default_value, actual_value, collect_time 
from (
select host_name,port_number,param_category, param_name,queue_name, actual_value, default_value, collect_time, 
row_number() over(partition by host_name,port_number,param_name order by collect_time desc) sp
from splex_param_detail 
where host_name !='0' and collect_time >= sysdate-1/24
                """
        if host_name != "":
            SQL = "%s and host_name = '%s'" % (SQL, host_name)
        if port != "":
            SQL = "%s and port_number = '%s'" % (SQL, port)
        if param_name != "":
            SQL = "%s and param_name = '%s'" % (SQL, param_name)
        if ismodified != "":
            SQL = "%s and ismodified = '%s'" % (SQL, ismodified)

        SQL_F = "%s order by ismodified desc,host_name,port_number,param_name ) t where t.sp <=1 " % (SQL)
        paramslist = self.session.execute(SQL_F).fetchall()
        rows['paramslist'] = [[row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7]]
            for row in paramslist]
        # print(rows['paramslist'])
        SQL = '''select  count(1) count from (%s) t where t.sp <=1) ''' % (SQL)
        count = self.session.execute(SQL).fetchone()
        rows["count"] = count[0]
        return rows

    def getDenyUserIdParamsNotMatch(self):
        SQL = """
        select host_name, param_name, port_number, actual_value , expected_value, src_db, schema, collect_time from (
select distinct pd.host_name, pd.param_name, to_char(pd.port_number) port_number, pd.actual_value, api.track_id as expected_value, si.src_db, api.schema, pd.collect_time
from shareplex_info si, splex_param_detail pd , instance_info ii, appln_pool_info api
where pd.param_name='SP_OCT_DENIED_USERID'
and si.src_host=pd.host_name
and si.port=pd.port_number
and si.src_host=ii.host_name
and si.src_db=ii.db_name
and ii.trim_host=api.trim_host
and ii.db_name=api.db_name
and api.schema=decode(api.db_name,'CONFIGDB',decode(si.port,33333,'splex_deny','splex33333'),'GCFGDB',decode(si.port,33333,'splex_deny','splex33333'),'BGCFGDB',decode(si.port,33333,'splex_deny','splex33333'),'splex_deny')
) where actual_value != expected_value
                        """
        rows = self.session.execute(SQL).fetchall()
        return rows

    def all_db_list(self):
        sql = '''
        select trim_host,db_name
        from database_info where db_type = 'PROD' and appln_support_code IN ('WEB')   
        --and db_name in ('RACBWEB')
        order by db_name
        '''
        rows = self.session.execute(sql).fetchall()
        return [dict(row) for row in rows]

    def all_instance_list(self):
        sql = '''
        SELECT db_name,trim_host, listagg(host_name, '/') within group(ORDER BY db_name,trim_host) AS host_name FROM instance_info T 
        GROUP BY db_name,trim_host
                '''
        rows = self.session.execute(sql).fetchall()
        return [dict(row) for row in rows]

    def get_db_domaininfo(self):
        sql = '''
        select * from (
        select webdomainid, domainname, decode( domainname ,  'iawd' , 'RACAHWEB',
                                'iwwd' , 'IWWEB',
                                'RAC'||replace(upper(substr(domainname,0,2)),'D','')||'WEB') db_name, starthour, startminute, endhour, endminute
        from (
        select webdomainid, domainname, min(starthour) starthour, min(startminute) startminute, max(endhour) endhour, max(endminute) endminute
        from test.WBXDBTIMEWINDOW ta, test.wbxwebdomain tb
        where ta.webdomainid=tb.domainid
        group by webdomainid, domainname
        ))
        '''
        rows = self.session.execute(sql).fetchall()
        return [dict(row) for row in rows]

    def getOneDBTnsInfo(self,db_name):
        SQL = '''
                     with tmp as (
               select distinct di.db_name, di.service_name,di.listener_port,ii.trim_host,hi.scan_ip1,hi.scan_ip2,hi.scan_ip3,di.db_type
               from database_info di ,instance_info ii,host_info hi
               where di.db_name= ii.db_name
               and di.trim_host = ii.trim_host
               and ii.host_name = hi.host_name
               and di.db_type in ('PROD','BTS_PROD')
               and hi.scan_ip1 is not null
               order by di.db_name,trim_host
               )
               select * from (
               select tmp.*, row_number() over(PARTITION BY db_name,trim_host order by db_name,trim_host ) as rn from  tmp
               ) where rn = 1 and db_name=:db_name order by db_type desc
                      '''
        list = self.session.execute(SQL, {"db_name": db_name}).fetchall()
        return list

    def getTrimhost(self,src_host, tgt_host):
        SQL = "select distinct trim_host,host_name from instance_info ii where ii.host_name in('%s','%s')" % (
            src_host, tgt_host)
        return self.session.execute(SQL).fetchall()

    def getQnames(self,port, src_db, src_host, tgt_db, tgt_host):
        SQL = '''
        select distinct replication_to||nvl2(qname,'_'||qname, '') replication_to,qname
              from shareplex_info 
              where port = :port and src_db= :src_db and src_host=:src_host and tgt_db= :tgt_db 
              AND tgt_host = :tgt_host '''
        _list = self.session.execute(SQL, {"port": port, "src_db": src_db, "src_host": src_host, "tgt_db": tgt_db,
                                      "tgt_host": tgt_host}).fetchall()
        return _list

    def get_schema_password(self,schemaname, src_dbid, trim_host_src):
        sql = '''select distinct schema as username, trim_host,schematype, appln_support_code, f_get_deencrypt(password) as password 
                               from appln_pool_info where db_name='%s' and trim_host='%s' and schema='%s'
                               ''' % (src_dbid, trim_host_src, schemaname)
        rows = self.session.execute(sql).fetchall()
        return [dict(row) for row in rows]

    def updatewbxadbmon(self,port, src_db, src_host, tgt_db, tgt_host, replication_to, logtime):
        if logtime:
            SQL = "update wbxadbmon set lastreptime = to_date('%s','yyyy-mm-dd hh24:mi:ss'),montime= sysdate where port= '%s' and src_db = '%s' " \
                  "and src_host= '%s' and replication_to = '%s' and tgt_db = '%s' and tgt_host = '%s'" % (
                      logtime, port, src_db, src_host, replication_to, tgt_db, tgt_host)
        else:
            SQL = "update wbxadbmon set montime= sysdate where port= '%s' and src_db = '%s' " \
                  "and src_host= '%s' and replication_to = '%s' and tgt_db = '%s' and tgt_host = '%s'" % (port, src_db, src_host, replication_to, tgt_db, tgt_host)
        self.session.execute(SQL)

    def updatewbxadbmonforkafka(self, src_db, src_host, replication_to, lastreptime, port):
        SQL = """ UPDATE wbxadbmon 
                  SET lastreptime=to_date('%s','YYYY-MM-DD hh24:mi:ss'), montime=SYSDATE 
                  WHERE src_host='%s' and src_db='%s' and replication_to='%s' and port = '%s'""" \
              % (lastreptime, src_host, src_db, replication_to, port)
        self.session.execute(SQL)

    def getadbmonOne(self, port, src_db, src_host, tgt_db, tgt_host, replication_to):
        SQL = "select port,src_db,src_host,replication_to,tgt_db,tgt_host, to_char(lastreptime,'YYYY-MM-DD hh24:mi:ss') " \
              "lastreptime,to_char(montime,'YYYY-MM-DD hh24:mi:ss') montime " \
              "from wbxadbmon where port = %s and src_db= '%s' and src_host='%s' and tgt_db= '%s' " \
              "AND tgt_host = '%s' and replication_to ='%s' " % (
                  port, src_db, src_host, tgt_db, tgt_host, replication_to)
        _list = self.session.execute(SQL).fetchall()
        return _list

    def mergeIntoSplexDenyInfo(self,host_name,port,password):
        SQL = """
        MERGE INTO appln_pool_info ai
        USING (select distinct si.src_db, di.trim_host, di.APPLN_SUPPORT_CODE, lower(du.user_name) user_name, du.userid_sp from shareplex_info si, host_user_info hi,database_info di, SP_OCT_DENIED_USERID_LOG du where si.src_host='%s' and si.port='%s' and hi.host_name=si.src_host and du.host_name=si.src_host and du.port_number = si.port and di.db_name=si.src_db and di.trim_host= hi.trim_host) t
        ON (ai.trim_host = t.trim_host and ai.db_name = t.src_db and ai.schema=t.user_name)
        when matched then update set ai.track_id= t.userid_sp
        when not matched then INSERT (trim_host,db_name,APPLN_SUPPORT_CODE,schema,schematype,password,CREATED_BY,MODIFIED_BY,new_password,track_id) VALUES (t.trim_host,t.src_db,t.appln_support_code,t.user_name,t.user_name,'%s','auto_b','auto_b','%s',t.userid_sp)
        """ % ( host_name,port,password,password)
        self.session.execute(SQL)

    def add_wbxcrlog_one(self,wbxcrlogvo):
        self.session.add(wbxcrlogvo)

    def delete_wbxcrlog_one(self,wbxcrlogvo):
        sql = "DELETE FROM wbxcrlog WHERE host_name='%s' and splex_port='%s' and db_name='%s'" % (wbxcrlogvo.host_name,wbxcrlogvo.splex_port,wbxcrlogvo.db_name)
        self.session.execute(sql)

    def add_wbxcrmonitordetail_one(self,wbxcrmonitordetailVo):
        self.session.add(wbxcrmonitordetailVo)

    def delete_wbxcrmonitordetail_one(self,host_name,splex_port):
        # self.session.query(WbxcrmonitordetailVo).filter(WbxcrmonitordetailVo.host_name == host_name).filter(WbxcrmonitordetailVo.splex_port == splex_port).delete(synchronize_session=False)
        sql = "DELETE FROM wbxcrmonitordetail WHERE host_name='%s' and splex_port='%s'" % (host_name, splex_port)
        self.session.execute(sql)

    def get_wbxcrmonitordetail_failed(self):
        sql = "select distinct host_name, splex_port, db_name, status, errormsg, collecttime from wbxcrmonitordetail where status='FAILED'"
        rows = self.session.execute(sql).fetchall()
        return rows


    def backupinfoloadintooracle(self,**kargs):
        SQL = """
        MERGE INTO wbxdbbackupinfo wdi
        USING (select '%s' trim_host,'%s' db_name,%s SET_STAMP,%s recid,'%s' BACKUP_TYPE,'%s' start_time,'%s' end_time,%s ELAPSED_SECONDS,%s bytes,'%s' path from dual) t
        ON (wdi.trim_host = t.trim_host and wdi.db_name = t.db_name and wdi.SET_STAMP = t.SET_STAMP and wdi.recid=t.recid)
        when matched then update set bytes=t.bytes,path=t.path
        when not matched then INSERT (trim_host,db_name,SET_STAMP,recid,BACKUP_TYPE,start_time,end_time,ELAPSED_SECONDS,bytes,path)
        VALUES (t.trim_host,t.db_name,t.SET_STAMP,t.recid,t.BACKUP_TYPE,to_date(t.start_time,'yyyy-MM-dd HH24:mi:ss'),to_date(t.end_time,'yyyy-MM-dd HH24:mi:ss'),t.ELAPSED_SECONDS,t.bytes,t.path)
        """ % (kargs["trim_host"],kargs["db_name"],kargs["set_stamp"],kargs["recid"],kargs["backup_type"],kargs["start_time"],kargs["end_time"],
               kargs["elapsed_seconds"],kargs["bytes"],kargs["path"],)
        # print(SQL)
        self.session.execute(SQL)

    def getDBConnectionListForGatherStats(self, db_type, appln_support_code, apptype, dcname, schema_type):
        vSQL = (
                " select distinct ta.trim_host, ta.db_name,ta.appln_support_code,ta.db_type, ta.application_type, ta.wbx_cluster,decode('" + schema_type + "','SYSTEM','SYSTEM:sysnotallow',tb.schema||':'||f_get_deencrypt(password))||'@'||ta.connectionstring as connectinfo "
                                                                                                                                                             " from (                                                                              "
                                                                                                                                                             " select distinct db.trim_host, db.db_name, db.appln_support_code,db.db_type, db.application_type,db.wbx_cluster,       "
                                                                                                                                                             "  '(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST='||hi.scan_ip1||')(PORT='||db.listener_port||                      "
                                                                                                                                                             "  '))(ADDRESS=(PROTOCOL=TCP)(HOST='||hi.scan_ip2||')(PORT='||db.listener_port||                                 "
                                                                                                                                                             "  '))(ADDRESS=(PROTOCOL=TCP)(HOST='||hi.scan_ip3||')(PORT='||db.listener_port||                                 "
                                                                                                                                                             "  '))(LOAD_BALANCE=yes)(FAILOVER=on)(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME='||db.service_name||          "
                                                                                                                                                             "  '.webex.com)(FAILOVER_MODE=(TYPE=SELECT)(METHOD=BASIC)(RETRIES=3)(DELAY=5))))' as connectionstring            "
                                                                                                                                                             " from database_info db, instance_info ii, host_info hi                                                                            "
                                                                                                                                                             " where db.trim_host=ii.trim_host  "
                                                                                                                                                             " AND db.db_name=ii.db_name"
                                                                                                                                                             " AND ii.trim_host=hi.trim_host"
                                                                                                                                                             " AND ii.host_name=hi.host_name"
                                                                                                                                                             # " AND ii.host_name=hi.host_name AND db.db_name in ('RACGTWEB','TSJ9','RACLTWEB','TSJCOMB1') "
                                                                                                                                                             " AND db.db_name not in ('RACAFWEB','RACFWEB','TTA35','TSJ35','RACINTH','RACFTMMP','TTA136','TSJ136','IDPRDFTA','RACINFRG','IDPRDFSJ','RACINFRA') "
                                                                                                                                                             " AND hi.host_name not in ('tadborbf06','sjdborbf06','tadborbf07','sjdborbf07','tadbth351','tadbth352','sjdbwbf1','sjdbwbf2','sjdbth351','sjdbth352')"
                                                                                                                                                             " AND upper(db.db_vendor)='ORACLE'"
                                                                                                                                                             " and db.db_type<> 'DECOM'")

        if db_type != 'ALL':
            vSQL = vSQL + " AND db.db_type in ('" + db_type + "')"

        if appln_support_code != "ALL":
            vSQL = vSQL + " AND db.appln_support_code in ('" + appln_support_code + "') "

        if apptype != "ALL":
            vSQL = vSQL + " AND db.application_type in ('" + apptype + "') "

        if dcname != "ALL":
            vSQL = vSQL + " AND upper(hi.site_code) like '%" + dcname + "%'"

        vSQL = vSQL + (
            " ) ta, appln_pool_info tb                                                                                       "
            " where ta.trim_host=tb.trim_host                                                                                "
            " and ta.db_name=tb.db_name    "
            " AND tb.schema != 'stap_ro' "
            " and tb.schematype = 'app'                                                                                 "
            " and upper(tb.appln_support_code)=decode(upper(ta.appln_support_code),'RPT','TEO',upper(ta.appln_support_code)) "
        )
        if schema_type != "SYSTEM":
            vSQL = vSQL + " AND tb.schematype='" + schema_type + "'"

        rows = self.session.execute(vSQL).fetchall()
        dbvolist = []
        for row in rows:
            dbvo = wbxdatabase(row[0], row[1], row[2], row[3], row[4], row[5], row[6])
            dbvolist.append(dbvo)
        return dbvolist

    def getSchemaForGatherStats(self,trim_host,db_name):
        sql = "select upper(schema) from appln_pool_info where trim_host='%s' and schematype='app' and db_name='%s'" % (trim_host,db_name)
        rows = self.session.execute(sql).fetchall()
        dbvolist = []
        for row in rows:
            dbvolist.append(row[0])
        return dbvolist

    def getDBSchemaPwdInfoByDBname(self,trim_host,db_name,schema):
        sql = '''
            select f_get_deencrypt(password) pwd
            from database_info di,appln_pool_info ai
            where di.db_name = ai.db_name
            and di.db_type in ('PROD','BTS_PROD')
            and di.db_vendor='Oracle'
            and ai.trim_host='%s'
            and ai.db_name='%s'
            and upper(ai.schema)='%s'
            --and di.appln_support_code='WEB'
            and di.db_name not in ('RACAFWEB','RACFWEB','IDPRDFSJ','RACAFWEB','IDPRDFTA','RACINFRG','RACAFCSP','RACFTMMP','RACFCSP',
            'RACFMMP','RACFWEB','RACINFRA','TSJ136','TSJ35','TTA35','FSTAPDB','SJFTOOLS')
            --and di.db_name in ('RACASWEB')
            order by di.db_name
        '''% (trim_host,db_name,schema)
        rows = self.session.execute(sql).fetchone()
        return rows[0]

    def insert_appln_pool_info(self,trim_host,db_name,appln_support_code,schema,password):
        SQL = "insert into appln_pool_info(trim_host,db_name,APPLN_SUPPORT_CODE,schema,schematype,password,CREATED_BY,MODIFIED_BY,new_password)values('%s','%s',lower('%s'),lower('%s'),'%s','%s','%s','%s','%s')" % (trim_host,db_name,appln_support_code,schema,"app",password,'auto_b','auto_b',password)
        print(SQL)
        self.session.execute(SQL)

    def delete_gatherstatsjob(self,trim_host,db_name):
        sql = "DELETE FROM gatherstatsjob WHERE trim_host='%s' and db_name='%s'" % (trim_host,db_name)
        self.session.execute(sql)

    def insert_gatherstatsjob_all(self,gatherstatsjoblistvo):
        self.session.add_all(gatherstatsjoblistvo)

    def getKafkaDBList(self,schema):
        vSQL = """SELECT DISTINCT
    ta.trim_host,
    ta.db_name,
    ta.appln_support_code,
    ta.db_type,
    ta.application_type,
    ta.wbx_cluster,
    tb.schema,
    decode('%s', 'SYSTEM', 'SYSTEM:sysnotallow', tb.schema
                                                     || '/'
                                                     || f_get_deencrypt(password))
    || '@'
    || ta.connectionstring AS connectinfo
FROM
    (
        SELECT DISTINCT
            db.trim_host,
            db.db_name,
            db.appln_support_code,
            db.db_type,
            db.application_type,
            db.wbx_cluster,

            '(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST='
            || hi.scan_ip1
            || ')(PORT='
            || db.listener_port
            || '))(ADDRESS=(PROTOCOL=TCP)(HOST='
            || hi.scan_ip2
            || ')(PORT='
            || db.listener_port
            || '))(ADDRESS=(PROTOCOL=TCP)(HOST='
            || hi.scan_ip3
            || ')(PORT='
            || db.listener_port
            || '))(LOAD_BALANCE=yes)(FAILOVER=on)(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME='
            || db.service_name
            || '.webex.com)(FAILOVER_MODE=(TYPE=SELECT)(METHOD=BASIC)(RETRIES=3)(DELAY=5))))' AS connectionstring
        FROM
            database_info  db,
            instance_info  ii,
            host_info      hi
        WHERE
                db.trim_host = ii.trim_host
            AND db.db_name = ii.db_name
            AND ii.trim_host = hi.trim_host
            AND ii.host_name = hi.host_name
            AND upper(db.db_vendor) = 'ORACLE'
            AND db.db_type <> 'DECOM'
    )                ta,
    appln_pool_info  tb
WHERE
        ta.trim_host = tb.trim_host
    AND ta.db_name = tb.db_name
    AND tb.schema = '%s'
    AND upper(tb.appln_support_code) = decode(upper(ta.appln_support_code), 'RPT', 'TEO', upper(ta.appln_support_code))"""%(schema,schema)
        rows = self.session.execute(vSQL).fetchall()
        return [dict(row) for row in rows]


    def getKafkaSplexInfo(self):
        SQL = '''
        select distinct to_char(port) port,src_host,src_db,tgt_host,direction,tgt_db, listagg(qname,',') within group (order by 1) over(partition by port,src_host,src_db,tgt_host,direction,tgt_db) as qname_list
from (
select distinct port,src_host,src_db,tgt_host,replication_to as direction,tgt_db, NVL(qname,0) as qname from shareplex_info where tgt_splex_sid='KAFKA'
)  order by port
        '''
        rows = self.session.execute(SQL).fetchall()
        return [dict(row) for row in rows]

    def insert_data_to_wbxdbiundicator(self, data_list):
        sql = "insert into WBXDBINDICATOR(version, host_name, instance_name, date_time, current_scn, indicator)values('%s', '%s', '%s', to_date('%s','YYYY-MM-DD HH24:MI:SS'), %s, %s)" \
              % (data_list["version"], data_list["host_name"], data_list["instance_name"], data_list["date_time"], data_list["current_scn"], data_list["indicator"])
        print(sql)
        self.session.execute(sql)

    def getTempDelaySplexList(self,delay_min, splex_port):
        SQL = '''
        with ta as (
         select src_host,src_db,port,replication_to,tgt_host,tgt_db,
            to_char(lastreptime,'yyyy-mm-dd hh24:mi:ss') lastreptime,
             to_char(montime,'yyyy-mm-dd hh24:mi:ss') montime,
             diff_secend,diff_day,diff_hour,diff_min,(diff_day||':'||diff_hour||':'||diff_min) lag_by,nvl(wbl.lag_by,10) alert_mins,case when temp.diff_secend>%s*60  then '1' else '0' end alert_flag
             from (        select distinct ta.*,ROUND((ta.montime-ta.lastreptime)*24*60*60) diff_secend,
                        trunc(TO_NUMBER(ta.montime - ta.lastreptime)) diff_day,
                        trunc(TO_NUMBER(ta.montime - ta.lastreptime)*24)- trunc(TO_NUMBER(ta.montime - ta.lastreptime))*24 diff_hour,
                        trunc(TO_NUMBER(ta.montime - ta.lastreptime)*24*60)-trunc(TO_NUMBER(ta.montime - ta.lastreptime)*24)*60 diff_min
                         from wbxadbmon ta
                        where ta.port='%s'
                       
                        ) temp,wbxadbmonlagby wbl 
              
              order by temp.diff_secend desc
        ) 
        select ta.port,ta.src_db,ta.src_host,ta.replication_to,ta.tgt_db,ta.tgt_host,ta.lag_by,ta.lastreptime,ta.montime from ta  where ta.alert_flag = '1'
        '''%(delay_min,splex_port)
        rows = self.session.execute(SQL).fetchall()
        return rows

    def delete_sqlexecutionmon(self,trim_host,db_name):
        sql = "DELETE FROM sql_multi_plan_mon WHERE trim_host='%s' and db_name='%s'" % (trim_host,db_name)
        self.session.execute(sql)

    def insert_sqlexecutionmon(self,sqlexecutionplanmonvo):
        self.session.add_all(sqlexecutionplanmonvo)

    def getAllhostserver(self):
        SQL='''
         select distinct hi.host_name,f_get_deencrypt(pwd) ,hi.ssh_port||'|+|'||hi.physical_cpu||'|+|'||hi.cores
         from database_info db, instance_info ii, host_info hi,host_user_info hu
         where db.trim_host=ii.trim_host AND db.db_name=ii.db_name
         AND ii.trim_host=hi.trim_host AND ii.host_name=hi.host_name
         and hi.host_name=hu.host_name
         and hi.host_name not in ('tadborbf06','sjdborbf06','tadborbf07','sjdborbf07','tadbth351','tadbth352','sjdbwbf1','sjdbwbf2','sjdbth351','sjdbth352')
         --and hi.host_name='sgdbwth12'
         AND upper(db.db_vendor)='ORACLE'
         and db.db_type<> 'DECOM'
        '''
        rows = self.session.execute(SQL).fetchall()
        hostvolist = []
        for row in rows:
            dbvo = wbxcronjobmonitor(row[0], row[1],'',row[2])
            hostvolist.append(dbvo)
        return hostvolist

    def getListenerlogMonitorDBInfo(self):
        sql = '''
        select distinct a.db_name, 
        lower(b.instance_name)||'('||c.host_name||'.'||c.domain ||')' instance_name
        from database_info a,instance_info b ,host_info c
              where a.db_name=b.db_name  
             and a.trim_host=b.trim_host 
              and b.trim_host=c.trim_host
              and b.host_name=c.host_name
              and a.appln_support_code in ('CONFIG','WEB','TEO','TEL','DIAGS','CI','CSP','TRANSCRIPT','CALENDAR')
              and a.db_type in ('PROD','BTS')
              and a.catalog_db!='FEDERAMP'
              --and a.db_name= 'JP1TELDB'
              order by db_name,instance_name
        '''
        rows = self.session.execute(sql).fetchall()
        return rows

    def getListenerlogMonitorServerInfo(self):
        sql = '''
        select distinct host_name from (
          select distinct di.db_name,
              lower(ii.instance_name)||'('||hi.host_name||'.'||hi.domain ||')' instance_name,
              hi.host_name
              from database_info di,instance_info ii ,host_info hi,wbxjobmanagerinstance wi
            where di.db_name=ii.db_name and di.trim_host=ii.trim_host 
            and ii.trim_host=hi.trim_host and ii.host_name=hi.host_name
            and hi.host_name=wi.host_name
            and di.appln_support_code in ('CONFIG','WEB','TEO','TEL','DIAGS','CI','CSP','TRANSCRIPT','CALENDAR')
            and di.db_type in ('PROD','BTS')
            and di.catalog_db!='FEDERAMP'
            and  hi.site_code not in ('BOM01')
            order by db_name,instance_name
        )
        '''
        rows = self.session.execute(sql).fetchall()
        return rows

    def getDBPWDBySchema(self,schema):
        sql = '''
                select distinct ai.db_name,ai.trim_host, ai.schema, f_get_deencrypt(password) as pwd
                from appln_pool_info ai  where lower(ai.schema) = '%s'
                ''' %(str(schema).lower())
        rows = self.session.execute(sql).fetchall()
        return rows

    def getListenerlogMonitorDBInfoChina(self):
        sql = '''
        select distinct a.db_name, 
        lower(b.instance_name)||'('||c.host_name||'.'||c.domain ||')' instance_name
        from database_info a,instance_info b ,host_info c
              where a.db_name=b.db_name  
             and a.trim_host=b.trim_host 
              and b.trim_host=c.trim_host
              and b.host_name=c.host_name
              and a.appln_support_code in ('CONFIG','WEB','TEO','TEL','DIAGS','CI','CSP','TRANSCRIPT','CALENDAR')
              and a.db_type in ('PROD','BTS')
              order by db_name,instance_name
        '''
        rows = self.session.execute(sql).fetchall()
        return rows

    def updatewbxadbmon1(self, port, src_db, tgt_db, tgt_trim_host, replication_to, logtime,delayinsecond):
        SQL = "update wbxadbmon1 set logtime = to_date('%s','yyyy-mm-dd hh24:mi:ss'),delayinsecond=%s,montime= sysdate where port= '%s' and src_db = '%s' " \
              "and tgt_host like '%s%s' and replication_to = '%s' and tgt_db = '%s'" % (
                  logtime, delayinsecond,port, src_db, tgt_trim_host, "%",replication_to, tgt_db)
        self.session.execute(SQL)

    def getFEDERAMP_DB(self):
        sql = '''
        select distinct db_name from database_info where catalog_db = 'FEDERAMP'
        '''
        rows = self.session.execute(sql).fetchall()
        return [dict(row)['db_name'] for row in rows]

    def listdbappuser(self):
        sql = '''
        select trim_host, db_name, connectionurl, listagg(upper(schema),',') within group (order by schema) over (partition by trim_host,db_name)
        from (
            select distinct di.trim_host, di.db_name,'(DESCRIPTION = (ADDRESS = (PROTOCOL = TCP)(HOST = '|| hi.scan_ip1 ||')(PORT = '|| di.listener_port ||'))(ADDRESS = (PROTOCOL = TCP)(HOST = '|| hi.scan_ip2 ||')(PORT = '|| di.listener_port ||'))(ADDRESS = (PROTOCOL = TCP)(HOST = '|| hi.scan_ip3 ||')(PORT = '|| di.listener_port ||'))(LOAD_BALANCE = yes)(FAILOVER = on)(CONNECT_DATA =(SERVER = DEDICATED)(SERVICE_NAME = '|| di.service_name ||'.webex.com)(FAILOVER_MODE =(TYPE = SELECT)(METHOD = BASIC)(RETRIES = 3)(DELAY = 5))))' connectionurl,
                    ai.schema
            from database_info di, instance_info ii, host_info hi, appln_pool_info ai
            where di.db_type in ('BTS_PROD','PROD')
            and di.catalog_db='COMMERCIAL'
            and di.db_name in ('AM1TELDB','AM2CALDB')
            and di.db_name=ii.db_name
            and ii.host_name=hi.host_name
            and di.trim_host=ii.trim_host
            and di.db_name=ai.db_name
            and di.trim_host=ai.trim_host
            and ai.schematype in ('app','xxrpth')
        )
        '''
        rows = self.session.execute(sql).fetchall()
        return rows

    def getPGShareplexDelayAlert(self,delay_min):
        sql = '''
        select * from (
            select distinct ta.src_host,ta.src_db,ta.port,ta.replication_to,ta.tgt_host,ta.tgt_db,ta.lastreptime,ta.montime,
            ROUND((sysdate-ta.montime)*24*60*60) delay_secend,
            trunc(TO_NUMBER(sysdate - ta.montime)) delay_day,
            trunc(TO_NUMBER(sysdate - ta.montime)*24)- trunc(TO_NUMBER(sysdate - ta.montime))*24 delay_hour,
            trunc(TO_NUMBER(sysdate - ta.montime)*24*60)-trunc(TO_NUMBER(sysdate - ta.montime)*24)*60 delay_min,
            case when ROUND((sysdate-ta.montime)*24*60*60)>%s*60  then '1' else '0' end alert_flag,
            sdi.appln_support_code src_appln_support_code,
            tdi.appln_support_code tgt_appln_support_code
            from wbxadbmon ta, shareplex_info si, database_info sdi, instance_info sii, database_info tdi, instance_info tii
                            where ta.src_host=si.src_host
                            and ta.src_db=si.src_db
                            and ta.tgt_host=si.tgt_host
                            and ta.tgt_db=si.tgt_db
                            and ta.port=si.port
                            and ta.replication_to = si.replication_to||nvl2(qname,'_'||qname, '')
                            and si.src_host=sii.host_name
                            and si.src_db=sii.db_name
                            and sii.db_name=sdi.db_name
                            and sii.trim_host=sdi.trim_host
                            and si.tgt_host=tii.host_name
                            and si.tgt_db=tii.db_name
                            and tdi.db_name=tii.db_name
                            and tdi.trim_host=tii.trim_host
                            and tdi.db_type in ('PROD','BTS')
                            and sdi.db_type in ('PROD','BTS')
                           and si.port in (60063)
        ) where alert_flag='1'
        ''' %(delay_min)
        rows = self.session.execute(sql).fetchall()
        return rows

    def getwbxadbmonlist(self,minute):
        sql = ''' 
          select distinct ta.* from wbxadbmon ta, shareplex_info si, database_info sdi, instance_info sii, database_info tdi, instance_info tii
         where ta.src_host=si.src_host
         and ta.src_db=si.src_db
         and ta.tgt_host=si.tgt_host
         and ta.tgt_db=si.tgt_db
         and ta.port=si.port
         and ta.replication_to = si.replication_to||nvl2(qname,'_'||qname, '')
         and si.src_host=sii.host_name
         and si.src_db=sii.db_name
         and sii.db_name=sdi.db_name
         and sii.trim_host=sdi.trim_host
         and si.tgt_host=tii.host_name
         and si.tgt_db=tii.db_name
         and tdi.db_name=tii.db_name
         and tdi.trim_host=tii.trim_host
         and (tdi.db_type in ('PROD','BTS_PROD') or (ta.src_db='RACBTW6' ) or  (ta.src_db='BGSBWEB')  )
         and (sdi.db_type in ('PROD','BTS_PROD') or (ta.tgt_db='BGSBWEB' ) or  (ta.tgt_db='RACBTW6') )
         and ta.montime > sysdate - 1 and ta.montime < sysdate - %s/24/60
         order by ta.montime
        ''' %(minute)
        rows = self.session.execute(sql).fetchall()
        return rows

    def getPGAlertlist(self,minute):
        sql = '''
        select distinct  t.alert_type,t.alertid,t.alerttitle,t.host_name,t.db_name,t.splex_port,t.parameter,t.createtime from (
        select  w.status,w.alert_type,w.alertid,w.alerttitle,w.host_name,w.db_name,w.splex_port,w.parameter,
        to_char(w.createtime,'yyyy-mm-dd hh24:mi:ss') createtime,to_char(w.lastmodifiedtime,'yyyy-mm-dd hh24:mi:ss') lastmodifiedtime
        from wbxmonitoralertdetail w,host_info hi,database_info di
        where w.host_name =hi.host_name
        and hi.trim_host = di.trim_host
        and di.db_vendor in ('POSTGRESQL')
        and w.status = 'NEW'
        and w.createtime >sysdate - %s/(24*60)
        )t ORDER BY createtime DESC
        ''' %(minute)
        rows = self.session.execute(sql).fetchall()
        return rows

    def getPGAlertRule(self):
        sql = '''
        select alert_id,alert_type,alert_title,
        host_name,db_name,splex_port,alert_channel_type,alert_channel_value,is_public
        from wbxmonitoralertconfig 
        '''
        rows = self.session.execute(sql).fetchall()
        return rows

    def all_dblink_monitor_list(self,db_name):
        sql = '''
        select db_name,trim_host
        from database_info where DB_TYPE = 'PROD' 
        and appln_support_code in ('TEL','WEB','TEO','CONFIG','OPDB','LOOKUP','DIAGNS','CSP','CI','TRANS') 
        --and appln_support_code in ('TRANS')
        and db_name not in ('TTA136','RACFWEB')
        and db_type = 'PROD'
        -- and appln_support_code ='CI'
        '''
        if db_name:
            sql += " and db_name='%s'" %(db_name)
        sql += "order by db_name"
        rows = self.session.execute(sql).fetchall()
        return [dict(row) for row in rows]

    def getShareplexinfos_test(self, src_db, tgt_db):
        sql = '''
        select distinct si.src_db,si.src_host,si.port,si.qname,si.replication_to||nvl2(si.qname,'_'||si.qname, '') replication_to,si.tgt_db,si.tgt_host ,sii.trim_host src_trim_host,tii.trim_host tgt_trim_host
        from shareplex_info si,instance_info sii,instance_info tii,database_info sdi,database_info tdi
        where si.src_db=sii.db_name
        and si.src_host=sii.host_name
        and si.tgt_db= tii.db_name
        and si.tgt_host=tii.host_name
        and sii.db_name=sdi.db_name
        and sii.trim_host=sdi.trim_host
        and tdi.db_name=tii.db_name
        and tdi.trim_host=tii.trim_host

        and sdi.db_type in ('PROD')
        and tdi.db_type in ('PROD')

        -- and (sdi.db_type in ('PROD') or sdi.db_name='RACBTW6' or sdi.db_name='BGSBWEB')
        -- and (tdi.db_type in ('PROD') or tdi.db_name='BGSBWEB' or tdi.db_name='RACBTW6')
    
        and si.port !=24531
        and nvl(length(si.qname),0) <=12

        '''

        if src_db:
            src_dbs = src_db.split("_")
            sql += " and si.src_db in ("
            for index, src in enumerate(src_dbs):
                sql += "'" + src + "'"
                if index != len(src_dbs) - 1:
                    sql += ","
            sql += ")"

        if tgt_db:
            tgt_dbs = tgt_db.split("_")
            sql += " and si.tgt_db in ("
            for index, tgt in enumerate(tgt_dbs):
                sql += "'" + tgt + "'"
                if index != len(tgt_dbs) - 1:
                    sql += ","
            sql += ")"
        sql += " order by src_db,port "
        rows = self.session.execute(sql).fetchall()
        return rows