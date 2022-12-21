from dao.wbxdao import wbxdao
from common.wbxutil import wbxutil

class DBpatchDeploymentMonitorDao(wbxdao):

    def __init__(self, connectionurl):
        super(DBpatchDeploymentMonitorDao, self).__init__(connectionurl)

    def getdbpatchdeploymentList(self, schema_name, timerange_in_days):
        vSQL = """
            select distinct version as releasenumber,
                    min(decode(instr(description,'shareplex_type'),0,createtime, null)) over (partition by version) as dbcreatetime,
                    min(case when instr(description,'shareplex_type') > 0 then createtime else null end) over (partition by version) as spcreatetime
            from %s.wbxdatabase
            where createtime > sysdate- %s
            order by nvl(dbcreatetime, spcreatetime)
        """ % (schema_name, timerange_in_days)
        volist = self.session.execute(vSQL).fetchall()
        return volist

    def getScheduledReleaseNumber(self):
        vSQL = """
               select  releasenumber from 
              (
                    select distinct substr(dplcmcinfo,16,5) releasenumber, min(to_date(schstartdate,'YYYY-MM-DD hh24:mi:ss')) over (partition by substr(dplcmcinfo,16,5)) firsttime
                    from stapuser.STAP_CHANGEDBINFO 
                    where to_date(schstartdate,'YYYY-MM-DD hh24:mi:ss') > sysdate-30
                    and implementergroup='Production DBA'
                    and dplcmcinfo like 'dbpatch%'
                ) order by firsttime
                """
        rows = self.session.execute(vSQL).fetchall()
        return [row[0] for row in rows]