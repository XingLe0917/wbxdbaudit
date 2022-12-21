from dao.wbxdao import wbxdao
class logmnrdao(wbxdao):
    def __init__(self, connectionurl):
        super(logmnrdao, self).__init__(connectionurl)

    def listDeprecatedLomnrTable(self):
        vSQL = ''' select 'drop table wbxbackup.'||object_name||' purge;' 
                   from dba_objects 
                   where owner='WBXBACKUP' 
                   and object_type='TABLE' 
                   and object_name like 'LOGMNR%'  
                   and created < sysdate -1 '''
        dropSQLList = self.session.execute(vSQL).fetchall()
        return dropSQLList
