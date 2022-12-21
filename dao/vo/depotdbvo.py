from datetime import datetime

from sqlalchemy import Column,Integer,String, DateTime, func, text, select
from dao.vo.wbxvo import Base

class DBLinkBaselineVO(Base):
    __tablename__ = "wbxdblinkbaseline"
    dblinkid = Column(String(64), primary_key=True)
    db_type  = Column(String(64))
    appln_support_code = Column(String(64))
    application_type = Column(String(64))
    schematype = Column(String(64))
    tgt_db_type  = Column(String(64))
    tgt_appln_support_code = Column(String(64))
    tgt_application_type  = Column(String(64))
    tgt_schematype = Column(String(64))
    dblink_name = Column(String(64))
    status =  Column(Integer)
    dblinktarget = Column(String(16))
    description = Column(String(64))

    def __str__(self):
        return "db_type=%s, appln_support_code=%s, schematype=%s, " \
               "tgt_db_type=%s, tgt_appln_support_code=%s, tgt_schematype=%s, dblink_name=%s, dblinktarget=%s" \
               % (self.db_type, self.appln_support_code, self.schematype,
                  self.tgt_db_type, self.tgt_appln_support_code, self.tgt_schematype,
                  self.dblink_name,self.dblinktarget)

class DBLinkMonitorResultVO(Base):
    __tablename__ = "wbxdblinkmonitordetail"
    trim_host = Column(String(30), primary_key=True)
    db_name = Column(String(25), primary_key=True)
    schema_name = Column(String(35), primary_key=True)
    dblink_name = Column(String(30,collation='NOCASE'), primary_key=True)
    status = Column(String(16))
    errormsg  = Column(String(4000))
    monitor_time = Column(DateTime)