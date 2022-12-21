from sqlalchemy import Column,Integer,String, DateTime, func, text, select
from dao.vo.wbxvo import Base

class DBPatchReleaseVO(Base):
    __tablename__ = "wbxdbpatchrelease"
    releasenumber = Column(Integer, primary_key=True)
    appln_support_code = Column(String(15), primary_key=True)
    schematype = Column(String(30), primary_key=True)
    major_number = Column(Integer)
    minor_number = Column(Integer)
    description = Column(String(512))
    isversionchanged = Column(Integer)
    createtime = Column(DateTime, default=func.now())
    lastmodifiedtime = Column(DateTime, default=func.now(), onupdate=func.now())
    def __str__(self):
        return "releasenumber=%s, appln_support_code=%s, schematype=%s" % (self.releasenumber, self.appln_support_code, self.schematype)

class DBPatchDeploymentVO(Base):
    __tablename__ = "wbxdbpatchdeployment"
    deploymentid = Column(String(64), primary_key=True, default=func.sys_guid(), server_default=text("SYS_GUID()"))
    releasenumber = Column(Integer)
    appln_support_code = Column(String(15))
    db_type  = Column(String(15))
    schematype = Column(String(30))
    trim_host = Column(String(30))
    db_name = Column(String(30))
    schemaname = Column(String(30))
    cluster_name = Column(String(30))
    major_number = Column(Integer)
    minor_number = Column(Integer)
    deploytime =  Column(DateTime)
    deploystatus = Column(String(16))
    spdeploytime = Column(DateTime)
    spdeploystatus = Column(String(16))
    change_id = Column(String(30))
    change_sch_start_date = Column(String(30))
    change_completed_date = Column(String(30))
    change_imp = Column(String(50))
    createtime = Column(DateTime, default=func.now())
    lastmodifiedtime = Column(DateTime, default=func.now(), onupdate=func.now())

    def __str__(self):
        return "release_number=%s, db_name=%s, appln_support_code=%s, schematype=%s, deploystatus=%s" % (self.releasenumber, self.db_name, self.appln_support_code, self.schematype, self.deploystatus)



class ShareplexBaselineVO(Base):
    __tablename__ = "wbxshareplexbaseline"
    releasenumber = Column(Integer, primary_key=True)
    src_appln_support_code = Column(String(25), primary_key=True)
    src_schematype = Column(String(16), primary_key=True)
    src_tablename  = Column(String(30), primary_key=True)
    tgt_appln_support_code = Column(String(25), primary_key=True)
    tgt_schematype = Column(String(16), primary_key=True)
    tgt_tablename = Column(String(30), primary_key=True)
    tgt_application_type = Column(String(15))
    tablestatus = Column(String(16))
    specifiedkey = Column(String(200))
    columnfilter = Column(String(4000))
    specifiedcolumn = Column(String(4000))
    changerelease = Column(Integer)

    def __str__(self):
        return "src_appln_support_code=%s,src_schematype=%s, src_tablename=%s,tgt_appln_support_code=%s, tgt_schematype=%s" %\
               (self.src_appln_support_code, self.src_schematype,self.src_tablename,self.tgt_appln_support_code,self.tgt_schematype)