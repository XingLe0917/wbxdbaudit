from sqlalchemy import Column, Integer,String, DateTime, func
from dao.vo.wbxvo import Base

class WbxmonitoralertdetailVo(Base):
    __tablename__ = "wbxmonitoralertdetail"
    alertdetailid = Column(String(64), primary_key=True)
    alertid = Column(String(64))
    alerttitle = Column(String(64))
    db_name = Column(String(30))
    host_name = Column(String(30))
    splex_port = Column(Integer)
    instance_name = Column(String(30))
    alert_type = Column(String(30))
    job_name = Column(String(64))
    parameter = Column(String(128))
    alerttime = Column(DateTime, default=func.now())
    createtime = Column(DateTime, default=func.now())
    lastmodifiedtime = Column(DateTime, default=func.now(), onupdate=func.now())