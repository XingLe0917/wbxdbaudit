from sqlalchemy import Column,Integer,String, DateTime, func, text, select
from sqlalchemy.dialects.oracle import NUMBER
from dao.vo.wbxvo import Base

class SplexparamdetailVo(Base):
    __tablename__ = "splex_param_detail"
    host_name = Column(String(30),primary_key=True)
    port_number = Column(Integer,primary_key=True)
    param_category = Column(String(60))
    param_name = Column(String(60),primary_key=True)
    queue_name = Column(String(60),primary_key=True)
    actual_value = Column(String(100))
    default_value = Column(String(30))
    collect_time = Column(DateTime)
    ismodified = Column(String(10))
    createtime = Column(DateTime, default=func.now())
    lastmodifiedtime = Column(DateTime, default=func.now(), onupdate=func.now())

    include_relationships = True
    load_instance = True