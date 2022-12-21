from sqlalchemy import Column,Integer,String, DateTime, func, text, select
from sqlalchemy.dialects.oracle import NUMBER
from dao.vo.wbxvo import Base

class WbxcrlogVo(Base):
    __tablename__ = "wbxcrlog"
    host_name = Column(String(30), primary_key=True)
    trim_host = Column(String(30))
    db_name = Column(String(30),primary_key=True)
    conflictdate = Column(DateTime,primary_key=True)
    crcount = Column(Integer)
    splex_port = Column(Integer)
    collecttime = Column(DateTime,primary_key=True)


class WbxcrmonitordetailVo(Base):
    __tablename__ = "wbxcrmonitordetail"
    host_name = Column(String(30),primary_key=True)
    trim_host = Column(String(30))
    splex_port = Column(Integer)
    db_name = Column(String(30),primary_key=True)
    status = Column(String(16))
    errormsg = Column(String(4000))
    collecttime = Column(DateTime,primary_key=True)