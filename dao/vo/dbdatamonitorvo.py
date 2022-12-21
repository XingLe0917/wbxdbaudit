from datetime import datetime

from sqlalchemy import Column,Integer,String, DateTime, func, text, select
from sqlalchemy import Text, VARCHAR
from sqlalchemy.dialects.oracle import NUMBER
from dao.vo.wbxvo import Base

class WebDomainDataMonitorVO(Base):
    __tablename__ = "wbxwebdomaindatammonitor"
    clustername = Column(String(16), primary_key=True)
    itemname  = Column(String(64), primary_key=True)
    itemvalue = Column(String(4000), primary_key=True)
    monitortime = Column(DateTime, default=datetime.now,onupdate=datetime.now)

class MeetingDataMonitorVO(Base):
    __tablename__ = "wbxmeetingdatamonitor"
    trim_host = Column(String(30), primary_key=True)
    db_name = Column(String(30), primary_key=True)
    cluster_name = Column(String(30))
    case1 = Column(Integer)
    case2 = Column(Integer)
    case3 = Column(Integer)
    case4 = Column(Integer)
    case5 = Column(Integer)
    monitor_time = Column(DateTime)

class WbxdbWaitEventMonitorVO(Base):
    __tablename__ = 'wbxdbwaiteventmonitor'
    db_name = Column(VARCHAR(30), primary_key=True, nullable=False)
    instance_number = Column(NUMBER(1, 0, False), nullable=False)
    event = Column(VARCHAR(64), nullable=False)
    osuser = Column(VARCHAR(30), nullable=False)
    machine = Column(VARCHAR(64), nullable=False)
    sid = Column(NUMBER(10, 0, False), primary_key=True, nullable=False)
    program = Column(VARCHAR(64), nullable=False)
    username = Column(VARCHAR(64), nullable=False)
    sql_id = Column(VARCHAR(64), nullable=False)
    sql_exec_start = Column(DateTime, primary_key=True, nullable=False)
    monitor_time = Column(DateTime, nullable=False)
    duration = Column(NUMBER(10, 0, False))