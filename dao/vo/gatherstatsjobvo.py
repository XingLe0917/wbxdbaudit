from sqlalchemy import Column,Integer,String, DateTime, func, text, select
from sqlalchemy.dialects.oracle import NUMBER
from dao.vo.wbxvo import Base

class Gatherstatsjob(Base):
    __tablename__ = "gatherstatsjob"
    trim_host = Column(String(30), primary_key=True)
    db_name = Column(String(30),primary_key=True)
    schema_name = Column(String(30))
    job_name = Column(String(4000),primary_key=True)
    last_start_date = Column(DateTime)
    last_run_duration = Column(DateTime)
    collect_time = Column(DateTime,primary_key=True)
    job_action = Column(String(60))
    repeat_interval = Column(String(4000))
