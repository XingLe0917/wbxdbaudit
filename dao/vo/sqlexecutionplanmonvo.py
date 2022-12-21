from sqlalchemy import Column,Integer,String, DateTime, func, text, select
from sqlalchemy.dialects.oracle import NUMBER
from dao.vo.wbxvo import Base

class SqlExecutionPlanMon(Base):
    __tablename__ = "sql_multi_plan_mon"
    trim_host = Column(String(20))
    db_name = Column(String(20),primary_key=True)
    sql_id = Column(String(20),primary_key=True)
    sql_plan_id = Column(String(20),primary_key=True)
    create_time = Column(DateTime)
    modify_time = Column(DateTime)
    monitor_id = Column(String(20))
    executions_delta = Column(Integer)
    cost_cpu = Column(Integer)
    cost_logic = Column(Integer)
    cost_physical = Column(Integer)
    cost_time = Column(Integer)
    monitor_label = Column(String(20))
    monitor_time = Column(DateTime)
    fix_label = Column(String(20))
    fix_time = Column(DateTime)
    spl_profile = Column(String(64))
    best_plan = Column(String(20))
    cpu_increase = Column(Integer)
    logic_increase = Column(Integer)
    physical_increase = Column(Integer)
    time_increase = Column(Integer)
    best_time = Column(DateTime)
    problem_label = Column(String(20))
    new_lable = Column(String(10))
    problem_time = Column(DateTime)



