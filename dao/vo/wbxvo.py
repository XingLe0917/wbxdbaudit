from sqlalchemy.ext.declarative import declarative_base
from common.wbxutil import wbxutil
from datetime import datetime
Base = declarative_base()

def to_dict(self):
    objdict = {}
    for c in self.__table__.columns:
        val = getattr(self, c.name, None)
        if isinstance(val, datetime):
            val = wbxutil.convertDatetimeToString(val)
        objdict[c.name] = val
    return objdict
    # return {c.name: getattr(self, c.name, None) for c in self.__table__.columns}

@classmethod
def loadFromJson(cls, dict):
    obj = cls()
    for c in obj.__table__.columns:
        if c.name in dict:
            val = dict[c.name]
            setattr(obj, c.name, val)
    return obj

Base.to_dict = to_dict
Base.loadFromJson = loadFromJson