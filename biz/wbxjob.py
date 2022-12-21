import logging

from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from sqlalchemy.orm import Session, sessionmaker

from common.config import Config
from dao.vo.dbdatamonitorvo import WebDomainDataMonitorVO

# logging.basicConfig(format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s',level=logging.ERROR)

class WbxJob(object):

    def __init__(self):
        pass

    def initialize(self, *args):
        config = Config()
        schemaname, schemapwd,connectionurl = config.getDepotConnectionurl()
        self.depot_connectionurl = "%s:%s@%s"% (schemaname, schemapwd, connectionurl)
        schemaname1, schemapwd1, connectionurl1 = config.getChinaDepotConnectionurl()
        self.china_depot_connectionurl = "%s:%s@%s" % (schemaname1, schemapwd1, connectionurl1)
        self.args = args

    def start(self):
        pass
