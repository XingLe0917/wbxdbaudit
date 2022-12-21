from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from sqlalchemy.orm import Session, sessionmaker
import threading
import logging

from sqlalchemy.orm import scoped_session

from common.wbxexception import wbxexception

# logging.basicConfig(format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s',level=logging.DEBUG)
# logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

threadlocal = threading.local()
threadlocal.isTransactionStart = False

class wbxdao(object):

    def __init__(self, connectionurl):
        self._connectionurl = connectionurl
        self.session = None
        self._lock = threading.Lock()

    def connect(self):
        self._engine = create_engine('oracle+cx_oracle://%s' % (self._connectionurl),poolclass=NullPool, echo=False)
        if self.session is None:
            sessionclz = sessionmaker(bind=self._engine, expire_on_commit=True)
            # self.session = sessionclz()
            self.session = scoped_session(sessionclz)

    def startTransaction(self):
        with self._lock:
            if not hasattr(threadlocal, "isTransactionStart"):
                threadlocal.isTransactionStart = True
            elif threadlocal.isTransactionStart:
                raise wbxexception("Transction already started, please close previous transaction at first")
            else:
                threadlocal.isTransactionStart = True

    def flush(self):
        self.session.flush()

    def commit(self):
        self.session.commit()
        threadlocal.isTransactionStart = False

    def rollback(self):
        self.session.rollback()
        threadlocal.isTransactionStart = False

    def close(self):
        self.session.close()
        threadlocal.isTransactionStart = False

    def dropTable(self, vSQL):
        result = self._engine.execute(vSQL)

