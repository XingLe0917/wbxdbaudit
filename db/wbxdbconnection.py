import logging
import traceback
import  cx_Oracle
from cx_Oracle import DatabaseError, DataError
from common.config import Config
from DBUtils.PooledDB import PooledDB
from threading import Lock, current_thread

from common.wbxexception import wbxDataException, wbxConnectionException

logger = logging.getLogger("dbmetricagent")

# create one pool for all instances, max 3 connections. if exceed 3 instances, will block more connections
# This class should not cover up exception, it can only wrap exception and throw out again
# Because cx_Oracle does not split Connection error and SQL error, so we should do it
class wbxdbconnection(object):
    _lock = Lock()

    # If maxconnections is not set, then connection will be built as required
    # maxcached only guarantee the Idle connections, means if there are 10 connections,but maxcached is 2, then 8 connection will be stopped
    def __init__(self, username, pwd, connectionurl):
        self._userame = username
        self._pwd = pwd
        self._connectionurl = connectionurl
        self._pool = None
        # self._conn = wbxdatabase._getConn()
        # self._cursor = self._conn.cursor()

    def getConnnection(self):
        wbxdbconnection._lock.acquire(True)
        # t = current_thread()
        # logger.info("Get connection in thread %s" % (t.name))
        try:
            if self._pool is None:
                # I think this should be more common code style, set connection parameter for DBAPI and parameter for DBUtil
                connKwargs = {'dsn': self._connectionurl, 'user': self._userame, 'password': self._pwd, 'encoding': "utf8"}
                self._pool = PooledDB(creator=cx_Oracle, mincached=1, maxcached=1, maxconnections=3, maxshared=0,maxusage=None, **connKwargs)
            conn =  self._pool.dedicated_connection()
            csr = conn.cursor()
            return conn,csr
        except DatabaseError as e:
            logger.error("DatabaseError occured %s" % e, exc_info = e)
            raise wbxConnectionException(e)
        except Exception as e:
            logger.error("Exception occured %s" % e, exc_info=e)
            raise wbxConnectionException(e)
        finally:
            wbxdbconnection._lock.release()

    def _query(self, csr, sql, param=None):
        try:
            if param is None:
                csr.execute(sql)
            else:
                csr.execute(sql, param)
        except DatabaseError as e:
                raise wbxConnectionException(e)
        except Exception as e:
            raise wbxDataException(e)

    def _makeDictFactory(self, csr):
        columnNames = [d[0] for d in csr.description]
        def createRow(*args):
            return dict(zip(columnNames, args))
        return createRow

    def queryOne(self, csr, sql,param = None):
        try:
            if param is None:
                res = csr.execute(sql)
            else:
                res = csr.execute(sql, param)
            row = res.fetchone()
            return row
        except DatabaseError as e:
            raise wbxDataException(e)
        except Exception as e:
            raise wbxDataException(e)

    # the return res will not be None, just empty list
    def queryAll(self, csr, sql, param = None):
        try:
            if param is None:
                res = csr.execute(sql)
            else:
                res = csr.execute(sql, param)
            rows = res.fetchall()
            return rows
        except DatabaseError as e:
            raise wbxDataException(e)
        except Exception as e:
            raise wbxDataException(e)

    def queryOneAsDict(self, csr, sql,param = None):
        try:
            if param is None:
                res = csr.execute(sql)
            else:
                res = csr.execute(sql, param)
            csr._cursor.rowfactory = self._makeDictFactory(csr)
            row = res.fetchone()
            return row
        except DatabaseError as e:
            raise wbxDataException(e)
        except Exception as e:
            raise wbxDataException(e)

    # the return res will not be None, just empty list
    def queryAllAsDict(self, csr, sql, param = None):
        try:
            if param is None:
                res = csr.execute(sql)
            else:
                res = csr.execute(sql, param)
            # DBUtils return SteadyDBCursor, not DBAPI2 cursor
            csr._cursor.rowfactory = self._makeDictFactory(csr)
            rows = res.fetchall()
            return rows
        except DatabaseError as e:
            raise wbxDataException(e)
        except Exception as e:
            raise wbxDataException(e)

    def insertOne(self, csr, sql, value):
        self._query(csr, sql, value)

    def update(self, csr,sql, param=None):
        self._query(csr, sql, param)

    def delete(self, csr, sql, param=None):
        self._query(csr, sql, param)

    def startTransaction(self, conn):
        try:
            conn.begin()
        except DatabaseError as e:
            raise wbxConnectionException(e)

    def commit(self, conn):
        conn.commit()

    def rollback(self, conn):
        try:
            conn.rollback()
        except Exception as e:
            logger.error("Error occurred when rollback", exc_info = e)

    def close(self, conn, csr):
        try:
            csr.close()
            conn.close()
        except DatabaseError as e:
            logger.error(e)
            logger.error(traceback.format_exc())

    def verifyConnection(self, conn, csr):
        SQL = "SELECT 1 FROM dual"
        res = self.queryOne(csr, SQL)


