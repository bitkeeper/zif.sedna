import time, random, thread, threading

from zope.component import getSiteManager, ComponentLookupError, getUtility
from zope.interface import Interface, implements, classImplements

from zope.rdb import parseDSN, ZopeConnection, ZopeCursor
from zope.rdb.interfaces import IManageableZopeDatabaseAdapter

import pool
import dbapi

import threading

# use a module-level connection pool so the connections may survive when
# the thread dies.  Under Paste, threads die periodically.
#local = threading.local()

connectionPool = pool.manage(dbapi)

#connectionPool = pool.manage(dbapi,poolclass=pool.SingletonThreadPool)

DEFAULT_ENCODING = 'utf-8'

class SednaTypeInfo(object):
    paramstyle = 'pyformat'
    threadsafety = 1
    encoding = 'utf-8'

    def getEncoding(self):
        return self.encoding

    def setEncoding(self,encoding):
        raise RuntimeError('Cannot set Sedna encoding.')

    def getConverter(self,anything):
        return identity

def identity(x):
    return x

class SednaCursor(ZopeCursor):
    """a zope.rdb.cursor with conversion disabled"""

    def _convertTypes(self,results):
        return results

class SednaConnection(ZopeConnection):
    """a zope.rdb.ZopeConnection with conversions disabled"""

    def getTypeInfo(self):
        return SednaTypeInfo()

    def cursor(self):
        return SednaCursor(self.conn.cursor(),self)

class SednaAdapter(object):
    """This is zope.rdb.ZopeDatabaseAdapter, but not Persistent

    Since Sedna Adapter does not want any special conversions,
    A SednaConnection is returned instead of a
    ZopeConnection.

    """
    implements(IManageableZopeDatabaseAdapter)

    # pool takes care of thread affinity, so getting connections is a
    # matter of getting a connection from the pool.  Connections
    # return to the pool when they expire.

    def __init__(self, dsn):
        self.setDSN(dsn)
        self._unique_id = '%s.%s.%s' % (
                time.time(), random.random(), thread.get_ident()
                )

    def _connection_factory(self):
        return connectionPool.connect(self.dsn)

    def setDSN(self, dsn):
        assert dsn.startswith('dbi://'), "The DSN has to start with 'dbi://'"
        self.dsn = dsn

    def getDSN(self):
        return self.dsn

    def connect(self):
        lock1 = threading.Lock()
        lock1.acquire()
        # let the other threads have a timeslice so they can see this lock
        time.sleep(0)
        self.connection = SednaConnection(self._connection_factory(), self)
        lock1.release()

    def disconnect(self):
        if self.isConnected:
            self.connection.close()
            self.connection = None

    def isConnected(self):
        return self.connection

    def __call__(self):
        self.connect()
        return self.connection

    # Pessimistic defaults
    paramstyle = 'pyformat'
    threadsafety = 0
    encoding = DEFAULT_ENCODING

    def setEncoding(self, encoding):
        # Check the encoding
        "".decode(encoding)
        self.encoding = encoding

    def getEncoding(self):
        return self.encoding

    def getConverter(self, type):
        'See IDBITypeInfo'
        return identity

    #def __del__(self):
        #self.disconnect()
