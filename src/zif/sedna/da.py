import time, random

try:
    import threading as _threading
except ImportError:
    import dummy_threading as _threading
try:
    import thread as _thread
except ImportError:
    import dummy_thread as _thread

from zope.interface import implements

from zope.rdb import parseDSN, ZopeConnection, ZopeCursor
from zope.rdb.interfaces import IManageableZopeDatabaseAdapter

import pool
import dbapi

# use a module-level connection pool so the connections may survive when
# the thread dies.  Under Paste, threads die periodically.
#local = threading.local()

connectionPool = pool.manage(dbapi)

#connectionPool = pool.manage(dbapi,poolclass=pool.SingletonThreadPool)

lock = _threading.Lock()

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

    def execute(self, operation, parameters=None, pretty_print=False):
        """Executes an operation, registering the underlying
        connection with the transaction system.  """
        #operation, parameters = self._prepareOperation(operation, parameters)
        self.connection.registerForTxn()
        if parameters is None:
            return self.cursor.execute(operation, pretty_print=pretty_print)
        return self.cursor.execute(operation,parameters,
            pretty_print=pretty_print)


class SednaConnection(ZopeConnection):
    """a zope.rdb.ZopeConnection with conversions disabled"""

    def getTypeInfo(self):
        return SednaTypeInfo()

    def registerForTxn(self):
        if not self._txn_registered:
            self.conn.begin()
            super(SednaConnection,self).registerForTxn()

    def cursor(self):
        curs =  SednaCursor(self.conn.cursor(),self)
        return curs

    def debugOn(self):
        self.conn.debugOn()

    def debugOff(self):
        self.conn.debugOff()

    def traceOn(self):
        self.conn.traceOn()

    def traceOff(self):
        self.conn.traceOff()

class SednaAdapter(object):
    """This is zope.rdb.ZopeDatabaseAdapter, but not Persistent

    Since Sedna Adapter does not want any results conversion,
    A SednaConnection is returned instead of a
    ZopeConnection.

    """
    implements(IManageableZopeDatabaseAdapter)

    def __init__(self, dsn):
        self.setDSN(dsn)
        self._unique_id = '%s.%s.%s' % (
                time.time(), random.random(), _thread.get_ident()
                )

    def _connection_factory(self):
        return connectionPool.connect(self.dsn)

    def setDSN(self, dsn):
        assert dsn.startswith('dbi://'), "The DSN has to start with 'dbi://'"
        self.dsn = dsn

    def getDSN(self):
        return self.dsn

    def connect(self):
        self.connection = SednaConnection(self._connection_factory(), self)

    def disconnect(self):
        if self.isConnected:
            self.connection.close()
            self.connection = None

    def isConnected(self):
        return self.connection

    def __call__(self):
        """
        we lock so other threads cannot get a connection while this
        thread is getting a connection.  Two threads will not get the same
        connection, presumably.
        """
        lock.acquire()
        try:
            self.connect()
        finally:
            lock.release()
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

