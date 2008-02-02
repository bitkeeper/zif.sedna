import time, random, thread, threading

from zope.component import getSiteManager, ComponentLookupError, getUtility
from zope.interface import Interface, implements, classImplements

from zope.rdb import parseDSN, ZopeConnection, ZopeCursor
from zope.rdb.interfaces import IManageableZopeDatabaseAdapter

import pool
import dbapi

# use a module-level connection pool so the connections may survive when
# the thread dies.  Under Paste, threads die periodically.
connectionPool = pool.manage(dbapi)

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

    def _convertTypes(self,results):
        return results

class SednaAdapterConnection(ZopeConnection):

    def getTypeInfo(self):
        return SednaTypeInfo()

    def cursor(self):
        return SednaCursor(self.conn.cursor(),self)

class SednaAdapter(object):
    """This is zope.rdb.ZopeDatabaseAdapter, but not Persistent

    Since Sedna Adapter does not want any special conversions,
    A SednaADapterConnection is returned instead of a
    ZopeConnection.

    """
    implements(IManageableZopeDatabaseAdapter)

    # We need to store our connections in a thread local to ensure that
    # different threads do not accidently use the same connection. This
    # is important when instantiating database adapters using
    # rdb:provideConnection as the same ZopeDatabaseAdapter instance will
    # be used by all threads.

    _connections = threading.local()

    def __init__(self, dsn):
        self.setDSN(dsn)
        self._unique_id = '%s.%s.%s' % (
                time.time(), random.random(), thread.get_ident()
                )

    def _get_v_connection(self):
        """We used to store the ZopeConnection in a volatile attribute.
           However this was not always thread safe.
        """
        return getattr(SednaAdapter._connections, self._unique_id, None)

    def _set_v_connection(self, value):
        setattr(SednaAdapter._connections, self._unique_id, value)

    _v_connection = property(_get_v_connection, _set_v_connection)

    def _connection_factory(self):
        return connectionPool.connect(self.dsn)

    def setDSN(self, dsn):
        assert dsn.startswith('dbi://'), "The DSN has to start with 'dbi://'"
        self.dsn = dsn

    def getDSN(self):
        return self.dsn

    def connect(self):
        self._v_connection = SednaAdapterConnection(
        self._connection_factory(), self)

    def disconnect(self):
        if self.isConnected():
            self._v_connection.close()
            self._v_connection = None

    def isConnected(self):
        return self._v_connection is not None

    def __call__(self):
        self.connect()
        return self._v_connection

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

