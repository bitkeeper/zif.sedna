from zope.rdb import ZopeDatabaseAdapter, parseDSN
from zope.rdb import DatabaseException, DatabaseAdapterError
from zope.rdb import ZopeConnection, ZopeCursor

from zope.interfaces import Interface, implements


class SednaTypeInfo(object):
    implements(IDBITypeInfo)
    paramstyle = 'pyformat'
    threadsafety = 1
    encoding = 'utf-8'
    def getEncoding(self):
        return self.encoding
    def setEncoding(self,encoding):
        raise RuntimeError('Cannot set Sedna encoding.')


class SednaAdapter(ZopeDatabaseAdapter):
    def _connection_factory(self):
        conn_info = parseDSN(self.dsn)
