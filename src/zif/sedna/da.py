from zope.rdb import ZopeDatabaseAdapter, parseDSN
from zope.rdb import DatabaseException, DatabaseAdapterError
from zope.rdb import ZopeConnection, ZopeCursor

#from zope.rdb.interfaces import IZopeConnection

from zope.interface import Interface, implements

from protocol import SednaProtocol, DatabaseError

def connect(dsn):
    """
        connect by dsn
        dsn is formatted like

        dbi://user:passwd@host:port/dbname
    """
    conn_info = parseDSN(dsn)
    if conn_info['host']:
        host = conn_info['host']
    else:
        host='localhost'
    if conn_info['port']:
        port = int(conn_info['port'])
    else:
        port=5050
    if conn_info['username']:
        username = (conn_info['username'])
    else:
        raise DatabaseAdapterError('dsn requires username.')
    if conn_info['password']:
        password = (conn_info['password'])
    else:
        password = ''
    if conn_info['dbname']:
        database = (conn_info['dbname'])
    else:
        raise DatabaseAdapterError('dsn requires dbname.')
    return SednaProtocol(host,database,username,password,port)

class SednaTypeInfo(object):
    paramstyle = 'pyformat'
    threadsafety = 1
    encoding = 'utf-8'

    def getEncoding(self):
        return self.encoding

    def setEncoding(self,encoding):
        raise RuntimeError('Cannot set Sedna encoding.')


class SednaCursor(ZopeCursor):

    def _convertTypes(self,results):
        return results

class SednaAdapterConnection(ZopeConnection):

    def getTypeInfo(self):
        return SednaTypeInfo()

    def cursor(self):
        return SednaCursor(self.conn.cursor(),self)


class SednaAdapter(ZopeDatabaseAdapter):

    def _connection_factory(self):
        return connect(self.dsn)

    def connect(self):
        if not self.isConnected():
            try:
                self._v_connection = SednaAdapterConnection(
                    self._connection_factory(), self)
            except DatabaseError, error:
                raise DatabaseException(str(error))

