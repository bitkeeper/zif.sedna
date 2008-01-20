
# standard errors from PEP-249
from dbapiexceptions import Error, Warning, InterfaceError, DatabaseError,\
InternalError, OperationalError, ProgrammingError, IntegrityError,\
DataError, NotSupportedError

from protocol import SednaProtocol

try:
    from zope.rdb import parseDSN
except ImportError:
    # too many dependencies to require the egg.
    from externals import parseDSN

# dbapi 2.0-ish

apilevel = 2.0
threadsafety = 2
paramstyle = 'format'

def connect(dsn=None,host=None,port=None,username=None,password=None,
        database=None):
    """ we hope to have a dsn formatted like
        dbi://user:passwd@host:port/dbname
    """
    if dsn:
        conn_info = parseDSN(dsn)
    if conn_info['host']:
        host = conn_info['host']
    if conn_info['port']:
        port = int(conn_info['port'])
    if conn_info['username']:
        username = (conn_info['username'])
    if conn_info['password']:
        password = (conn_info['password'])
    if conn_info['dbname']:
        database = (conn_info['dbname'])
    return SednaProtocol(host,port,username,password,database)

