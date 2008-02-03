"""
Sedna Protocol Driver for Python

A very synchronous communicator to a Sedna database,
This is the basic protocol, Connection pooling, auto-commit, dbapi, and other
functionalities can be employed by other modules.

Usage:
    init a Protocol with host, port, username, password, and database.

    conn = Protocol(host,db,login,passwd,port)
        host: string - hostname or ip address
        db: string - Sedna databases hold XML documents and collections.
        login: string
        passwd: string
        port: int - default: 5050

    conn.begin()  - start a transaction
    conn.commit() - commit a transaction
    conn.rollback() - rollback a transaction
    conn.close() - close the connection
    conn.loadText(source,doc_name) - load some text as doc_name
    conn.loadFile(filename,doc_name) - load a file as doc_name

    between begin and commit or rollback, execute queries on the database

    conn.begin()
    result = conn.execute('some_query')

    - queries are XQuery format. See Sedna and XQuery documentation.
      Besides retrieving, you can also insert, update, replace, etc.

    - result is an iterable that returns strings.
    - you may get the entire result with
      str(result) -or- result,tostring() -or- result,tounicode()

    - Sedna stores in utf-8, so queries should be utf-8 encoded strings, and
      string results will be utf-8 encoded strings.

    conn.commit()
    conn.close()

"""

import socket
from struct import pack, unpack, calcsize
import time

try:
    from cStringIO import cStringIO as StringIO
except ImportError:
    from StringIO import StringIO

from elementtree import ElementTree

# Sedna token constants
from msgcodes import *



# standard errors from PEP-249
from dbapiexceptions import Error, Warning, InterfaceError, DatabaseError,\
InternalError, OperationalError, ProgrammingError, IntegrityError,\
DataError, NotSupportedError

SEDNA_VERSION_MAJOR = 3
SEDNA_VERSION_MINOR = 0
SEDNA_MAX_BODY_LENGTH = 10240

LOAD_BUFFER_SIZE = SEDNA_MAX_BODY_LENGTH / 2

import logging
logger = logging.getLogger()
import threading

threadlock = threading.Lock()

# utility functions

def zString(aString):
    """
    return a string prefixed with null+length in network format
    """
    strlen = len(aString)
    return pack('!bi%ss'% strlen,0,strlen,aString)

def splitString(text,length):
    """
    Yield substrings of length or smaller
    """
    while text:
        split, text = text[:length], text[length:]
        yield split

def normalizeMessage(message):
    """
    un-tab and rstrip an informational message

    tab-to-space replacement and rstripping helps with repeatable doctests
    """
    message = message.decode('utf-8')
    n = []
    for k in message.split('\n'):
        n.append(k.rstrip().replace('\t','    '))
    return u'\n'.join(n)



class BasicCursor(object):
    arraysize = 1
    rowcount = -1
    lastrowid = None
    def __init__(self,connection):
        self.connection = connection

    def execute(self,statement, parameters=None):
        if parameters:
            statement = statement % parameters
        self.result =  self.connection.execute(statement)
        return self.result

    def executemany(self,statements,parameters=None):
        for statement in statements:
            if parameters:
                statement = statement % parameters
                self.execute(statement)

    def __iter__(self):
        return iter(self.result)

    def fetchall(self):
        return [item for item in self.result]

    def fetchone(self):
        try:
            return self.result.next()
        except StopIteration:
            return None

    def fetchmany(self,size=None):
        if size is None:
            size = self.arraysize
        else:
            theList = []
            for counter in xrange(size):
                try:
                    theList.append(self.fetchone())
                except StopIteration:
                    break
            return theList

    def setinputsizes(self,sizes):
        pass

    def setoutputsize(self,size,column=None):
        pass

    def close(self):
        self.connection = None


class Result(object):
    """Object representing the result of a query. iterable.

    Iterating over a result will yield a utf-8 encoded string for each "item".

    result.time is a string with the server processing time. This is perhaps
        useful for optimizing queries.

    result.value returns the entire result as a
        utf-8 encoded string

    """

    def __init__(self,conn):
        self.conn = conn
        self._time = None
        self.more = True
        self.item = None

    def __iter__(self):
        return self

    def getTime(self):
        if not self._time:
            time = self.conn._send_string(token=SEDNA_SHOW_TIME)
            self._time = time.decode('utf-8')
        return self._time

    time = property(getTime)

    def next(self):
        currItem = self.item
        if self.more:
            self.conn._send_string(token=SEDNA_GET_NEXT_ITEM)
        if currItem is None:
            raise StopIteration
        else:
            return currItem.decode('utf-8')

    def _get_value(self):
        return u''.join(list(self))

    value = property(_get_value)


class ErrorInfo(object):
    def __init__(self,msg):
        #first Int is the code.
        self.code, = unpack('!I',msg[:4])
        # two Ints and a byte = 9
        # normalize the info so it works reliably in doctests.
        # the world makes sense again... :)
        self.sednaCode = msg[msg.find(':')+8:msg.find('\n')]
        #print "Sedna Code is %s" % self.sednaCode
        self.info = "[%s] %s" % (self.code, normalizeMessage(msg[9:].strip()))


class DebugInfo(ErrorInfo):
    def __init__(self,msg):
        self.code = None
        self.info = "%s" % normalizeMessage(msg[9:].strip())

class SednaError(object):
    def __init__(self,item):
        if isinstance(item,ErrorInfo):
            self.code = item.code
            self.info = item.info
        raise DatabaseError(self.info)
        #super(SednaError,self).__init__(self.info)

class DatabaseRuntimeError(SednaError):
    pass

class SednaProtocol(object):
    """Sedna protocol

    init with
    host         string host name or ip address
    db           string sedna database name to connect to
    login        string user name
    passwd       string user password
    port         int    port for connection default:5050

    Exceptions are raised when operations fail.

    Query execution must take place within a transaction.
    The result of a query will be in self.result; there is only one query
    and one result available at a time, though sedna's ACID properties will
    allow multiple instances to be employed concurrently.

    successful updates return True

    """
    headerFormat = '!II'
    prefixLength = calcsize(headerFormat)
    maxDataLength = SEDNA_MAX_BODY_LENGTH - prefixLength
    receiveBuffer = ''
    result = None
    error = None
    closed = True
    maxQueryLength = SEDNA_MAX_BODY_LENGTH
    notabs = False
    nonewlines = False
    doTrace = False
    inTransaction = False
    ermsgs = None
    cursorFactory = BasicCursor

    # error exposition (PEP-249)
    Error = Error
    Warning = Warning
    InterfaceError = InterfaceError
    DatabaseError = DatabaseError
    InternalError = InternalError
    OperationalError = OperationalError
    ProgrammingError = ProgrammingError
    IntegrityError = IntegrityError
    DataError = DataError
    NotSupportedError = NotSupportedError

# Public interfaces

    # queries

    def execute(self,query,format=0):
        """
        Send query to the Sedna server.

        query should be unicode or otherwise encodable to utf-8
        format is 0 for XML
                  1 for SXML
        """
        # first, clear out previous stuff in case we are in a LRP
        self.ermsgs = []
        self.currItem = []
        self.result = None
        try:
            query = query.encode('utf-8')
        except UnicodeEncodeError,e:
            raise ProgrammingError('Query must be utf-8 or unicode. %s' % e)
        if not self.inTransaction:
            self.begin()
        self.error = None
        self._send_string(query,token=SEDNA_EXECUTE,format=format)
        return self.result

    query = execute

    def close(self):
        """close the connection"""
        if self.socket and not self.closed:
            self._send_string(token=SEDNA_CLOSE_CONNECTION)
            self.closed = True

    # dbi wants a cursor

    def cursor(self):
        """return a cursor from cursorFactory"""
        self.ermsgs = []
        self.currItem = []
        self.result = None
        return self.cursorFactory(self)

    # transactions

    def begin(self):
        """
        start transaction
        """
        if not self.inTransaction:
            lock = threadlock
            lock.acquire()
            self._send_string(token=SEDNA_BEGIN_TRANSACTION)
            while not self.inTransaction:
                time.sleep(0)
            lock.release()

    beginTransaction = begin

    def commit(self):
        """
        commit transaction
        """
        lock = threadlock
        lock.acquire()
        res = self._send_string(token=SEDNA_COMMIT_TRANSACTION)
        while self.inTransaction:
            time.sleep(0)
        lock.release()
        return res

    def rollback(self):
        """
        rollback transaction
        """
        lock = threadlock
        lock.acquire()
        res = self._send_string(token=SEDNA_ROLLBACK_TRANSACTION)
        while self.inTransaction:
            time.sleep(0)
        lock.release()
        return res

    def endTransaction(self,how):
        """endTransaction from Sedna pydriver API"""
        if how == 'commit':
            self.commit()
        elif how == 'rollback':
            self.rollback()
        else:
            raise ProgrammingError(
                "Expected 'commit' or 'rollback', got '%s'" % how)

    def transactionStatus(self):
        """transactionStatus from Sedna pydriver API"""
        if self.inTransaction:
            return 'active'
        else:
            return 'none'

# Miscellaneous public methods

    # sometimes, you just want to upload a document...

    def loadText(self,text,document_name,collection_name=None):
        """
        load a string or stringio into the database as document_name

        if collection_name is provided, document will go in that
        collection.
        """
        self._inputBuffer = StringIO(text)
        s = u'LOAD STDIN "%s"' % document_name
        if collection_name:
            s += ' "%s"' % collection_name
        return self.execute(s)


    def loadFile(self,filename,document_name,collection_name=None):
        """
        load a file by name into the database as document_name

        if the file is not ascii or utf-8 encoded, assure that the
        XML header indicates the correct encoding.

        if collection_name is provided, document will go in that
        collection.
        """
        s = u'LOAD "%s" "%s"' % (filename, document_name)
        if collection_name:
            s += u' "%s"' % collection_name
        return self.execute(s)

# database metadata sugar

    @property
    def documents(self):
        return self._listMetadata('$documents')

    @property
    def modules(self):
        return self._listMetadata('$modules')

    @property
    def collections(self):
        return self._listMetadata('$collections')

    @property
    def indexes(self):
        return self._listMetadata('$indexes')

    @property
    def schema(self):
        return self._listMetadata('$schema')

    def _listMetadata(self,loc):
        s = self.execute(u'doc("%s")' % loc)
        theList = []
        z = s.value
        t = ElementTree.XML(z)
        for item in t:
            name = item.get('name')
            theList.append(name)
        return theList

    def getSchema(self,doc_or_collection_name):
        return self.execute(u'doc("$schema_%s")' % doc_or_collection_name).value

    def getDocumentStats(self,doc_name):
        return self.execute(u'doc("$document_%s")' % doc_name).value

    def getCollectionStats(self,collection_name):
        return self.execute(u'doc("$collection_%s")' % collection_name).value

# debug helpers

    def debugOn(self):
        """
        Sedna should send debugging info.

        Set this within a transaction.
        """
        token = SEDNA_SET_SESSION_OPTIONS
        data = pack("!I",DEBUG_ON)+zString('')
        self._send_string(data,token)

    def setDebugHandler(self,fn):
        self.handleDebug = fn

    def handleDebug(self,debugInfo):
        """Handle debug information.

        if you want to deal with debug info, override this or
        use setDebugHandler, above.

        This method will be called with a DebugInfo object when debug info is
        available as part of a query result.

        you only need to handle this if you call debugOn()

        a DebugInfo object has .code and .info members
        """
        raise NotImplementedError

    def debugOff(self):
        """
        Sedna stops sending debugging info

        Also sent within a transaction.
        """
        token = SEDNA_SET_SESSION_OPTIONS
        data = pack("!I",DEBUG_OFF)+zString('')

        self._send_string(data,token)

    def traceOn(self):
        self.doTrace = True

    def traceOff(self):
        self.doTrace = False

    def resetSessionOptions(self):
        """
        Put session options back to default.
        """
        self._send_string(token=SEDNA_RESET_SESSION_OPTIONS)
# init

    def __init__(self,host='localhost',db="test",login="SYSTEM",
        passwd="MANAGER",port=5050,trace=False):
        self.host = host
        self.port = port
        self.username = login
        self.password = passwd
        self.database = db
        self.ermsgs = []
        # handlers.  left side is a response token from Sedna.
        # right side is the local callback for the body associated
        # with that token.
        self.handlers = {
            SEDNA_SEND_SESSION_PARAMETERS : self._sendSessionParameters,
            SEDNA_SEND_AUTH_PARAMETERS : self._sendAuthParameters,
            SEDNA_AUTHENTICATION_OK : self._authenticationOK,
            SEDNA_AUTHENTICATION_FAILED : self._authenticationFailed,

            SEDNA_ERROR_RESPONSE : self._errorResponse,

            SEDNA_QUERY_SUCCEEDED : self._querySucceeded,
            SEDNA_QUERY_FAILED : self._queryFailed,
            SEDNA_UPDATE_SUCCEEDED : self._updateSucceeded,
            SEDNA_UPDATE_FAILED : self._updateFailed,

            SEDNA_BULKLOAD_FILENAME : self._bulkloadFilename,
            SEDNA_BULKLOAD_FROMSTREAM : self._bulkloadFromstream,
            SEDNA_BULKLOAD_SUCCEEDED : self._bulkloadSucceeded,
            SEDNA_BULKLOAD_FAILED : self._bulkloadFailed,

            SEDNA_BEGIN_TRANSACTION_OK : self._beginTransactionOK,
            SEDNA_BEGIN_TRANSACTION_FAILED : self._beginTransactionFailed,
            SEDNA_COMMIT_TRANSACTION_OK : self._commitTransactionOK,
            SEDNA_COMMIT_TRANSACTION_FAILED : self._commitTransactionFailed,
            SEDNA_ROLLBACK_TRANSACTION_OK : self._rollbackTransactionOK,
            SEDNA_ROLLBACK_TRANSACTION_FAILED : self._rollbackTransactionFailed,

            SEDNA_DEBUG_INFO : self._debugInfo,
            SEDNA_ITEM_PART : self._itemPart,
            SEDNA_ITEM_END : self._itemEnd,
            SEDNA_RESULT_END : self._resultEnd,

            SEDNA_LAST_QUERY_TIME : self._lastQueryTime,

            SEDNA_CLOSE_CONNECTION_OK : self._closeConnectionOK,
            SEDNA_TRANSACTION_ROLLBACK_BEFORE_CLOSE : \
                self._transactionRollbackBeforeClose,
            SEDNA_SET_SESSION_OPTIONS_OK : self._setSessionOptionsOK,
            SEDNA_RESET_SESSION_OPTIONS_OK : self._resetSessionOptionsOK

        }

        self.openSocket(host,port)

        if trace:
            self.traceOn()
        self._send_string(token=SEDNA_START_UP)

# the rest of the module is non-public methods

# socket opening

    def openSocket(self,host,port):
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        except socket.error,e:
            raise InterfaceError(u"Could not create socket: %s" % e)
        try:
            self.socket.connect((host,port))
        except socket.error,e:
            if self.socket:
                self.socket.close()
            raise InterfaceError(
                u'Server connection failed. Is Sedna server running? %s' % e)
        if self.socket:
            # found this on the net,  It's supposed to be faster than default?
            self.socket.setsockopt(socket.SOL_TCP,socket.TCP_NODELAY,0)
        # start handshaking and authenticating
        self.closed = False

# communication with the server

    def _send_string(self,data='',token=0,format=0,respond=True):
        """
        send a message to the server

        data         a string
        token        a defined message id
        format       desired return format for queries

        respond      internal flag indicating whether a particular send is
                     the final send of a request

        The message is token|length prefixed.

        If it is a query message, and it is too long, we split it into smaller
        messages with execute_long...long_query_end

        format is the requested return format for queries.
        format = 0 : XML
        format = 1 : SXML - see Sedna docs - it's the format with parens

        """
        # just a bit of sanity.  data at this point should be a
        # utf-8 encoded string
        if not isinstance(data,str):
            raise InterfaceError (u"Expected string, got %s." % data)
        if token in (SEDNA_EXECUTE, SEDNA_EXECUTE_LONG):
            self.result = None
            datalen = len(data)
            if datalen+self.prefixLength > self.maxQueryLength:
                #if it is a really long request, split it into smaller requests
                for split in splitString(data,LOAD_BUFFER_SIZE):
                    # each of these this is not a final request, so we
                    # set respond to False
                    self._send_string(split,token=SEDNA_EXECUTE_LONG,
                        format=format,respond=False)
                # send a message to end the request
                self._send_string(token=SEDNA_LONG_QUERY_END)
                # return here to prevent endless recursion...
                return
            # if we are doing EXECUTE or EXECUTE_LONG, we need to prefix the
            # request with the byte indicating the desired output format
            data = pack('!b',format) + zString(data)
        elif len(data)+self.prefixLength > self.maxDataLength:
            raise InterfaceError(u"Message is too long.")
        self._sendSocketData(pack(self.headerFormat,int(token),len(data)
            ) + data)
        if self.doTrace:
            if token in (SEDNA_EXECUTE, SEDNA_EXECUTE_LONG):
                trace = data[6:]
            elif token == SEDNA_SET_SESSION_OPTIONS:
                trace = ''
            else:
                trace = data[5:]
            if trace:
                logger.info("(C) %s %s" % (codes[token],
                    trace.strip()))
            else:
                logger.info("(C) %s" % codes[token])
        # Yield current timeslice to other threads. We're always waiting for a
        # sedna server response at this point.  Overall a teeny bit better
        # throughput.
        time.sleep(0)
        if respond:
            return self._get_response()

    def _get_response(self):
        """get the response

        first, get enough of the response to determine its length,
        then obtain the remainder of the response based on the length.

        dispatch response to appropriate handler
        """
        prefixLen = self.prefixLength
        #get the header, two ints
        header = self._getSocketData(prefixLen)
        token,length = unpack(self.headerFormat,header)
        msg = self._getSocketData(length)
        # handlers are call-backs after the data are received
        if self.doTrace:
            if token in (SEDNA_ERROR_RESPONSE, SEDNA_DEBUG_INFO):
                z = msg[9:]
            else:
                z = msg[5:]
            if z:
                logger.info("(S) %s %s" % (codes[token], normalizeMessage(z)))
            else:
                logger.info("(S) %s" % codes[token])
        return self.handlers[token](msg)

# communications at a bit lower level

    def _sendSocketData(self,data):
        """
        send data to the socket, trying to make sure it is all sent
        """
        datalen = len(data)
        totalsent = 0
        while totalsent < datalen:
            try:
                sent = self.socket.send(data[totalsent:])
            except socket.error,e:
                raise InterfaceError('Error writing to socket: %s' % e)
            if sent == 0:
                raise InterfaceError("Socket connection broken.")
            totalsent += sent


    def _getSocketData(self,length):
        """
        get 'length' bytes from the socket
        """
        bufferLen = len(self.receiveBuffer)
        while bufferLen < length:
            if bufferLen == 0:
                # We don't have anything yet.
                # Yield this processing time-slice to other threads.
                time.sleep(0)
            try:
                data = self.socket.recv(length-bufferLen)
            except socket.error,e:
                raise InterfaceError('Error reading from socket: %s' % e)
            self.receiveBuffer += data
            bufferLen += len(data)
        data, self.receiveBuffer = self.receiveBuffer[:length], \
            self.receiveBuffer[length:]
        return data

# handlers

# start-up

    def _sendSessionParameters(self,msg):
        token = SEDNA_SESSION_PARAMETERS
        msg = pack('!bb',SEDNA_VERSION_MAJOR,SEDNA_VERSION_MINOR) \
            + zString(self.username.encode('utf-8')) \
            + zString(self.database.encode('utf-8'))
        self._send_string(msg, token)

# authentication

    def _sendAuthParameters(self,msg):
        token = SEDNA_AUTHENTICATION_PARAMETERS
        msg = zString(self.password.encode('utf-8'))
        self._send_string(msg,token)

    def _authenticationOK(self,msg):
        pass

    def _authenticationFailed(self,msg):
        error = ErrorInfo(msg)
        self.socket.close()
        raise OperationalError(error.info)

# protocol error noticed by the server

    def _errorResponse(self,msg):
        error = ErrorInfo(msg)
        self.inTransaction = False
        self.ermsgs.append(error.info)
        error.info = '\n'.join(self.ermsgs)
        raise SednaError(error)

# transactions - receivers

    def _beginTransactionOK(self,msg):
        self.inTransaction = True

    def _beginTransactionFailed(self,msg):
        error = ErrorInfo(msg)
        self.inTransaction = False
        raise SednaError(error)

    def _commitTransactionOK(self,msg):
        self.inTransaction = False
        return True

    def _commitTransactionFailed(self,msg):
        error = ErrorInfo(msg)
        self.inTransaction = False
        raise SednaError(error)

    def _rollbackTransactionOK(self,msg):
        self.inTransaction = False
        return True

    def _rollbackTransactionFailed(self,msg):
        self.inTransaction = False
        error = ErrorInfo(msg)
        raise SednaError(error)

# queries - receivers

    def _querySucceeded(self,msg):
        self.result = Result(self)
        # sedna immediately sends the first part of the result, so get it.
        self._get_response()
        return self.result

    def _queryFailed(self,msg):
        error = ErrorInfo(msg)
        raise ProgrammingError(error.info)
#        self.result = Result(self)
#        return self.result

    def _updateSucceeded(self,msg):
        self.result = True
        return self.result

    def _updateFailed(self,msg):
        error = ErrorInfo(msg)
        raise SednaError(error)

    def _bulkloadFilelike(self,filelike):
        """
        general internal method for bulk-loading filelikes

        used in _bulkloadFilename and _bulkloadFromstream
        """
        data = filelike.read(LOAD_BUFFER_SIZE)
        token = SEDNA_BULKLOAD_PORTION
        while data:
            if isinstance(data,unicode):
                # this should be acceptable. sockets cannot handle
                # python unicodes, and sedna is always utf-8
                data = data.encode('utf-8')
            data = zString(data)
            self._send_string(data,token,respond=False)
            data = filelike.read(LOAD_BUFFER_SIZE)
        filelike.close()
        self._send_string(token=SEDNA_BULKLOAD_END)

    def _bulkloadFilename(self,msg):
        """
        upload the file we asked to upload
        """
        # Int and a byte = 5
        theFile = open(msg[5:],'r')
        self._bulkloadFilelike(theFile)

    def _bulkloadFailed(self,msg):
        error = ErrorInfo(msg)
        raise SednaError(error)

    def _bulkloadFromstream(self,msg):
        self._bulkloadFilelike(self._inputBuffer)

    def _bulkloadSucceeded(self,msg):
        self._inputBuffer = ''
        return True

    def _lastQueryTime(self,msg):
        #Int-and-a-byte = 5
        return msg[5:]

# Results processing

    def _itemPart(self,msg):
        """
        part of a response is available
        """
        c = self.currItem
        # 5 is Int + byte
        c.append(msg[5:])
        # this is not the final answer, so ask for more...
        self._get_response()

    def _itemEnd(self,msg):
        item = ''.join(self.currItem)
        self.currItem = []
        self.result.item = item

    def _resultEnd(self,msg):
        self.result.more = False
        if self.currItem:
            item = ''.join(self.currItem)
            self.currItem = None
            self.result.item = item
        else:
            self.result.item = None

# debug info

    def _debugInfo(self,msg):
        """
        package a SEDNA_DEBUG_INFO message for client handler.

        client may provide a handleDebug method, using setDebugHandler(fn)
        regardless, debug info ends up in the traceback if enabled.

        """
        di = DebugInfo(msg)
        try:
            self.handleDebug(di)
        except NotImplementedError:
            pass
        self.ermsgs.append(di.info)
        self._get_response()

# Connection and transaction feedback

    def _closeConnectionOK(self,msg):
        self.socket.close()


    def _transactionRollbackBeforeClose(self,msg):
        self.socket.close()
        raise Warning("Transaction rolled back when connection closed")

# setting session options

    def _setSessionOptionsOK(self,msg):
        pass

    def _resetSessionOptionsOK(self,msg):
        pass

