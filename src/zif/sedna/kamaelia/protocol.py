from struct import pack, unpack, calcsize
from datetime import datetime
import sys

import Kamaelia.Util.Chunkifier
from Axon.Component import component
from Axon.Ipc import producerFinished, shutdownMicroprocess
from Kamaelia.Internet.TCPClient import TCPClient
from Kamaelia.Chassis.Pipeline import Pipeline
from Kamaelia.Util.PureTransformer import PureTransformer
from Kamaelia.File.BetterReading import IntelligentFileReader
from Kamaelia.Chassis.Graphline import Graphline
#from PurerTransformer import PurerTransformer
from Kamaelia.Util.OneShot import OneShot
from Axon.CoordinatingAssistantTracker import coordinatingassistanttracker

import zif.sedna.msgcodes as se

#errfile = sys.stderr

debug = False

default_config = {
    "sednahost" : u"localhost",
    "sednadb":u"test",
    "sednalogin":"SYSTEM", 
    "sednapasswd":"MANAGER",
    "sednaport":5050,
    "tcpdelay":0,
    "sednaformat":0
    }

config = default_config
config_used = "DEFAULT_INTERNAL"

ZSTRING_FORMAT = '>bi%ss'
BYTE_FORMAT = '>b'
VERS_FORMAT = '>bb'
VERSION_MAJOR = 3
VERSION_MINOR = 0
MAX_MSG_SIZE = 10240
HEADER_FORMAT = ">II"
LONGINT_FORMAT = ">I"

class TransactionFailed(Exception):
    pass

class RollbackFailed(Exception):
    pass

# Error / debug messages are: Int, null byte, Int message length, message: 9
# Data messages are: null byte, Int message length, message: 5
headersizes = {se.SEDNA_ITEM_PART: 5,
             se.SEDNA_LAST_QUERY_TIME: 5,
             se.SEDNA_BULKLOAD_FILENAME: 5,
             se.SEDNA_ERROR_RESPONSE: 9,
             se.SEDNA_DEBUG_INFO: 9} 

def responseheaderlength(token):
    """
    Error / debug messages are: Int, null byte, Int message length, message: 9
    Data messages are: null byte, Int message length, message: 5
    """
    try:
        return headersizes[token]
    except KeyError:
        return 0

def zstring(aString):
    """
    return a string prefixed with null+length in network format
    """
    #first, make sure it is a bytestring in utf-8
    if isinstance(aString, unicode):
        aString = aString.encode('utf-8')
    length = len(aString)
    return pack('>bi%ss' % length, 0, length, aString)

class SednaMessage(object):
    __slots__ = ('code', 'msg')
    msg = ''
    type = None
    def __init__(self, code=None, msg=''):
        self.code = code
        if msg:
            self.msg = msg

    def __str__(self):
        return "%s\n%s" % (se.codes[self.code], 
                           self.msg)

class SednaRequest(SednaMessage):
    pass

class SednaResponse(SednaMessage):
    pass

class SednaError(SednaResponse):
    def __init__(self,code=None,msg=u''):
        """
        on init, get the SE Error code and normalize whitespace in the message
        """
        splittext = []
        self.msg = msg
        for k in msg.split('\n'):
            splittext.append(k.strip())
        self.code = splittext[0].split()[-1]
        self.msg = '\n'.join(splittext)
    def __str__(self):
        return self.msg
    def __repr__(self):
        return self.msg

class SednaDebug(SednaResponse):
    def __init__(self,code=None,msg=u''):
        splittext = []
        self.msg = msg
        for k in msg.split('\n'):
            splittext.append(k.strip())
        self.msg = '\n'.join(splittext)


class AuthenticatedSednaCursor(component):
    """
    This is a connection to a sedna server.
    It needs to init with the connection parameters
      host, port, db, login, and passwd.
      
    This component will automatically connect and authenticate with the
    server.  It will shutdown gracefully if sent a producerFinished() 
    message into its control inbox.
       
    """
    Inboxes  = {"inbox" : "Sedna requests in",
                "control" : "Control messages",
                "connectioncontrol" : "IPC from connection",
                "response" : "responses from Sedna",
                }
    Outboxes = {"outbox": "Sedna response messages out",
                "signal": "Signal Messages",
                "connectionsignal":"IPC to connection",
                "pool":"self to pool when ready",
                "request": "requests to Sedna",
                }
    connected = False
    authenticated = False
    host=config['sednahost']
    db=config['sednadb']
    login=config['sednalogin']
    passwd=config['sednapasswd']
    port=config['sednaport']
    delay=config['tcpdelay']
    format=config['sednaformat'] 
    Client = TCPClient
    Buffer = Kamaelia.Util.Chunkifier.CharacterFIFO
    shutdownMsg = None
    executecodes = set([se.SEDNA_EXECUTE, se.SEDNA_EXECUTE_LONG])
    responseheader = None
    # pool may be a (component, inbox) tuple
    pool = None

    def request(self,msg):
        if not isinstance(msg, SednaRequest):
            raise ValueError(
                "expected SednaRequest object, got %s." % msg)
        self.send(self.wireformat(msg), "request")        

    def info(self,msg):
        pass

    ######## Response stuff ###########

    def getheader(self):
        if not self.responseheader:
            self.responseheader = self.getbytes(8) 
        return self.responseheader 
    
    def getbytes(self, length):
        if length:
            try:
                msg = self.readbuffer.poplength(length)
            except IndexError:
                msg = None
        else:
            msg = u''
        return msg

    def handleresponses(self):
        """
        Parse the data stream coming from a Sedna server into SednaResponse 
        objects.
       
        The output wire format from Sedna is a header and a body.
        The header is a pair of long integers, token and body length.
        After we have the header, we know the length of the message body.
        We obtain the header first, then the message.
        A SednaResponse object is created from the token and the message.
        Any c-style length accounting is removed from text, and the text is
        converted to python unicode.
        
        Intercept and handle anything we have a handler for.
        Any other SednaResponses go out the outbox.
        """
        while self.dataReady("response"):
            msg = self.recv("response")
            self.readbuffer.push(msg)

            while len(self.readbuffer):
                header = self.getheader()
                if header is None:
                    # Return None here so main() gets more data.
                    return
                token, msg_length = unpack(">II", header)
                msg = self.getbytes(msg_length)
                if msg is None:
                    # Return None here so main() gets more data.
                    return
                if msg:
                    headerlen = responseheaderlength(token)
                    if headerlen:
                        msg = msg[headerlen:]
                    msg = msg.decode('utf-8')
    
                self.responseheader = None
                if debug:
                    print  >> sys.stderr, se.codes[token]
                if token in self.handlers:
                    # we can handle this locally
                    self.handlers[token](self, msg)
                else:
                    # pass it downstream for handling
                    if token in self.specialresponses:
                       message = self.specialresponses[token](token,msg) 
                    else:
                        message = SednaResponse(token, msg)
                    self.send(message)
    
    specialresponses = {se.SEDNA_ERROR_RESPONSE:SednaError,
                       se.SEDNA_DEBUG_INFO:SednaDebug,
                      }
    
    def handleinputs(self):
        request = self.recv("inbox")
        assert isinstance(request, SednaRequest)
        self.send(self.wireformat(request),"request")

    def wireformat(self, msg):
        code, data = msg.code, msg.msg
        if code in self.executecodes:
            # queries need to ask for return format
            data = pack('>b', self.format) + zstring(data)
        
        elif code == se.SEDNA_SET_SESSION_OPTIONS:
            # session options have a different format
            data = pack(">I", msg) + zstring('')
        
        return pack(">II", int(code), len(data)) + data


    def main(self):
        """ Initialize the connection, and send the start-up message
        """
        self.readbuffer = self.Buffer()
        connection = self.Client(self.host, self.port, delay=self.delay)
        self.addChildren(connection)
        self.link((self, "request"),(connection, "inbox"))
        self.link((connection, "outbox"),(self, "response"))
        self.link((self, "connectionsignal"),(connection, "control"))
        self.link((connection,"signal"),(self, "connectioncontrol"))
        connection.activate()
        # send start-up
        self.request(SednaRequest(code=se.SEDNA_START_UP))
        self.pause()
        yield 1
        #yield 1
        # Now we loop.
        while not self.shutdown():
            self.pause()
            yield 1
            if self.dataReady("response"):
                self.handleresponses()
            # don't handle anything from the inbox until we are authenticated.
            while self.authenticated and self.dataReady("inbox"):
                self.handleinputs()
            
        # we are in shutdown.
        self.removeChild(self.children[0])
        self.send(producerFinished(self),"signal")
        
    def shutdown(self):
        while self.dataReady("control"):
            # these are outside messages telling us to shutdown.
            msg = self.recv("control")
            
            if isinstance(msg, producerFinished):
                # We got producerFinished from outside, so we should
                # shutdown gracefully.  Tell Sedna we want to close.
                # Sedna will send a message back, then close its end
                # of the connection.  Catch the producerFinished() from the
                # TCPClient below. 
                self.request(SednaRequest(se.SEDNA_CLOSE_CONNECTION))
                return False
            elif isinstance(msg, shutdownMicroprocess):
                # We got shutdownMicroprocess so we do not
                # bother to shutdown gracefully.
                self.send(msg,"connectionsignal")
                return True
            else:
                # Hmmm. Not something we handle here. Send it on.
                self.send(msg,"signal")
                
        while self.dataReady("connectioncontrol"):
            # This is signal from the TCPClient
            msg = self.recv("connectioncontrol")
            if isinstance(msg, (producerFinished, shutdownMicroprocess)):
                if self.shutdownMsg:
                    # we have a nice shutdown message from CloseConnectionOK
                    # or TransactionRollbackBeforeClose.  Send that.
                    msg = self.shutdownMsg
                    self.send(msg,"signal")
                else:
                    # nothing nice.  Send it on anyway.
                    self.send(msg,"signal")
                return True
        return False

    def sendSessionParameters(self, msg):
        self.connected = True
        token = se.SEDNA_SESSION_PARAMETERS
        msg = (pack(VERS_FORMAT, VERSION_MAJOR, 
                   VERSION_MINOR) 
            + zstring(self.login.encode('utf-8')) 
            + zstring(self.db.encode('utf-8')))
        self.request(SednaRequest(token, msg))     
    
    def sendAuthParameters(self, msg):
        token = se.SEDNA_AUTHENTICATION_PARAMETERS
        msg = zstring(self.passwd.encode('utf-8'))
        self.request(SednaRequest(token, msg))
    
    def authenticationOK(self, msg):
        self.authenticated = True
        #self.info(u"Authentication OK.")
        if self.pool:
            # Ready.  Take a dip in the pool. :)
            print >> sys.stdout, "Sedna connection: %s@%s:%s." % (self.db,
                            self.host,self.port)
            self.link((self,"pool"),(self.pool[0],self.pool[1]))
            self.send(self,"pool")
    
    def authenticationFailed(self, msg):
        self.closeConnectionOK(msg)
        self.info(msg)
    
    def closeConnectionOK(self, msg):
        self.send(producerFinished(self),"connectionsignal")
        msg = "Cursor graceful shutdown."
        self.shutdownMsg = producerFinished(self,msg)
        self.authenticated = False

    def rollbackBeforeClose(self, msg):
        self.closeConnectionOK(msg)
        msg = u"Cursor shutdown. Transaction rolled back before close."
        self.shutdownMsg = producerFinished(self,msg)
    
    def itempart(self,msg):
        """
        Optimization tweak.
        Send getnextitem immediately.  Downstream does not need to 
        handle this.
        """
        self.send(SednaResponse(se.SEDNA_ITEM_PART,msg))
        self.request(SednaRequest(code=se.SEDNA_GET_NEXT_ITEM))
    
    handlers = {
            se.SEDNA_SEND_SESSION_PARAMETERS : sendSessionParameters,
            se.SEDNA_SEND_AUTH_PARAMETERS : sendAuthParameters,
            se.SEDNA_AUTHENTICATION_OK : authenticationOK,
            se.SEDNA_AUTHENTICATION_FAILED : authenticationFailed,
            se.SEDNA_CLOSE_CONNECTION_OK : closeConnectionOK,
            se.SEDNA_TRANSACTION_ROLLBACK_BEFORE_CLOSE : rollbackBeforeClose,
            se.SEDNA_ITEM_PART : itempart,
            }

class ClientError(object):
    def __init__(self, msg):
        self.msg = msg
    def __str__(self):
        return self.msg


class Halt(Exception):
    pass


class CursorProcess(component):
    """
    Temporarily use a sedna db cursor.
    
    This is a base component that does not do anything interesting.
    
    The general plan is:
    
    Check control and inbox for incoming. 
    
    When we receive a cursor at the inbox, do our onreceivecursor method. This
    usually sends our desired statement(s) to the db, and is overridden by
    descendent classes for that purpose.
    
    Loop through the following:
      
      Look in the fromdb inbox and do something with the data there that
      should be the response to the onreceivecursor method. 
   
      Responses are handled in the handledata method.Override this for
      descendent classes.  Only three kinds of things should be received.
        SednaError items are error messages.
        SednaDebug items may have useful info for debugging
        SednaResponse items have the data you really want.
      Default handling is to send the response out the "response" outbox. 
    
    Standard "inbox" receives cursors.  When we set self.cursorFinished to True,
    the cursor gets sent to "outbox".
    
    When done with the operation, clean up links, then send the cursor out the
    outbox and terminate.
    
    """
    Inboxes = {"inbox": "cursor in",
               "control": "IPC and shutdown signaling",
               "fromdb" : "responses from the db",
               "bulkdata" : "auxiliary port for bulkload data",
               "bulkcontrol" : "notifications from bulkload source",
               "parameters" : "parameters, etc., for dynamic query statements",
               "paramscontrol" : "notifications from parameters source",
               }
    Outboxes = {"outbox": "cursor out",
                "signal": "IPC and shutdown signaling",
                "todb": "messages to the database",
                "response": "response out",
                }
    
    FileLoader = IntelligentFileReader
    fileinpt = None
    def linkcursor(self):
        self.link((self.cursor, "outbox"), (self, "fromdb"))
        self.link((self, "todb"),(self.cursor, "inbox"))
    
    def unlinkcursor(self):
        self.unlink(self.cursor)
        
    def checkControl(self):
        """
        look for shutdownMicroprocess and producerFinished on control
        """
        while self.dataReady("control"):
            msg = self.recv("control")
            if isinstance(msg, shutdownMicroprocess):
                self.shutdownMsg = msg
                raise Halt()
            elif isinstance(msg, producerFinished):
                self.upstreamDone = True
                self.shutdownMsg = msg
            else:
                self.send(msg, "signal")
    
#    def handleerror(self,item):
#        """
#        handle sedna error messages
#        """
#        self.send(item, "response")
#        self.cursorFinished = True
#    
#    def handledebug(self,item):
#        """
#        handle debug objects
#        """
#        self.send(item, "response")
    
    def onparamscomplete(self):
        """
        do something when all the parameters have been received.  For example,
        create a query using the parameters.  Then set parametersready.
        """
        self.parametersready = True
        
    def onreceivecursor(self):
        """
        do this method when a cursor is received.
        base object does nothing.
        descendent classes will want to override this 
        """
        pass
    
    def handleinbox(self):
        """
        receive cursor from inbox
        """
        if self.dataReady("inbox") and not self.cursor:
            item = self.recv()
            if isinstance(item, AuthenticatedSednaCursor):
                self.cursor = item
                self.linkcursor()
                self.onreceivecursor()
            else:
                self.send(item)
                self.cursorFinished = True
    
    def handledata(self, item):
        """
        SednaResponses arrive here.
        Override to do something more interesting with the incoming
        responses.
        SednaResponse items have code and msg instance variables.
        """
        self.send(item, "response")
    
    def fetch(self):
        """
        get responses from the 'fromdb' inbox.
        """
        while self.dataReady("fromdb"):
            item = self.recv("fromdb")
            if isinstance(item, SednaError):
                self.cursorFinished = True
            if isinstance(item, SednaResponse):
                self.handledata(item)
            else:
                raise ValueError("Expected SednaResponse, got %s" % item)
    
    def handlebulk(self):
        """
        Get utf-8 encoded XML from the bulkdata port and send it to Sedna.
        Sedna will complain if it is not well-formed.  Bulkdata port must send
        producerFinished to complete loading. If it is not UTF-8, your data
        will be weird, so make sure it is encoded either utf-8 or ascii.
        """
        if self.cursor:
            while self.dataReady("bulkdata"):
                item = self.recv("bulkdata")
                while item:
                    out, item = item[:MAX_MSG_SIZE], item[MAX_MSG_SIZE:]
                    self.send(SednaRequest(se.SEDNA_BULKLOAD_PORTION,
                        zstring(out)), 'todb')
    
    def handleparameters(self):
        """
        default assumes that all parameters are received in one chunk.
        override to do something different
        """
        if self.dataReady("parameters"):
            item = self.recv("parameters")
            self.parameters = item
    
    def main(self):
        self.cursor=None
        self.shutdownMsg = None
        self.upstreamDone = False
        self.cursorFinished = False
        self.loadFromBulk = False
        self.parametersready = None
        self.needparameters = False
        try:

            while 1:
                self.checkControl()
                while self.needparameters and self.dataReady("parameters"):
                    self.handleparameters()
                    
                if not self.cursor and self.dataReady("inbox"):
                    # get a cursor  
                    self.handleinbox()

                if self.dataReady("fromdb"):
                    # handle incoming from the database request
                    self.fetch()
                
                if self.loadFromBulk and self.dataReady("bulkdata"):
                    self.handlebulk()
                
                while self.loadFromBulk and self.dataReady("bulkcontrol"):
                    item = self.recv("bulkcontrol")
                    if isinstance(item,producerFinished):
                        # When we get producerFinished in bulkcontrol, the
                        # bulkload is done. Tell Sedna, then turn off this
                        # switch.
                        self.send(SednaRequest(se.SEDNA_BULKLOAD_END), "todb")
                        self.loadFromBulk = False
                        
                while self.needparameters and self.dataReady("paramscontrol"):
                    item = self.recv("paramscontrol")
                    if isinstance(item,producerFinished):
                        self.onparamscomplete()

                if self.cursorFinished:
                    raise Halt()
                else:
                    self.pause()
                    yield 1

        except Halt:
            if self.cursor:
                self.releasecursor()
            if self.shutdownMsg:
                self.send(self.shutdownMsg,"signal")
            else:
                self.send(producerFinished(),"signal")
            return

    def releasecursor(self):
        if self.cursor:
            self.unlinkcursor()
            self.send(self.cursor,"outbox")
            self.cursor = None
            #self.cursorFinished = False

                
class BeginTransaction(CursorProcess):
    """Send "begin transaction".
       Get the "begin transaction OK" response.
       Stop to send the cursor on.
    """
    def onreceivecursor(self):
        #print "sending begin transaction"
        request = SednaRequest(code=se.SEDNA_BEGIN_TRANSACTION)
        self.send(request,"todb")    
    
    def handledata(self,item):                    
        if item.code == se.SEDNA_BEGIN_TRANSACTION_OK:
            self.cursorFinished = True
        else:
            # I've never seen this fail, but just in case, raise ValueError
            if item.code in se.codes:
                msg = ("Expected BEGIN_TRANSACTION_OK, got %s."
                 % se.codes[item.code])
            else:
                msg = ("Begin Transaction failed.\n%s" % item.msg)
            raise ValueError(msg)


class Execute(CursorProcess):
    """
    Execute a query on the cursor.
    Exhaust the resulting items (if any) as python unicode objects out
    the "response" outbox.
    
    This also has support for handling bulkload data, since the statement for
    bulkloading is also a query.
    
    
    """
    query=u'doc($version)'
    def onreceivecursor(self):
        query = self.query
        if isinstance(query,basestring):
            if len(query) > MAX_MSG_SIZE:
                while query:
                    item, query = query[:MAX_MSG_SIZE], query[MAX_MSG_SIZE:]
                    self.send(SednaRequest(se.SEDNA_EXECUTE_LONG, item), "todb")
                self.send(SednaRequest(se.SEDNA_LONG_QUERY_END), "todb")
            else:
                self.send(SednaRequest(code=se.SEDNA_EXECUTE,msg=query),"todb")
        elif isinstance(query, SednaRequest) and query.code == se.SEDNA_EXECUTE:
            self.send(query,"todb")
        else:
            self.send("Not a request: %s" % query)
            self.cursorFinished = True
            self.upstreamDone = True
#            raise ValueError(
#                "Expected SednaRequest or query text, got:\n%s" % query)
        
    def handledata(self,item):
        code = item.code
        msg = item.msg
        if code in self.handlers:
            self.handlers[code](self,msg)
        else:
            self.cursorFinished = True
            self.send(item, "response")
    
    def itempart(self,msg):
        """
        This is ordinarily the place where se.SEDNA_GET_NEXT_ITEM
        gets requested from Sedna. In this implementation,
        AuthenticatedSednaCursor takes care of sending
        se.SEDNA_GET_NEXT_ITEM, so we only need to 
        accumulate the item part here.
        """
        self.accumulator.append(msg)
        
    def itemend(self,msg):
        if msg:
            self.accumulator.append(msg)
        self.send(''.join(self.accumulator), "response")
        self.accumulator = []
        
    def resultend(self,msg):
        self.cursorFinished = True
        
    def lastquerytime(self,msg):
        self.send(msg, "response")
        self.cursorFinished = True
        
    def querysucceeded(self,msg):
        self.accumulator = []
        
    def updatesucceeded(self,msg):
        self.loadFromBulk = False
        self.cursorFinished = True
        
    def bulkloadfilename(self,msg):
        filename =  msg
        inpt = self.fileinpt = self.FileLoader(filename=filename)
        self.link((inpt, "outbox"), (self, "bulkdata"))
        self.link((inpt,"signal"),(self,"bulkcontrol"))
        inpt.activate()
        self.loadFromBulk = True
        
    def bulkloadfromstream(self,msg):
        self.loadFromBulk = True
        
    def bulkloadsucceeded(self,msg):
        if self.fileinpt:
            self.unlink(self.fileinpt)
        self.loadFromBulk = False
        self.cursorFinished = True
        
    def bulkloadfailed(self,msg):
        if self.fileinpt:
            self.unlink(self.fileinpt)
        self.loadFromBulk = False
        self.cursorFinished = True
        self.handleerror(item)
        
    def bulkloaderror(self,msg):
        if self.fileinpt:
            self.unlink(self.fileinpt)
        self.loadFromBulk = False
        self.cursorFinished = True
        self.handleerror(item)

    handlers = {se.SEDNA_ITEM_PART : itempart,
                se.SEDNA_ITEM_END : itemend,
                se.SEDNA_RESULT_END : resultend,
                se.SEDNA_LAST_QUERY_TIME : lastquerytime,
                se.SEDNA_QUERY_SUCCEEDED : querysucceeded,
                se.SEDNA_UPDATE_SUCCEEDED : updatesucceeded,
                se.SEDNA_BULKLOAD_FILENAME : bulkloadfilename,
                se.SEDNA_BULKLOAD_FROMSTREAM : bulkloadfromstream,
                se.SEDNA_BULKLOAD_SUCCEEDED : bulkloadsucceeded,
                se.SEDNA_BULKLOAD_FAILED : bulkloadfailed,
                se.SEDNA_BULKLOAD_ERROR : bulkloaderror,
                }

class BulkLoad(Execute):
    """
    Convenience class for bulk-loading
    """
    document = None
    collection = None
    path = None
    def onreceivecursor(self):
        op = se.SEDNA_EXECUTE
        s = []
        s.append(u'LOAD')
        if self.path:
            s.append(u'"%s"' % self.path)
        else:
            s.append(u'STDIN')
        s.append('"%s"' % self.document)
        if self.collection:
            s.append('"%s"' % self.collection)
        self.send(SednaRequest(op,u' '.join(s)))


class CommitTransaction(CursorProcess):
    def onreceivecursor(self):
        op = SednaRequest(code=se.SEDNA_COMMIT_TRANSACTION)
        self.send(op,"todb")
    
    def handledata(self,item):
        code = item.code
        self.cursorFinished = True
        if code == se.SEDNA_COMMIT_TRANSACTION_OK:
            return
        if code == se.SEDNA_COMMIT_TRANSACTION_FAILED:
            raise TransactionFailed()
        else:
            self.send(item)
            #raise ValueError("Expected COMMIT_TRANSACTION_OK, got %s." 
            #     % se.codes[item.code])


class RollbackTransaction(CursorProcess):
    def onreceivecursor(self):
        op = SednaRequest(code=se.SEDNA_ROLLBACK_TRANSACTION)
        self.send(op,"todb")
    
    def handledata(self,item):
        code = item.code
        if code == se.SEDNA_ROLLBACK_TRANSACTION_OK:
            self.cursorFinished = True
            return
        if code == se.SEDNA_ROLLBACK_TRANSACTION_FAILED:
            raise RollbackFailed()
        else:
            raise ValueError("Expected ROLLBACK_TRANSACTION_OK, got %s." 
                 % se.codes[item.code])


class GetCursor(component):
    Inboxes = {"inbox": "cursors from cursorservice",
               "control": "IPC and shutdown signaling",
               "fromdb" : "responses from the db",
               }
    Outboxes = {"outbox": "cursor out",
                "signal": "IPC and shutdown signaling",
                "todb": "messages to the cursor server",
                }
    service = "SEDNA_CURSORS"
    cursortype = AuthenticatedSednaCursor
    def main(self):
        theCat = coordinatingassistanttracker.getcat()
        service, inboxname = theCat.retrieveService(self.service)
        self.link((self,"todb"), (service, inboxname))
        # send self and my inbox name to the cursor server
        self.send((self,"inbox"), "todb")
        while 1:
            self.pause()
            yield 1
            while self.dataReady("control"):
                msg = self.recv("control")
                if isinstance(msg,shutdownMicroprocess):
                    self.send(msg,"signal")
                    return
                else:
                    self.send(msg,"signal")
            if self.dataReady("inbox"):
                conn = self.recv()
                if isinstance(conn,self.cursortype):
                    self.send(conn)
                else:
                    raise ValueError(
                        "Expected %s, got %s" % (self.cursortype,conn))
                self.send(producerFinished(),"signal")
                self.unlink()
                return


class SetOption(CursorProcess):
    code = se.SEDNA_SET_SESSION_OPTIONS
    option = None
    def onreceivecursor(self):
        if self.option is not None:
            op = SednaRequest(self.code, self.option)
        self.send(op,"todb")
    
    def handledata(self,item):
        code = item.code
        if code == se.SEDNA_SET_SESSION_OPTIONS_OK:
            self.cursorFinished = True
            return

class DebugOn(SetOption):
    option = se.DEBUG_ON


class ReleaseCursor(CursorProcess):
    service = "SEDNA_CURSORS"
    def onreceivecursor(self):
        self.cursorFinished = True
    
    def releasecursor(self):
        theCat = coordinatingassistanttracker.getcat()
        service, inboxname = theCat.retrieveService(self.service)
        self.unlinkcursor()
        self.link((self,"todb"), (service, inboxname))
        self.send(self.cursor,"todb")
        self.unlink(service)
        self.cursor = None
        self.cursorFinished = False
        

class AutoCommit(component):
    service = "SEDNA_CURSORS"
    end = CommitTransaction
    graphline = None
    terminable = True
    def main(self):
        while 1:
            self.pause()
            yield 1
            if self.dataReady("inbox"):
                self.handlein()
            while self.dataReady("control"):
                msg = self.recv("control")
                self.send(msg,"signal")
                if isinstance(msg, shutdownMicroprocess):
                    return

    def handlein(self):
        self.statement = self.recv()
        graphline = Graphline(
                    get = GetCursor(service=self.service),
                    begin = BeginTransaction(),
                    exe = Execute(query=self.statement),
                    end = self.end(),
                    release = ReleaseCursor(service=self.service),
                    linkages = {
                    
                    ('get','outbox'):('begin','inbox'),
                    ('begin','outbox'):('exe','inbox'),
                    ('exe','outbox'):('end','inbox'),
                    ('end','outbox'):('release','inbox'),

                    ('get','signal'):('begin','control'),
                    ('begin','signal'):('exe','control'),
                    ('exe','signal'):('end','control'),
                    ('end','signal'):('release','control'),

                    ('exe','response'):('self','outbox'),
                    ('release','signal'):('self','signal'),
                    })
        self.link((graphline,"outbox"),(self, "outbox"), passthrough=2)
        if self.terminable:
            self.link((graphline,"signal"),(self, "signal"), passthrough=2)
        graphline.activate()


class AutoRollback(AutoCommit):
    end=RollbackTransaction

    
class SednaConsoleInput(PureTransformer):
    """
    Do basic console input for interaction with a Sedna server.
    
    input a macro, preceded by backslash, or input an xquery
    """
    macros= {
            'time':(se.SEDNA_SHOW_TIME, ''),
            'documents':(se.SEDNA_EXECUTE, u'doc("$documents")'),
            'collections':(se.SEDNA_EXECUTE, u'doc("$collections")'),
            'version':(se.SEDNA_EXECUTE, u'doc("$version")'),
            'next':(se.SEDNA_GET_NEXT_ITEM, ''),
            'begin':(se.SEDNA_BEGIN_TRANSACTION, ''),
            }
    
    def processMessage(self,msg):
        msg = msg.strip()
        if msg.startswith('\\'):
            msg = msg[1:].lower()
            if msg == 'quit':
                self.send(producerFinished(),'signal')
                return
            try:
                code, message = self.macros[msg]
            except KeyError:
                return ClientError("Invalid console command")
            request = SednaRequest(code, message)
            if debug:
                print  >> sys.stderr, se.codes[code]
            return request
        elif msg:
            code = se.SEDNA_EXECUTE
            request = SednaRequest(code, msg)
            if debug:
                print  >> sys.stderr, se.codes[code]
            return request


class CursorServer(component):
    Inboxes = {"inbox": "cursor requests",
               "control": "IPC and shutdown signaling",
               "cursorcontrol" : "IPC from cursor",
               }
    Outboxes = {"outbox": "cursors out",
                "signal": "ordinary IPC and shutdown signaling",
                "cursorsignal": "IPC to cursor",
                }
    maxconnections = 5
    minspares = 2
    maxspares = 3
    factory = AuthenticatedSednaCursor
    host=config['sednahost']
    db=config['sednadb']
    login=config['sednalogin']
    passwd=config['sednapasswd']
    port=config['sednaport']
    delay=config['tcpdelay']
    format=config['sednaformat']
    shutdownMsg = None 
    
    def main(self):
        self.spareconnections = []
        self.inuse = []
        yield 1
        self.assurespares()
        yield 1
        while 1:
            try:
                #self.assurespares()
                #yield 1
                while not self.anyReady():
                    self.pause()
                    yield 1
                while self.dataReady("inbox"):
                    
                    item = self.recv("inbox")
                    if isinstance(item, self.factory):
                        # A cursor has been returned
                        if len(self.spareconnections) < self.maxspares:
                            if item in self.inuse:
                                self.inuse.remove(item)
                            self.spareconnections.append(item)
                        else:
                            self.inuse.remove(item)
                            self.closecursor(item)
                    elif isinstance(item,tuple):
                        # This is a (destination, inboxname) request
                        # for a cursor. 
                        dest,inbox = item
                        while not len(self.spareconnections):
                            self.assurespares()
                            self.pause()
                            yield 1
                        self.sendconnection(dest,inbox)
                while self.dataReady("control"):
                    rsp = self.recv("control")
                    if isinstance(rsp, cursorAvailable):
                        cursor = rsp.object
                        self.spareconnections.append(cursor)
                        self.inuse.remove(cursor)
                    elif isinstance(rsp,(shutdownMicroprocess,
                                    producerFinished)):
                        raise Halt()
                while self.dataReady("cursorcontrol"):
                    rsp = self.recv("cursorcontrol")
                    if isinstance(rsp,(shutdownMicroprocess, producerFinished)):
                        if isinstance(rsp.caller, self.factory):
                            self.killcursor(rsp.caller)
                        
            except Halt:
                cursors = self.spareconnections + self.inuse
                self.spareconnections = None
                self.inuse = None
                while cursors:
                    cursor = cursors.pop()
                    self.killcursor(cursor)
                if self.shutdownMsg:
                    self.send(self.shutdownMsg, "signal")
                else:
                    msg = "CursorServer shut down."
                    self.send(producerFinished(self, msg), "signal")
    
    def closecursor(self, cursor):
        self.link((self,"cursorsignal"),(cursor,"control"))
        self.send(producerFinished(self),"cursorsignal")
        self.unlink(thecomponent=cursor)
    
    def killcursor(self,cursor):
        self.remove(cursor)
        self.link((self,"cursorsignal"),(cursor,"control"))
        self.send(shutdownMicroprocess(self),"cursorsignal")
        self.unlink(thecomponent=cursor)
        
    def remove(self,cursor):
        if cursor in self.spareconnections:
            self.spareconnections.remove(cursor)
        elif cursor in self.inuse:
            self.inuse.remove(cursor)
        else:
            raise LookupError("Cursor not found!")    
    
    def checkControl(self):
        while self.dataReady("control"):
            rsp = self.recv("control")
            if isinstance(rsp, (shutdownMicroprocess,producerFinished)):
                self.shutdownMsg = rsp
                raise Halt()
            else:
                self.send(rsp,"signal")
    
    def assurespares(self):
        """
        Make a spare connection if needed.
        """
        if ((len(self.inuse) + 
               len(self.spareconnections) < self.maxconnections)
         and (len(self.spareconnections) < self.minspares)):
            self.createconnection()
        
    def sendconnection(self,dest,inbox):
        """ 
        Send an authenticated connection out the outbox.
        """
        conn = self.spareconnections.pop()
        self.link((self,"outbox"),(dest,inbox))
        self.send(conn)
        self.unlink(dest)
        self.assurespares()
        self.inuse.append(conn)
    
    def createconnection(self):
        conn = self.factory(host=self.host,
            db=self.db, login=self.login, passwd=self.passwd, 
            port=self.port, delay=self.delay, format=self.format, 
            pool=(self,"inbox"))
        #self.addChildren(conn)
        #self.spareconnections.append(conn)
        #self.link((conn,"signal"),(self,"cursorcontrol"))
        conn.activate()

def starttestcursorserver():        
    cursorServer = CursorServer()
    cursorServer.activate()
    theCat = coordinatingassistanttracker.getcat()
    theCat.registerService("SEDNA_CURSORS", cursorServer, "inbox")

if __name__ == '__main__':
    from Kamaelia.Util.Console import ConsoleReader, ConsoleEchoer
    
    starttestcursorserver()
    Pipeline(ConsoleReader(">>> ", ""),
    SednaConsoleInput(),
    AutoCommit(service="SEDNA_CURSORS",terminable=False),
    ConsoleEchoer()).run()
