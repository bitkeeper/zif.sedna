Sedna is a read-write xml storage.  The interface is a network socket, using a
message-passing scheme for query and update.   Queries are according to the
W3C XQuery standard.  Updates are an extension of XQuery.

Installing Sedna is beyond the scope of this document.  It has Apache 2.0
license and may be obtained from

http://modis.ispras.ru/sedna/

The tests here assume a running Sedna database on localhost named 'test' with
the default login, 'SYSTEM' and password, 'MANAGER'

    1.  start sedna governor
    $ se_gov
    2.  create database 'test' if necessary
    $ se_cdb test
    3.  start database 'test'
    $ se_sm test

Change these if necessary to match your system.
You may also wish to tailf [directory-where-sedna-is ]/data/event.log to
monitor what the server is doing.

    >>> username = 'SYSTEM'
    >>> password = 'MANAGER'
    >>> database = 'test'
    >>> port = 5050
    >>> host = 'localhost'
    >>> sedna_version = '2.2'
    >>> sedna_build = '141'

    >>> import protocol

We do a connection:

    >>> conn = protocol.SednaProtocol(host,port,username,password,database)
    >>> conn.close()

If login fails, you get a DatabaseError.

    >>> bad_password = 'hello'
    >>> conn = protocol.SednaProtocol(host,port,username,bad_password,database)
    Traceback (most recent call last):
    ...
    OperationalError: [226] SEDNA Message: ERROR SE3053
    Authentication failed.

Let's get the Sedna version.  This is a query that "always works".  There are
a bunch of document('$something') queries available that yield system
information.

    >>> conn = protocol.SednaProtocol(host,port,username,password,database)
    >>> conn.begin()
    >>> result = conn.execute(u'document("$version")')
    >>> result.value == '<sedna version="%s" build="%s"/>' % (sedna_version,
    ...       sedna_build)
    True

Let's try a bulk load from a file. Since the region folder is local to this
module, a relative path will work.  In practice, we need to specify an absolute
path.  If this fails, it raises a protocol.DatabaseError.  begin() will happen
automatically when you query and have not already sent begin().

For the list of documents and collections in the current database, we use
connection.documents

    >>> conn = protocol.SednaProtocol(host,port,username,password,database)
    >>> db_docs = conn.documents
    >>> if not 'region' in db_docs:
    ...     z = conn.execute(u'LOAD "example/region.xml" "region"')
    >>> conn.commit()
    True

Equivalently, this could have been written:

conn.loadFile('example/region.xml','region')

If we to load this again, we get an error.

    >>> z = conn.loadFile('example/region.xml','region')
    Traceback (most recent call last):
    ...
    DatabaseError: [163] SEDNA Message: ERROR SE2001
    Document with the same name already exists.

Let's see what's in the document. Note that the resulting output is nicely
formatted.  This is done with leading space and following newline ('\n')
characters in each line of the result.  Since this is XML, they are just there
for output formatting and are not really in the document.  Clients may wish to
iterate the result instead of obtaining value.

    >>> result = conn.execute(u'doc("region")/*/*')
    >>> print result.value
    <africa>
     <id_region>afr</id_region>
    </africa>
    <asia>
     <id_region>asi</id_region>
    </asia>
    <australia>
     <id_region>aus</id_region>
    </australia>
    <europe>
     <id_region>eur</id_region>
    </europe>
    <namerica>
     <id_region>nam</id_region>
    </namerica>
    <samerica>
     <id_region>sam</id_region>
    </samerica>

Extra spaces and newlines may be turned off inside the query.

    >>> ns = u'declare namespace se = "http://www.modis.ispras.ru/sedna";'
    >>> newquery=ns+'declare option se:output "indent=no";doc("region")/*/asia'
    >>> result = conn.execute(newquery)
    >>> print result.value
    <asia><id_region>asi</id_region></asia>

XQuery lets you get just part of the document. 'doc' and 'document' are
synonymous in Sedna XQueries.

    >>> data = conn.execute(u'doc("region")//*[id_region="eur"]')
    >>> print data.value
    <europe>
     <id_region>eur</id_region>
    </europe>

Let's store a new document from just a string. 'BS' stands for 'bookstore'.
We shortened it for readability in this document.

    >>> mytext = """<?xml version="1.0" encoding="ISO-8859-1"?>
    ... <BS>
    ...
    ... <book category="COOKING">
    ...   <title lang="en">Everyday Italian</title>
    ...   <author>Giada De Laurentiis</author>
    ...   <year>2005</year>
    ...   <price>30.00</price>
    ... </book>
    ...
    ... <book category="CHILDREN">
    ...  <title lang="en">Harry Potter</title>
    ...  <author>J K. Rowling</author>
    ...  <year>2005</year>
    ...  <price>29.99</price>
    ... </book>
    ...
    ... <book category="WEB">
    ...  <title lang="en">XQuery Kick Start</title>
    ...  <author>James McGovern</author>
    ...  <author>Per Bothner</author>
    ...       <author>Kurt Cagle</author>
    ...  <author>James Linn</author>
    ...  <author>Vaidyanathan Nagarajan</author>
    ...  <year>2003</year>
    ...  <price>49.99</price>
    ... </book>
    ...
    ... <book category="WEB">
    ...  <title lang="en">Learning XML</title>
    ...  <author>Erik T. Ray</author>
    ...  <year>2003</year>
    ...  <price>39.95</price>
    ... </book>
    ...
    ... </BS>"""
    >>> string1 = mytext
    >>> if 'BS' in db_docs:
    ...     rs = conn.execute(u'DROP DOCUMENT "%s"' % 'BS')
    >>> result = conn.loadText(string1, 'BS')
    >>> result = conn.execute(u'document("BS")//book[price>30]')
    >>> print result.value
    <book category="WEB">
     <title lang="en">XQuery Kick Start</title>
     <author>James McGovern</author>
     <author>Per Bothner</author>
     <author>Kurt Cagle</author>
     <author>James Linn</author>
     <author>Vaidyanathan Nagarajan</author>
     <year>2003</year>
     <price>49.99</price>
    </book>
    <book category="WEB">
     <title lang="en">Learning XML</title>
     <author>Erik T. Ray</author>
     <year>2003</year>
     <price>39.95</price>
    </book>

We can get a book by its index. XQuery indices are 1 based; 2 means second book.
    >>> result = conn.execute(u'document("BS")/BS/book[2]')
    >>> print result.value
    <book category="CHILDREN">
     <title lang="en">Harry Potter</title>
     <author>J K. Rowling</author>
     <year>2005</year>
     <price>29.99</price>
    </book>

We can get the last book.
    >>> result = conn.execute(u'doc("BS")/BS/book[last()]')
    >>> print result.value
    <book category="WEB">
     <title lang="en">Learning XML</title>
     <author>Erik T. Ray</author>
     <year>2003</year>
     <price>39.95</price>
    </book>

We can get the count of the books.
    >>> query = u"""let $items := doc('BS')/BS/book
    ...    return <count>{count($items)}</count>"""
    >>> result = conn.execute(query)
    >>> print result.value
    <count>4</count>

Let's make sure empty results return an empty string.
    >>> result = conn.execute(u'document("BS")//book[price>300]')
    >>> result.value
    u''

Empty string was the right answer. :)

Asking for an element that does not exist returns an empty result, not an
exception.
    >>> result = conn.execute(u'document("BS")/BS/book[9]')
    >>> result.value
    u''

Hmmm. Can we retrieve an item from a list based on a previous selection?
Yes, we can.  This is interesting, since this means we can get back to this
item if we want to update it...

Let's get the second book with a price greater than 30.
    >>> prevQuery = u'document("BS")//book[price>30]'
    >>> query = prevQuery + '[2]'
    >>> result = conn.execute(query)
    >>> print result.value
    <book category="WEB">
     <title lang="en">Learning XML</title>
     <author>Erik T. Ray</author>
     <year>2003</year>
     <price>39.95</price>
    </book>

Here's a query longer than 10240 bytes.  It will go through anyway.

    >>> result = conn.execute(
    ... u'document("BS")//book[price>300]'+' '*15000)
    >>> result.value
    u''

Let's see how long that took.

    >>> z = result.time

Sorry, can't show you the value here. You will have to try it yourself.

    >>> z.endswith('secs')
    True
    >>> conn.commit()
    True

Let's try an update

    >>> qry = u'document("BS")//book[title="Learning XML"]'
    >>> data = conn.execute(qry)
    >>> print data.value
    <book category="WEB">
     <title lang="en">Learning XML</title>
     <author>Erik T. Ray</author>
     <year>2003</year>
     <price>39.95</price>
    </book>

The above "book" element is the item we want to change.  We use the same xpath
to id the item and do an "UPDATE insert" to put a new "quality" element
into the item.  Unicode is handled very simply.  Sedna stores in utf-8
encoded strings.  The protocol encodes to utf-8 before submitting, so any
query must be ascii or utf-8 encoded or unicode.  The protocol always returns
python unicode strings, unless it makes sense to return a boolean.  We are
also checking mixed-mode element handling here.

    >>> ins = u'<quality>Great <i>happy </i>quality</quality>'
    >>> qry2 = u'UPDATE insert %s into %s' % (ins,qry)
    >>> update = conn.execute(qry2)
    >>> print update
    True

OK. Update succeeded.  Let's see the new item.

    >>> check = conn.execute(qry)
    >>> print check.value
    <book category="WEB">
     <quality>Great <i>happy </i>quality</quality>
     <title lang="en">Learning XML</title>
     <author>Erik T. Ray</author>
     <year>2003</year>
     <price>39.95</price>
    </book>

    >>> conn.commit()
    True
    >>> conn.close()

What about rollbacks? Let's try one.
    >>> conn = protocol.SednaProtocol(host,port,username,password,database)
    >>> conn.begin()
    >>> qry = u'document("BS")//book[title="Learning XML"]/quality'
    >>> result = conn.execute(qry)

We have a <quality> element in the book.
    >>> print result.value
    <quality>Great <i>happy </i>quality</quality>
    >>> qry2 = u'UPDATE delete %s' % qry
    >>> result = conn.execute(qry2)
    >>> data = conn.execute(qry)

Now, it's gone
    >>> print len(data.value)
    0

We rollback
    >>> conn.rollback()
    True
    >>> conn.close()

We reopen the database just to be sure that we are not looking at a cache.
    >>> conn = protocol.SednaProtocol(host,port,username,password,database)
    >>> conn.begin()
    >>> data = conn.execute(qry)

The <quality> element is back! Rollback successful!
    >>> print data.value
    <quality>Great <i>happy </i>quality</quality>

Before closing this connection, let's see what the other format looks like.
The default format is 0, XML.  1 gives us SXML.

    >>> qry = u'document("BS")//book[title="Learning XML"]'
    >>> data = conn.execute(qry,format=1)
    >>> print data.value
    (book(@ (category  "WEB"))
     (quality"Great "(i"happy ")"quality")
     (title(@ (lang  "en"))"Learning XML")
     (author"Erik T. Ray")
     (year"2003")
     (price"39.95")
    )
    >>> conn.commit()
    True
    >>> conn.close()
    >>> conn.closed
    True

Starting a new connection here.

    >>> conn = protocol.SednaProtocol(host,port,username,password,database)

Error handling.  Let's try to catch a DatabaseError.
This should be an XQuery syntax error, so will be caught right when the
request is sent.
    >>> conn.begin()
    >>> try:
    ...     result = conn.execute(u'hello world')
    ... except protocol.DatabaseError,e:
    ...     err =  e.info
    ...     print err
    [3] SEDNA Message: ERROR XPST0003
        It is a static error if an expression is not a valid instance of the grammar defined in A.1 EBNF.
    Details: syntax error at token: 'world', line: 1

Now for errors in 'valid' but troublesome queries, errors that happen while the
result is being generated.  The lesson I learn here is "keep it simple".
YMMV. :)

    >>> conn.begin()

For a full idea of the client-server communication, set traceOn().  We'll
log to stdout here.  Trace happens at logging.DEBUG level.

    >>> import logging
    >>> import sys
    >>> logging.basicConfig(stream=sys.stdout)
    >>> log = logging.getLogger()
    >>> log.setLevel(logging.DEBUG)

We turn on tracing

    >>> conn.traceOn()

Here's a query that fails at run-time.

    >>> qry = u'''(: In this query dynamic error will be raised   :)
    ... (: due to "aaaa" is not castable to xs:integer. :)
    ... declare function local:f()
    ... {
    ... "aaaa" cast as xs:integer
    ... };
    ... local:f()
    ... '''
    >>> result = conn.execute(qry)
    DEBUG:root:(C) SE_EXECUTE (: In this query dynamic error will be raised   :)
    (: due to "aaaa" is not castable to xs:integer. :)
    declare function local:f()
    {
    "aaaa" cast as xs:integer
    };
    local:f()
    DEBUG:root:(S) SE_QUERY_SUCCEEDED

Tracing gives a representation of the internal client-server interaction.
(C) messages are sent by the client, and (S) messages are the server's response.
Above, we see the client sending the query, and the server's response.

The server says above that the query succeeds, but when we try to get the
value, a terrible thing happens.

    >>> print result.value
    Traceback (most recent call last):
    ...
    DatabaseError: [112] SEDNA Message: ERROR FORG0001
        Invalid value for cast/constructor.
    Details: Cannot convert to xs:integer type
    [413] SEDNA Message: ERROR SE4614
    There is no next item of the user's query.

We get an error, but this is not as helpful as it can be.  We set debugOn to
get a bit more info.

    >>> conn.debugOn()
    DEBUG:root:(C) SE_SET_SESSION_OPTIONS
    DEBUG:root:(S) SE_SET_SESSION_OPTIONS_OK

Retry the same query.

    >>> result = conn.execute(qry)
    DEBUG:root:(C) SE_BEGIN_TRANSACTION
    DEBUG:root:(S) SE_BEGIN_TRANSACTION_OK
    DEBUG:root:(C) SE_EXECUTE (: In this query dynamic error will be raised   :)
    (: due to "aaaa" is not castable to xs:integer. :)
    declare function local:f()
    {
    "aaaa" cast as xs:integer
    };
    local:f()
    DEBUG:root:(S) SE_QUERY_SUCCEEDED

Now, when we get the traceback, there is a bit more info that is maybe more
helpful.

    >>> print result.value
    Traceback (most recent call last):
    ...
    DatabaseError: [1] PPCast : 1
    [1] PPFunCall : 1 : http://www.w3.org/2005/xquery-local-functions:f
    [112] SEDNA Message: ERROR FORG0001
        Invalid value for cast/constructor.
    Details: Cannot convert to xs:integer type
    [413] SEDNA Message: ERROR SE4614
    There is no next item of the user's query.
    >>> conn.debugOff()
    DEBUG:root:(C) SE_SET_SESSION_OPTIONS
    DEBUG:root:(S) SE_SET_SESSION_OPTIONS_OK

This is an example of a less contentious session.

    >>> qry = '''for $item in document("BS")//book
    ... let $price := round-half-to-even($item/price * 1.1,2)
    ... where $item/title = "Learning XML"
    ... return <price>{$price}</price>'''
    >>> data = conn.execute(qry)
    DEBUG:root:(C) SE_BEGIN_TRANSACTION
    DEBUG:root:(S) SE_BEGIN_TRANSACTION_OK
    DEBUG:root:(C) SE_EXECUTE for $item in document("BS")//book
    let $price := round-half-to-even($item/price * 1.1,2)
    where $item/title = "Learning XML"
    return <price>{$price}</price>
    DEBUG:root:(S) SE_QUERY_SUCCEEDED
    >>> print data.value
    DEBUG:root:(C) SE_GET_NEXT_ITEM
    DEBUG:root:(S) SE_ITEM_PART <price>43.95</price>
    DEBUG:root:(S) SE_ITEM_END
    DEBUG:root:(C) SE_GET_NEXT_ITEM
    DEBUG:root:(S) SE_RESULT_END
    <price>43.95</price>
    >>> conn.traceOff()
    >>> conn.commit()
    True
    >>> conn.close()

Reset the log level
    >>> log.setLevel(logging.ERROR)

Final cleanup. We'll remove the local documents.
    >>> conn = protocol.SednaProtocol(host,port,username,password,database)
    >>> conn.begin()
    >>> for doc in ['region','BS']:
    ...    rs = conn.execute(u'DROP DOCUMENT "%s"' % doc)
    >>> conn.commit()
    True
    >>> conn.close()
