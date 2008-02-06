Sedna is a read-write xml storage.  The interface is a network socket, using
message-passing for queries and updates.   Queries are according to the
W3C XQuery standard.  Updates are an extension of XQuery.

Installing Sedna and XQuery syntax is beyond the scope of this document.  Sedna
has Apache 2.0 license and may be obtained from

http://modis.ispras.ru/sedna/

The tests here assume a running Sedna database on localhost named 'test' with
the default login, 'SYSTEM' and passwd, 'MANAGER'


    1.  start sedna governor

        $ se_gov

    2.  create database 'test' if necessary

        $ se_cdb test

    3.  start database 'test'

        $ se_sm test

Change these if necessary to match your system.
On \*nix you can also $ tailf [sedna-directory]/data/event.log to
monitor what the Sedna server is doing.

    >>> login = 'SYSTEM'
    >>> passwd = 'MANAGER'
    >>> db = 'test'
    >>> port = 5050
    >>> host = 'localhost'

Ordinarily, this statement will be "from zif.sedna import protocol"

    >>> import protocol

We open and close a connection:

    >>> conn = protocol.SednaProtocol(host,db,login,passwd,port)
    >>> conn.close()

If login fails, you get an OperationalError.

    >>> bad_passwd = 'hello'
    >>> conn = protocol.SednaProtocol(host,db,login,bad_passwd,port)
    Traceback (most recent call last):
    ...
    OperationalError: [226] SEDNA Message: ERROR SE3053
    Authentication failed.


Let's start with an xquery that does not need to access any documents. The
result of a query is an iterator that may only be accessed once.  result.value
empties that iterator into a single python unicode string. You may iterate the
result and hold it in a list, or use the items as they are generated.  Items
in a result are python unicode strings, unless it makes sense for the result
to be a boolean, e.g., updates, inserts, deletes. zif.sedna's protocol will
send begin() to start a transaction automatically if necessary.  You may
execute multiple queries in a transaction, but transactions must be committed
or rolled back before the connection is closed.

    >>> conn = protocol.SednaProtocol(host,db,login,passwd,port)
    >>> result = conn.execute(u'for $i in (1,2,3) return <z>{$i}</z>')
    >>> print result.value
    <z>1</z>
    <z>2</z>
    <z>3</z>
    >>> result.value
    u''
    >>> result = conn.execute(u'for $i in (1,2,3) return <z>{$i}</z>')
    >>> res = list(result)
    >>> print res
    [u'<z>1</z>', u'\n<z>2</z>', u'\n<z>3</z>']
    >>> conn.commit()
    True

Internally, Sedna stores documents and processes queries in utf-8. The
zif.sedna protocol expects queries to be python unicode strings, which are
converted to utf-8 for processing. Any query other than a python unicode string
will raise a ProgrammingError.

    >>> result = conn.execute('for $i in (1,2,3) return <z>{$i}</z>')
    Traceback (most recent call last):
    ...
    ProgrammingError: Expected unicode, got <type 'str'>.

Let's bulk load a file so we have some data to work with. Since the "region"
folder is local to this module, a relative path will work to specify this file.
In practice, we will need to use an absolute path. If loading fails, it
raises a DatabaseError.

For the list of documents and collections in the current database, we use
connection.documents

    >>> conn = protocol.SednaProtocol(host,db,login,passwd,port)
    >>> db_docs = conn.documents
    >>> if not 'testx_region' in db_docs:
    ...     z = conn.execute(u'LOAD "example/region.xml" "testx_region"')
    >>> conn.commit()
    True

Equivalently, this could have been written:

    conn.loadFile('example/region.xml','testx_region')

If we try to load this file again with the same name, we get an error.

    >>> z = conn.loadFile('example/region.xml','testx_region')
    Traceback (most recent call last):
    ...
    DatabaseError: [163] SEDNA Message: ERROR SE2001
    Document with the same name already exists.

Let's see what's in the document. Note that the resulting output is nicely
formatted.  This is done with leading space and following newline ('\\n')
characters in each line of the result.  Since this is XML, they are just there
for output formatting and are not really in the document.

    >>> result = conn.execute(u'doc("testx_region")/*/*')
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

Extra spaces and newlines may be turned off inside a query with a declaration
provided by Sedna.

    >>> ns = u'declare namespace se = "http://www.modis.ispras.ru/sedna";'
    >>> newquery=ns+'declare option se:output "indent=no";'
    >>> newquery = newquery + 'document("testx_region")/*/asia'
    >>> result = conn.execute(newquery)
    >>> print result.value
    <asia><id_region>asi</id_region></asia>

XQuery lets you get just part of the document. Note that 'doc' and 'document'
are synonymous.

    >>> data = conn.execute(u'doc("testx_region")//*[id_region="eur"]')
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
    >>> result = conn.loadText(string1, 'BS')

If we did not get any exceptions, the document is loaded.  Let's do a query
for books with price > 30.  We'll iterate the result and print the items.  We
strip() the individual results to remove trailing newline characters.

    >>> result = conn.execute(u'document("BS")//book[price>30]')
    >>> for item in result:
    ...     print item.strip()
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

Empty results return an empty string.

    >>> result = conn.execute(u'document("BS")//book[price>300]')
    >>> result.value
    u''

Querying for an element that does not exist returns an empty result, not an
exception.

    >>> result = conn.execute(u'document("BS")/BS/book[9]')
    >>> result.value
    u''

Hmmm. Can we retrieve an item from a list based on a previous selection?
Yes, we can.  This is interesting, since this means we can get back to this
item if we want to update it.

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


Let's see how long that took.

    >>> z = result.time

Sorry, can't show you the value here. You will have to try it yourself.

    >>> z.endswith('secs')
    True
    >>> conn.commit()
    True

Here's a query longer than 10240 bytes.  It will go through anyway.

    >>> result = conn.execute(
    ... u'document("BS")//book[price>300]'+' '*15000)
    >>> result.value
    u''

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
into the item.  We also look at mixed-mode element handling here.

    >>> ins = '<quality>Great <i>happy </i>quality</quality>'
    >>> qry2 = 'UPDATE insert %s into %s' % (ins,qry)
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

    >>> conn = protocol.SednaProtocol(host,db,login,passwd,port)
    >>> conn.begin()
    >>> qry = u'document("BS")//book[title="Learning XML"]/quality'
    >>> result = conn.execute(qry)

We have a <quality> element in the book. Let's delete it.

    >>> print result.value
    <quality>Great <i>happy </i>quality</quality>
    >>> qry2 = u'UPDATE delete %s' % qry
    >>> result = conn.execute(qry2)
    >>> data = conn.execute(qry)

Now, it's gone

    >>> data.value
    u''

We rollback

    >>> conn.rollback()
    True
    >>> conn.close()

We reopen the database just to be sure that we are not looking at a cache.

    >>> conn = protocol.SednaProtocol(host,db,login,passwd,port)
    >>> conn.begin()
    >>> data = conn.execute(qry)

The <quality> element is back! Rollback successful!

    >>> print data.value
    <quality>Great <i>happy </i>quality</quality>

We've done update and delete.  Now, let's do a "replace".  We raise the price
10% on all of the books.

    >>> qry0 = u'document("BS")//book/price'
    >>> qry = "UPDATE replace $price in " + qry0 +\
    ... """ with
    ... <price>{round-half-to-even($price * 1.1,2)}</price>
    ... """
    >>> data = conn.execute(qry)
    >>> data
    True
    >>> data = conn.execute(qry0)
    >>> print data.value
    <price>33</price>
    <price>32.99</price>
    <price>54.99</price>
    <price>43.95</price>

Sedna also provides statements for "UPDATE delete_undeep" and "UPDATE rename".
Consult the Sedna documentation for instructions on these and for additional
information about Sedna - indexing, ODBC inside XQuery, user management,
database exports, triggers, etc.

Before closing this connection, let's see what the other output format looks
like. The default format is 0, XML.  1 gives us SXML.  Maybe useful if you have
an SXML parser.  It is smaller...

    >>> qry = u'document("BS")//book[title="Learning XML"]'
    >>> data = conn.execute(qry,format=1)
    >>> print data.value
    (book(@ (category  "WEB"))
     (quality"Great "(i"happy ")"quality")
     (title(@ (lang  "en"))"Learning XML")
     (author"Erik T. Ray")
     (year"2003")
     (price"43.95")
    )
    >>> conn.commit()
    True
    >>> conn.close()
    >>> conn.closed
    True

Starting a new connection here.

    >>> conn = protocol.SednaProtocol(host,db,login,passwd,port)

Error handling.  Let's try to catch a DatabaseError.  Error definitions are
available in the connection object per PEP-249. This query should be an XQuery
syntax error, so will be caught right when the query is sent.

    >>> try:
    ...     result = conn.execute(u'hello world')
    ... except conn.DatabaseError,e:
    ...     print e
    [3] SEDNA Message: ERROR XPST0003
        It is a static error if an expression is not a valid instance of the grammar defined in A.1 EBNF.
    Details: syntax error at token: 'world', line: 1

Now for errors in 'valid' but troublesome queries, errors that happen while the
result is being generated.

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
    Traceback (most recent call last):
    ...
    DatabaseError: [112] SEDNA Message: ERROR FORG0001
        Invalid value for cast/constructor.
    Details: Cannot convert to xs:integer type

We get an error, but this is not as helpful as it can be.  We set debugOn to
get a bit more info.

We turn on debug messages.

    >>> conn.debugOn()

Retry the same query. Now, when we get the traceback, there is a bit more info
that is maybe helpful.

    >>> result = conn.execute(qry)
    Traceback (most recent call last):
    ...
    DatabaseError: PPCast : 1
    PPFunCall : 1 : http://www.w3.org/2005/xquery-local-functions:f
    [112] SEDNA Message: ERROR FORG0001
        Invalid value for cast/constructor.
    Details: Cannot convert to xs:integer type

    >>> conn.debugOff()

For a full idea of the client-server communication, we can do tracing. First,
we need to configure logging. We'll log to stdout here.

    >>> import logging
    >>> import sys
    >>> logging.basicConfig(stream=sys.stdout)
    >>> log = logging.getLogger()
    >>> log.setLevel(logging.INFO)

Tracing gives a representation of the internal client-server interaction.
Tracing happens at logging.INFO level. (C) messages are sent by the client,
and (S) messages are the server's response. We see the client sending
the query, and the server's response.  Here, we are seeing what the price would
look like of we raise it another 10% on the "Learning XML" book.

    >>> conn.traceOn()

    >>> qry = u'''for $item in document("BS")//book
    ... let $price := round-half-to-even($item/price * 1.1,2)
    ... where $item/title = "Learning XML"
    ... return <price>{$price}</price>'''
    >>> data = conn.execute(qry)
    INFO:root:(C) SEDNA_BEGIN_TRANSACTION
    INFO:root:(S) SEDNA_BEGIN_TRANSACTION_OK
    INFO:root:(C) SEDNA_EXECUTE for $item in document("BS")//book
    let $price := round-half-to-even($item/price * 1.1,2)
    where $item/title = "Learning XML"
    return <price>{$price}</price>
    INFO:root:(S) SEDNA_QUERY_SUCCEEDED
    INFO:root:(S) SEDNA_ITEM_PART <price>48.35</price>
    INFO:root:(S) SEDNA_ITEM_END

    >>> print data.value
    INFO:root:(C) SEDNA_GET_NEXT_ITEM
    INFO:root:(S) SEDNA_RESULT_END
    <price>48.35</price>

We turn tracing off and commit the session.

    >>> conn.traceOff()
    >>> conn.commit()
    True
    >>> conn.close()

Reset the log level.

    >>> log.setLevel(logging.ERROR)

Final cleanup. We'll remove the documents we created.

    >>> conn = protocol.SednaProtocol(host,db,login,passwd,port)
    >>> conn.begin()
    >>> for doc in ['testx_region','BS']:
    ...    rs = conn.execute(u'DROP DOCUMENT "%s"' % doc)
    >>> conn.commit()
    True
    >>> conn.close()
