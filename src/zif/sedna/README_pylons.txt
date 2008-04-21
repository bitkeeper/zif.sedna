README_pylons.txt

This is simple (preliminary) instructions for using the sedna zope3 da in
Pylons.  Read through once before proceeding.  Suggestions for being more
"pylons-like" will be graciously accepted.

You need zope.rdb

# easy_install zope.rdb

This has a lot of recursive dependencies and will download a large chunk of the
zope world, but the important parts are ZODB (only the transaction part),
zope.rdb itself, zope.component, and zope.interface.  Use a virtualenv, if
you are not already.

You need zif.sedna

# easy_install zif.sedna

For transaction support, you want repoze.tm2

Yes, you want transaction support. If you do not do this, you will need to be
very diligent with begin() and commit()/rollback() on connections.  For thread
safety, the adapter blocks beginning transactions in a thread until the current
transaction is complete.

# easy_install -i http://dist.repoze.org/simple repoze.tm2

Do the "Configuring the Application" part of
http://blog.repoze.org/repoze.tm_with_pylons-20071218.html

For example, an app named testapp would have something like the following
stanzas in the paste .ini file

[pipeline:main]
pipeline=
    egg:Paste#cgitb
    egg:Paste#httpexceptions
    egg:repoze.tm2#tm
    test

[app:test]
use = egg:testapp
full_stack = false
cache_dir = %(here)s/data
beaker.session.key = testapp
beaker.session.secret = somesecret

Somewhere in a module that executes once at start-up (maybe environment.py),
you want the following:

# imports
import zope.component
from zope.rdb.interfaces import IZopeDatabaseAdapter
from zif.sedna.da import SednaAdapter

# get the zope.component site manager for component registration
sm = zope.component.getSiteManager()

Then, in the same file, register utilities for Sedna database access.  Here, we
register a database adapter utility named "testsedna".  You may register more
than one, maybe one with read-write and another with readonly, and maybe another
with a different Sedna database.  The last parameter is the name you will
access the particular connection by.

# sedna database adapter registration
# always "dbi://", then username:password@host:port/dbname
sm.registerUtility(SednaAdapter("dbi://SYSTEM:MANAGER@localhost:5050/test"),
    IZopeDatabaseAdapter,'testsedna')

In your application's controller code, use the following

# imports at module level
from zope.rdb.interfaces import IZopeDatabaseAdapter
import zope.component

Put the following in a method that accesses the database. Note the call () at
the end. 'testsedna' is the utility name by which you registered the connection,
above.

sednaConn = zope.component.getUtility(IZopeDatabaseAdapter,'testsedna')()

If you want to use this adapter in multiple locations in your code within
a transaction, you may want to write a database-connection-getter so that
second and succeeding usages do not block.  Current policy is one connection
per thread, and the block is there for thread safety. Here is an example.

def getDb():
    if not isinstance(c.dbconn,str):
        return c.dbconn
    else:
        c.dbconn = zope.component.getUtility(IZopeDatabaseAdapter,'db')()
        return getDb()

Then,

    sednaConn = getDb()

This is a connection, much like any other database adapter.

Obtain a cursor

    c = sednaConn.cursor()

and do some queries.  Here, we use elementtree syntax to put Chapter 1 of
Genesis into a page.  body is the 'body' element of the page we are generating.

    res = c.execute(u'doc("ot")/tstmt/bookcoll[1]/book[1]/chapter[1]/v/text()')
    theList = c.fetchall()
    ol = SubElement(body,'ol')
    for k in theList:
        p = SubElement(ol,'li')
        p.text = k.strip()

fetchall() is one way of doing this; you may also iterate the result directly.

    res = c.execute(u'doc("ot")/tstmt/bookcoll[1]/book[1]/chapter[1]/v/text()')
    ol = SubElement(body,'ol')
    for k in res:
        p = SubElement(ol,'li')
        p.text = k.strip()

A query result may be a boolean for updates, inserts, etc.  Otherwise, it is
an iterable that produces python unicode strings.  Here, the xquery obtained
the text content, but we could have written the query without "text()" and
returned the full "v" elements and parsed them with an XML parser.

Generally, failing queries will raise an exception. The database adapter
takes care of begin(). repoze.tm takes care of commit() and rollback().
Generally, commit() is called by default, and rollback() is called when there
is an exception.

Take a look at sednaobject as well.  sednaobject classes are initialized with
a cursor and an XQuery string, and abstract many of the CRUD operations commonly
required.
