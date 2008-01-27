README.da

This is simple instructions for using the da in a zope3 installation.

You need to require zope.rdb in your app's configure.zcml.

<include package="zope.rdb" file="meta.zcml" />
<include package="zope.rdb" />

Then, you can do an rdb connection to sedna by dsn. For example:

<rdb:provideConnection
    name="testsedna"
    component="zif.sedna.da.SednaAdapter"
    dsn="dbi://SYSTEM:MANAGER@localhost:5050/test"
    />

From there, in application code, use

    from zope.rdb.interfaces import IZopeDatabaseAdapter
    from zope.component import getUtility
    sedna = getUtility(IZopeDatabaseAdapter,'testsedna')()

to obtain a handle, just like any other database adapter.

Obtain a cursor

    c = sedna.cursor()

and do some queries.  Here, we use zif.elements to put Chapter 1 of Genesis into
a page.

    res = c.execute('doc("ot")/tstmt/bookcoll[1]/book[1]/chapter[1]/v/text()')
    theList = c.fetchall()
    ol = SubElement(self.body,'ol')
    for k in theList:
        p = SubElement(ol,'li')
        p.text = k.strip()

fetchall() is not necessary; you can iterate the result directly.

    res = c.execute('doc("ot")/tstmt/bookcoll[1]/book[1]/chapter[1]/v/text()')
    ol = SubElement(self.body,'ol')
    for k in res:
        p = SubElement(ol,'li')
        p.text = k.strip()

query result may be a boolean for updates, inserts, etc.  Otherwise, it is
unicode text.  Here, we just got the text content in the query, but we could
have returned the full "v" elements and parsed them with an XML parser.

Generally, failing queries will raise an exception.  Zope takes care of
pooling connections and begin(), commit() / rollback().

