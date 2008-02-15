README_da.txt

This is simple instructions for using the da in a zope3 installation.

You need to require zope.rdb in your app's configure.zcml.

<include package="zope.rdb" file="meta.zcml" />
<include package="zope.rdb" />

Add a namespace to your <configure directive:
    xmlns:rdb="http://namespaces.zope.org/rdb"

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

and do some queries.  Here, we use elementtree syntax to put Chapter 1 of
Genesis into a page.  self.body is the 'body' element of the page.

    res = c.execute(u'doc("ot")/tstmt/bookcoll[1]/book[1]/chapter[1]/v/text()')
    theList = c.fetchall()
    ol = SubElement(self.body,'ol')
    for k in theList:
        p = SubElement(ol,'li')
        p.text = k.strip()

fetchall() is not necessary; you may iterate the result directly.

    res = c.execute(u'doc("ot")/tstmt/bookcoll[1]/book[1]/chapter[1]/v/text()')
    ol = SubElement(self.body,'ol')
    for k in res:
        p = SubElement(ol,'li')
        p.text = k.strip()

query result may be a boolean for updates, inserts, etc.  Otherwise, it is
an iterable that produces python unicode strings.  Here, the xquery obtained
the text content, but we could have written the query without "text()" and
returned the full "v" elements and parsed them with an XML parser.

Generally, failing queries will raise an exception.  Zope takes care of
pooling connections and begin(), commit() / rollback().

