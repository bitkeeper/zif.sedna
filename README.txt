****************
zif.sedna
****************

Sedna, available at http://modis.ispras.ru/sedna/, under Apache 2.0 license, is
a transactional native XML database operating over TCP/IP.  Sedna is open
source and has excellent documentation. The mailing list, [Sedna-discussion],
is responsive for questions. Sedna currently runs on Windows 2000/XP and
Linux x86, available in source and binary formats.

Analogous to an SQL database, a Sedna database is a set of related XML
documents and collections of XML documents. Documents hold data in an XML
structure, not restricted to any particular (e.g., tabular) format.  Collections
are used to organize documents with similar schemas so that those documents may
be queried together or separately.

A Sedna server may handle multiple databases.  A database may contain multiple
XML documents and multiple collections of multiple XML documents.  Data size is
unrestricted.

Analogous to an SQL database, data access is through a query language.  Sedna's
query language is XQuery, http://www.w3.org/TR/xquery/.  XQuery is more
like python or C or perl than like XML.  Particularly, XPath expressions are
like list generators, obtaining data elements by their type, value, and/or
path, and FLOWR expressions are like list comprehensions.  There are several
XQuery tutorials on the web.  Like SQL, XQuery may get complicated, but the
easy stuff is often powerful enough for your needs.

Sedna has extensions to XQuery for inserting, updating, deleting, etc., which
makes Sedna a worthy option for object persistence.  ZODB can store anything
picklable.  Similarly, Sedna can store anything that can be represented in XML.

Sedna has ACID transactions, triggers, indexes, support for ODBC within XQuery,
SQL database-like user/permission management, and many other interesting and
useful features.

zif.sedna provides

    a connection and query interface to a Sedna server

    a dbapi-like interface (e.g., connections and cursors)

    a database adapter for zope(3) with connection pooling and (provisional)
    thread safety.

    sednaobject, which provides pythonic interfaces to the Sedna server for
    CRUD operations. It abstracts read-only query results into python
    sequence-like items and also provides a read-write elementtree-like
    interface for managing individual elements and their descendents.

See 'src/zif/sedna/README.txt' for more information and doctest examples.
See 'src/zif/sedna/README_da.txt' to use the zope3 database adapter in zope.
See 'src/zif/sedna/README_sednaobject.txt' for sednaobject usage and doctests.
See 'src/zif/sedna/README_pylons.txt' to use the zope3 database adapter in
     pylons.

Releases
********

================
0.9 beta (2008/02/07)
================
Initial release

================
0.9 beta2 (2008/02/15)
================
Support pyformat %(var)s for atomic values in BasicCursor.
Improved thread friendliness.
Preliminary instructions for use with pylons.

================
0.10 alpha1 (2008/03/23)
================
sednaobject provided. Abstractions for pythonic CRUD operations.
lxml now required

