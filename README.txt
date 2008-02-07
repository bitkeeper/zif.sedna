****************
zif.sedna
****************

Sedna, available at http://modis.ispras.ru/sedna/, under Apache 2.0 license, is
a transactional "native" XML database operating over TCP/IP.  Sedna is open
source and has excellent documentation. The mailing list, [Sedna-discussion],
is responsive for questions. Sedna currently runs on Windows 2000/XP and
Linux x86, available in source and binary formats.

Analogous to an SQL database, a Sedna database is a set of related
XML documents and collections of XML documents.

A Sedna server may handle multiple databases.  A database may have multiple
XML documents and multiple collections of multiple XML documents.

Analogous to an SQL database, data access is through a query language.  Sedna's
query language is XQuery, http://www.w3.org/TR/xquery/.  Sedna has extensions
to XQuery for inserting, updating, deleting, etc.

Sedna has ACID transactions, triggers, support for ODBC within XQuery, and
many other interesting and useful features.

zif.sedna provides

    a connection and query interface to a Sedna server

    a dbapi-2.0-like interface (e.g., connections and cursors)

    a database adapter for zope(3) with connection pooling and (provisional)
    thread safety.

INSTALLATION NOTE: zif.sedna needs an elementtree implementation for some
functions, but not any one in particular, so setup will not install one for you.
lxml is recommended. lxml.objectify may be a useful addition to your toolkit for
handling XQuery results.

See 'src/zif/sedna/README.txt' for more information.
See 'src/zif/sedna/README_da.txt' to use the zope3 database adapter.

Releases
********

================
0.9 beta (2008/02/07)
================
