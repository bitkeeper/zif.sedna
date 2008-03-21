=====================
zif.sedna.sednaobject
=====================

sednaobject has a couple of classes to make interaction with the Sedna server a
bit more pythonic and friendly.

We'll start with the usual test document in the test database:

    >>> login = 'SYSTEM'
    >>> passwd = 'MANAGER'
    >>> db = 'test'
    >>> port = 5050
    >>> host = 'localhost'

    >>> import protocol
    >>> conn = protocol.SednaProtocol(host,db,login,passwd,port)
    >>> db_docs = conn.documents
    >>> if not 'testx_region' in db_docs:
    ...     z = conn.execute(u'LOAD "example/region.xml" "testx_region"')
    >>> conn.commit()
    True

zif.sedna.sednaobject.SednaXPath
--------------------------------

SednaXPath is a class intended to abstract XQuery results to provide pythonic
sequence methods.  XPath results are readonly, so this class mainly provides
length and accessors.

Initialize a SednaXPath with a cursor and an XQuery expression:

    >>> from sednaobject import SednaXPath
    >>> curs = conn.cursor()
    >>> expr = u"doc('testx_region')/regions/*"
    >>> z = SednaXPath(curs,expr)

Get the length of the result:

    >>> len(z)
    6
    >>> z.count()
    6

Print the result in one shot.  To obtain this into a variable, use str().

    >>> print z
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

Access by index:

    >>> z[0]
    u'<africa><id_region>afr</id_region></africa>'
    >>> z[-1]
    u'<samerica><id_region>sam</id_region></samerica>'
    >>> z[9]
    Traceback (most recent call last):
    ...
    IndexError: list index out of range

Do the "in" thing:

    >>> z[0] in z
    True
    >>> "<arbitrary>tag</arbitrary>" in z
    False

If we use lxml and parse to an Element, "in"  and "index" still work.

    >>> from lxml import objectify
    >>> item = objectify.fromstring(z[2])
    >>> item in z
    True
    >>> z.index(item)
    2
    >>> from lxml.etree import fromstring
    >>> item = fromstring(z[3])
    >>> item in z
    True
    >>> z.index(item)
    3

Slice:

    >>> for item in z[2:4]:
    ...     print item
    <australia><id_region>aus</id_region></australia>
    <europe><id_region>eur</id_region></europe>
    >>> for item in z[4:]:
    ...     print item
    <namerica><id_region>nam</id_region></namerica>
    <samerica><id_region>sam</id_region></samerica>
    >>> for item in z[:2]:
    ...     print item
    <africa><id_region>afr</id_region></africa>
    <asia><id_region>asi</id_region></asia>
    >>> z[-2:] ==  z[4:]
    True

Do list comprehension:

    >>> y = [item for item in z if 'samerica' in item]
    >>> print y[0].lstrip()
    <samerica>
     <id_region>sam</id_region>
    </samerica>

Get the index of an item:

    >>> u = z[3]
    >>> z.index(u)
    3

Enumerate:
    >>> s = [(idx,value) for idx,value in enumerate(z)]
    >>> s[-1]
    (5, u'\n<samerica>\n <id_region>sam</id_region>\n</samerica>')

This abstraction uses 0-based indexes.  XQuery uses 1-based indexes.  You can
get the server's index by using xindex.  This is handy if you need to construct
an expression for the server from the current path and the index:

    >>> u = z[3]
    >>> z.xindex(u)
    4

xenumerate similarly provides server indices.  Note that the semantics are a
bit different, since this is a method, not a built-in function.

    >>> s = [(idx,value) for idx,value in z.xenumerate()]
    >>> s[-1]
    (6, u'\n<samerica>\n <id_region>sam</id_region>\n</samerica>')

You are not restricted to pure XPath expressions; most XQuery expressions that
return iterables will work fine:

    >>> q = u" for $i in doc('testx_region')/regions/* "
    >>> q += u" return $i/id_region/text() "
    >>> z = SednaXPath(curs,q)
    >>> len(z)
    6
    >>> z[0]
    u'afr'
    >>> z[1] in z
    True
    >>> z[-2:]
    [u'nam', u'sam']

zif.sedna.sednaobject.SednaElement
----------------------------------

SednaElement is a class intended to abstract an Element on the server to
provide elementtree-like methods, particularly element information and
modification for persistence. This is a read-write interface and very handy for
container elements.  We do commits periodically here.  We want to show error
messages, and Sedna seems to be doing rolling back state on certain errors.

Initialize a SednaElement with a cursor and an XPath expression:

    >>> from sednaobject import SednaElement
    >>> curs = conn.cursor()
    >>> path = u"doc('testx_region')/regions"
    >>> z = SednaElement(curs,path)

It is an error if the expression returns more than one element.

    >>> path = u"doc('testx_region')/regions/*"
    >>> t = SednaElement(curs,path)
    Traceback (most recent call last):
    ...
    ValueError: Cannot init SednaElement with multiple elements.

Len provides the number of child elements.

    >>> len(z)
    6

Obtain the element in one shot:

    >>> k = str(z)
    >>> print k
    <regions>
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
    </regions>

Item access works as with SednaXPath, except you get the items within the
Element instead of the items of the list returned by the query:

    >>> z[0]
    u'<africa><id_region>afr</id_region></africa>'
    >>> z[-1] in z
    True
    >>> z[3:4]
    [u'<europe><id_region>eur</id_region></europe>']
    >>> z.xindex(z[-2])
    5

Some elementtree functions work.  Setting an attribute reads and rewrites the
entire item tree, so do this sparingly:

    >>> z.tag
    'regions'
    >>> z.attrib
    {}
    >>> z.set('attr', 'something')
    >>> z.attrib
    {'attr': 'something'}
    >>> z.get('attr')
    u'something'
    >>> z.cursor.connection.commit()
    True

Sometimes, you have a somewhat atomic element, and just want to replace it.

    >>> idx = z.xindex(z[0])
    >>> p = z.path + '/' + '*[%s]' % idx
    >>> t = SednaElement(z.cursor,p)
    >>> print t
    <africa>
     <id_region>afr</id_region>
    </africa>
    >>> t.replace('bob')
    Traceback (most recent call last):
    ...
    ValueError: Update failed.  Is the item well-formed?
    >>> item = fromstring(str(t))
    >>> from lxml.etree import SubElement
    >>> dummy = SubElement(item,'v',{'attr' : 'val'})
    >>> dummy.text = 'txt'
    >>> t.replace(item)
    >>> print t
    <africa>
     <id_region>afr</id_region>
     <v attr="val">txt</v>
    </africa>
    >>> print z[0]
    <africa><id_region>afr</id_region><v attr="val">txt</v></africa>

The list of subelements is mutable. Assign a new item at an index.  Subelements
must be well-formed.

   >>> t = fromstring(z[1])
   >>> t.xpath('id_region')[0].text = 'asia'
   >>> z[1] = t
   >>> print z[1]
   <asia><id_region>asia</id_region></asia>
   >>> for idx, item in list(enumerate(z)):
   ...     t = fromstring(item)
   ...     t.xpath('id_region')[0].tag = 'region_id'
   ...     z[idx] = t
   >>> print z[2]
   <australia><region_id>aus</region_id></australia>
   >>> z.cursor.connection.commit()
   True
   >>> z[0] = 'fred'
   Traceback (most recent call last):
   ...
   ValueError: Update failed.  Is the item well-formed?

Append, insert, and remove work.  Note that "remove" removes only the first
child whose normalized text representation matches the normalized text
representation of the item provided.

   >>> t = '<antarctica><region_id>ant</region_id></antarctica>'
   >>> len(z)
   6
   >>> z.append(t)
   >>> len(z)
   7
   >>> z[-1]
   u'<antarctica><region_id>ant</region_id></antarctica>'
   >>> z.remove('hello')
   Traceback (most recent call last):
   ...
   ValueError: item not in list
   >>> z.remove(t)
   >>> len(z)
   6
   >>> z[-1]
   u'<samerica><region_id>sam</region_id></samerica>'
   >>> s = z[3]
   >>> print s
   <europe><region_id>eur</region_id></europe>
   >>> z.remove(s)
   >>> len(z)
   5
   >>> z.insert(0,s)
   >>> len(z)
   6
   >>> z[0]
   u'<europe><region_id>eur</region_id></europe>'
   >>> z[1]
   u'<africa><region_id>afr</region_id><v attr="val">txt</v></africa>'

These functions work for lxml.etree Elements.

   >>> s = fromstring(s)
   >>> z.remove(s)
   >>> z[0]
   u'<africa><region_id>afr</region_id><v attr="val">txt</v></africa>'
   >>> z.insert(-1,s)
   >>> len(z)
   6
   >>> z[-2]
   u'<europe><region_id>eur</region_id></europe>'

del works, too
   >>> del z[0]
   >>> z[0]
   u'<asia><region_id>asia</region_id></asia>'
   >>> z[-1]
   u'<samerica><region_id>sam</region_id></samerica>'
   >>> del z[-1]
   >>> z[-1]
   u'<europe><region_id>eur</region_id></europe>'
   >>> len(z)
   4

Slice modification is unsupported.

   >>> del z[:]
   Traceback (most recent call last):
   ...
   TypeError: unsupported operand type(s) for +: 'slice' and 'int'

Extend works, though.
    >>> len(z)
    4
    >>> t = [z[0],z[1],z[2]]
    >>> z.extend(t)
    >>> len(z)
    7

It is sometimes handy to obtain the parent of the element.  When getparent()
returns None, you are at root. .parent is a synonym

    >>> p = z.path + '/' + '*[1]'
    >>> t = SednaElement(z.cursor,p)
    >>> t.tag
    'asia'
    >>> s = t.parent
    >>> s.tag
    'regions'
    >>> s = t.getparent()
    >>> s.tag
    'regions'
    >>> f = s.getparent()
    >>> f is None
    True

Cleanup.  We delete the previously-created documents and close the connection.

    >>> for doc in ['testx_region']:
    ...    rs = conn.execute(u'DROP DOCUMENT "%s"' % doc)
    >>> conn.commit()
    True
    >>> conn.close()
