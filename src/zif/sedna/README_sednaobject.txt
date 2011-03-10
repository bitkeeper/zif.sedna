=====================
zif.sedna.sednaobject
=====================

sednaobject has a few classes: SednaXQuery, SednaContainer, and
SednaObjectifiedElement, that abstract fetches and updates for a Sedna server.

SednaXQuery is for readonly query results.  It provides list-like behavior.
Access query result items by index, slice, or iteration.  It's for operations
like working with the results of a SELECT in SQL.

SednaContainer provides a read-write elementtree-like interface to a single
element and its children.  It provides mutable access to children by index.
It's for operations like working with a table or view in SQL.

SednaObjectifiedElement is a thin wrapper around lxml.objectify for a single
(object-like) element.  It's for operations like working with a record in SQL.

sednaobject requires lxml.  Items based on lxml Element are fully supported, so
functionality provided by lxml.etree and lxml.objectify may be used for item
creation and editing.  Plain-text XML may also be used.

sednaobject is transparent with regard to XML namespaces.  Namespace handling
is something your application and the Sedna server do.

We'll start with the usual test document in the test database:

    >>> login = 'SYSTEM'
    >>> passwd = 'MANAGER'
    >>> db = 'test'
    >>> port = 5050
    >>> host = 'localhost'

    >>> from zif.sedna import protocol
    >>> conn = protocol.SednaProtocol(host,db,login,passwd,port)
    >>> db_docs = conn.documents
    >>> import inspect
    >>> localfile = inspect.getsourcefile(protocol.SednaProtocol)
    >>> import os
    >>> loc = os.path.realpath(localfile)
    >>> path = os.path.split(loc)[0]
    >>> filepath = os.path.join(path,'example','region.xml')
    >>> if not 'testx_region' in db_docs:
    ...     z = conn.loadFile(filepath, "testx_region")
    >>> conn.commit()
    True

zif.sedna.sednaobject.SednaXQuery
--------------------------------

SednaXQuery is a class intended to abstract XQuery results to provide pythonic
sequence methods.  SednaXQuery results are readonly, so this class mainly
provides length, indexed access, and slicing.

This class provides functionality similar to working with the results of a
SELECT in SQL.

Initialize a SednaXQuery with a cursor, and an XQuery or XPath expression.  You
may provide a parser that will be used on each result item.  There is a bit more
about using a parser later.

    >>> from zif.sedna.sednaobject import SednaXQuery
    >>> curs = conn.cursor()
    >>> expr = u"doc('testx_region')/regions/*"
    >>> z = SednaXQuery(curs,expr,pretty_print=True)
    >>> def null_parse(s):
    ...     return s
    >>> y = SednaXQuery(curs,expr,parser=null_parse, pretty_print=True)

Get the length of the result:

    >>> len(z)
    6
    >>> y.count()
    6

Print the result in one shot.  To obtain this into a variable, use str().

    >>> print(z) #doctest: +NORMALIZE_WHITESPACE
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

    >>> z[0] #doctest: +NORMALIZE_WHITESPACE
    u'<africa>\n <id_region>afr</id_region>\n</africa>'
    >>> z[-1] #doctest: +NORMALIZE_WHITESPACE
    u'<samerica>\n <id_region>sam</id_region>\n</samerica>'
    >>> z[9]
    Traceback (most recent call last):
    ...
    IndexError: list index out of range

Do the "in" thing:

    >>> z[0] in z
    True
    >>> from lxml.etree import tostring, fromstring
    >>> d = fromstring(z[2])
    >>> d in z
    True
    >>> tostring(d) in z
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

    >>> for item in z[2:4]: #doctest: +NORMALIZE_WHITESPACE
    ...     print(item)
    <australia>
     <id_region>aus</id_region>
    </australia>
    <BLANKLINE>
    <europe>
     <id_region>eur</id_region>
    </europe>
    >>> for item in z[4:]: #doctest: +NORMALIZE_WHITESPACE
    ...     print(item)
    <namerica>
     <id_region>nam</id_region>
    </namerica>
    <BLANKLINE>
    <samerica>
     <id_region>sam</id_region>
    </samerica>
    >>> for item in z[:2]: #doctest: +NORMALIZE_WHITESPACE
    ...     print(item)
    <africa>
     <id_region>afr</id_region>
    </africa>
    <BLANKLINE>
    <asia>
     <id_region>asi</id_region>
    </asia>
    >>> z[-2:] ==  z[4:]
    True

Do list comprehension.  Note that this retrieves the entire set from the
server while iterating.  Provide an XQuery with a "where" clause if you want
the server to do the "if" for you.

    >>> y = [item for item in z if 'samerica' in item]
    >>> print(y[0].lstrip()) #doctest: +NORMALIZE_WHITESPACE
    <samerica>
     <id_region>sam</id_region>
    </samerica>

Get the index of an item:

    >>> u = z[3]
    >>> z.index(u)
    3

Enumerate:
    >>> s = [(idx,value) for idx,value in enumerate(z)]
    >>> s[-1] #doctest: +NORMALIZE_WHITESPACE
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
    >>> s[-1] #doctest: +NORMALIZE_WHITESPACE
    (6, u'\n<samerica>\n <id_region>sam</id_region>\n</samerica>')

You are not restricted to pure XPath expressions; most XQuery expressions that
return iterables will work fine:

    >>> q = u" for $i in doc('testx_region')/regions/* "
    >>> q += u" return $i/id_region/text() "
    >>> z = SednaXQuery(curs,q)
    >>> len(z)
    6
    >>> z[0]
    u'afr'
    >>> z[1] in z
    True
    >>> z[-2:]
    [u'nam', u'sam']

If you init the SednaXQuery object with a parser, results will be returned
parsed with that parser.  Good choices are lxml.etree.fromstring and
lxml.objectify.fromstring. Do not init with an XML parser if the results are
not XML.  You are not limited to XML parsers.

    >>> z = SednaXQuery(curs,q,parser=fromstring)
    >>> z[0]
    Traceback (most recent call last):
    ...
    XMLSyntaxError: Start tag expected, '<' not found, line 1, column 1
    >>> expr = u"doc('testx_region')/regions"
    >>> z = SednaXQuery(curs,expr,parser=fromstring)
    >>> z[0].tag
    'regions'
    >>> expr = u"doc('testx_region')/regions/*"
    >>> z = SednaXQuery(curs,expr,parser=fromstring)
    >>> type(z[0])
    <type 'lxml.etree._Element'>
    >>> [item.tag for item in z]
    ['africa', 'asia', 'australia', 'europe', 'namerica', 'samerica']
    >>> [item.tag for item in z[2:4]]
    ['australia', 'europe']


zif.sedna.sednaobject.SednaContainer
----------------------------------

SednaContainer is a class intended to abstract an Element on the server to
provide elementtree-like methods, particularly element information and
modification for persistence. This is a read-write interface and very handy for
container elements.

This class provides functionality similar to operating on a table or view
in SQL.

Initialize a SednaContainer with a cursor and an XPath expression:

    >>> from zif.sedna.sednaobject import SednaContainer
    >>> curs = conn.cursor()
    >>> path = u"doc('testx_region')/regions"
    >>> z = SednaContainer(curs, path, pretty_print=True)

It is a value error if the expression returns more than one element.

    >>> path = u"doc('testx_region')/regions/*"
    >>> t = SednaContainer(curs, path)
    Traceback (most recent call last):
    ...
    ValueError: Cannot init SednaContainer with multiple elements.

It is a LookupError if the expression returns no elements.

    >>> path = u"doc('testx_region')/notpresent"
    >>> t = SednaContainer(curs,path)
    Traceback (most recent call last):
    ...
    LookupError: The path did not return an element. ([0] might need to be [1]?)

Len provides the number of child elements.

    >>> len(z)
    6

Obtain the element in one shot:

    >>> k = str(z)
    >>> print(k) #doctest: +NORMALIZE_WHITESPACE
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

Item access works as with SednaXQuery, except you get the items within the
Element instead of the items of the list returned by the query:

    >>> z[0] #doctest: +NORMALIZE_WHITESPACE
    u'<africa>\n <id_region>afr</id_region>\n</africa>'
    >>> z[-1] in z
    True
    >>> z[0] in z
    True
    >>> z[3:4] #doctest: +NORMALIZE_WHITESPACE
    [u'<europe>\n <id_region>eur</id_region>\n</europe>']
    >>> z.xindex(z[-2])
    5

Some elementtree functions work.  Setting an attribute reads and rewrites the
entire item, so do this sparingly:

    >>> z.tag
    'regions'
    >>> z.attrib
    {}
    >>> z.set('attr', 'something')
    >>> z.attrib
    {'attr': 'something'}
    >>> z.get('attr')
    u'something'

Sometimes, you have a somewhat atomic element, and just want to replace the
entire item with an update.

    >>> idx = z.xindex(z[0])
    >>> p = z.path + '/' + '*[%s]' % idx
    >>> t = SednaContainer(z.cursor,p)
    >>> print(t)
    <africa><id_region>afr</id_region></africa>
    >>> t.replace('bob')
    Traceback (most recent call last):
    ...
    ValueError: Item is not well-formed.
    >>> item = fromstring(str(t))
    >>> from lxml.etree import SubElement
    >>> new_element = SubElement(item,'v',{'attr' : 'val'})
    >>> new_element.text = 'txt'
    >>> t.replace(item)
    >>> print(t)
    <africa><id_region>afr</id_region><v attr="val">txt</v></africa>

Check that z also has this value. Remember that z was instanciated with
pretty_print=True.

    >>> print(z[0]) #doctest: +NORMALIZE_WHITESPACE
    <africa>
     <id_region>afr</id_region>
     <v attr="val">txt</v>
    </africa>

The list of sub-elements is mutable. Assign a new item at an index.  Sub-elements
must be well-formed.  Note that we list the enumeration here.  There is only one
database iterator at a time for a cursor.

   >>> t = fromstring(z[1])
   >>> t.xpath('id_region')[0].text = 'asia'
   >>> z[1] = t
   >>> print(z[1]) #doctest: +NORMALIZE_WHITESPACE
   <asia>
    <id_region>asia</id_region>
   </asia>
   >>> for idx, item in list(enumerate(z)):
   ...     t = fromstring(item)
   ...     t.xpath('id_region')[0].tag = 'region_id'
   ...     z[idx] = t
   >>> print(z[2]) #doctest: +NORMALIZE_WHITESPACE
   <australia>
    <region_id>aus</region_id>
   </australia>
   >>> z[0] = 'fred'
   Traceback (most recent call last):
   ...
   ValueError: Item is not well-formed.

At this point we reinstanciate z without pretty_print so that formatting
does not get in the way of the examples.

    >>> expr = u"doc('testx_region')/regions"
    >>> z = SednaContainer(curs,expr)

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
   >>> z.append('hello')
   Traceback (most recent call last):
   ...
   ValueError: Item is not well-formed.
   >>> z.remove('hello')
   Traceback (most recent call last):
   ...
   ValueError: Item is not well-formed.
   >>> z.remove(t)
   >>> len(z)
   6
   >>> z[-1]
   u'<samerica><region_id>sam</region_id></samerica>'
   >>> s = z[3]
   >>> print(s)
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
   >>> j = z[:]
   >>> len(j)
   6
   >>> isinstance(j,list)
   True

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

del works.

   >>> t = z[-1]
   >>> t
   u'<samerica><region_id>sam</region_id></samerica>'
   >>> z.index(t)
   5
   >>> del z[0]
   >>> z[0]
   u'<asia><region_id>asia</region_id></asia>'
   >>> z.index(t)
   4
   >>> del z[-1]
   >>> z[-1]
   u'<europe><region_id>eur</region_id></europe>'
   >>> len(z)
   4

Slice modification is unsupported. This is the python2.5 message.  The
message differs for 2.4, so the statement is commented out.

   >>> #del z[:]
   Traceback (most recent call last):
   ...
   TypeError: unsupported operand type(s) for +: 'slice' and 'int'

Extend works.

    >>> len(z)
    4
    >>> t = [z[0],z[1],z[2]]
    >>> z.extend(t)
    >>> len(z)
    7

Note that "index" refers to the first appearance of an item by value, so the
following is correct.

    >>> z.index(z[-1])
    2

You may obtain the path SednaContainer was initialized with.
    >>> z.path
    u"doc('testx_region')/regions"

It is sometimes handy to obtain the parent of an element.  When getparent()
returns None, you are at root. .parent is a synonym.  The parent returned
is a SednaContainer.

    >>> p = z.path + '/' + '*[1]'
    >>> t = SednaContainer(z.cursor,p)
    >>> t.tag
    'asia'
    >>> s = t.parent
    >>> s.tag
    'regions'
    >>> s = t.getparent()
    >>> s.tag
    'regions'
    >>> isinstance(s,SednaContainer)
    True
    >>> f = s.getparent()
    >>> f is None
    True

If you init a SednaContainer with a parser, returned items will be parsed with
that parser. "fromstring" here is lxml.etree.fromstring
    >>> path = u"doc('testx_region')/regions"
    >>> z = SednaContainer(curs,path,parser=fromstring)
    >>> [item.tag for item in z]
    ['asia', 'australia', 'namerica', 'europe', 'asia', 'australia', 'namerica']
    >>> z[0].tag
    'asia'

Here, we use lxml.objectify.fromstring.  Just trying a bunch of things.  If you
want to work with a single object in this manner, you may consider using
SednaObjectifiedElement instead.

    >>> z = SednaContainer(curs,path,parser=objectify.fromstring)
    >>> [item.tag for item in z]
    ['asia', 'australia', 'namerica', 'europe', 'asia', 'australia', 'namerica']
    >>> z[1].tag
    'australia'
    >>> z[1].region_id
    'aus'
    >>> t = z[1]
    >>> t.region_id = 'aut'
    >>> t.city = "Canberra"
    >>> t['animal'] = 'kangaroo'
    >>> t.fun_words = ["g'day","barbie", "sheila"]
    >>> t['arb_list'] = [u'\u20ac (euro symbol)',3,False,4.0,-25, None]
    >>> t.arb_list = t.arb_list[:] + [True, 'true']
    >>> if not len(t.xpath('contact')):
    ...     dummy = SubElement(t,'contact')
    >>> if not len(t.xpath("contact/name")):
    ...     dummy = SubElement(t.contact,'name')
    >>> t.contact.name.last = 'Hogan'
    >>> t.contact.name.first = 'Paul'
    >>> z[1] = t
    >>> m = z[1]
    >>> m.region_id
    'aut'
    >>> m.city
    'Canberra'
    >>> m.fun_words
    "g'day"
    >>> list(m['fun_words'])
    ["g'day", 'barbie', 'sheila']
    >>> m.fun_words[:]
    ["g'day", 'barbie', 'sheila']
    >>> list(m.fun_words)
    ["g'day", 'barbie', 'sheila']
    >>> m['city']
    'Canberra'
    >>> m.animal
    'kangaroo'
    >>> m.arb_list[:]
    [u'\u20ac (euro symbol)', 3, False, 4.0, -25, None, True, 'true']
    >>> '%s, %s' % (m.contact.name.last, m.contact.name.first)
    'Hogan, Paul'

Let's create an objectified element from scratch and replace the existing item.

    >>> t = objectify.Element('australia')
    >>> t.region_id = 'aus'
    >>> t.city = "Canberra"
    >>> t.fun_words = ["g'day","barbie", "sheila"]
    >>> if not len(t.xpath('contact')):
    ...     dummy = SubElement(t,'contact')
    >>> if not len(t.xpath("contact/name")):
    ...     dummy = SubElement(t.contact,'name')
    >>> t.contact.name.last = 'Hogan'
    >>> t.contact.name.first = 'Paul'
    >>> z[1] = t
    >>> print(tostring(z[1], pretty_print=True, encoding=unicode).strip())
    <australia xmlns:py="http://codespeak.net/lxml/objectify/pytype" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" py:pytype="TREE">
      <region_id py:pytype="str">aus</region_id>
      <city py:pytype="str">Canberra</city>
      <fun_words py:pytype="str">g'day</fun_words>
      <fun_words py:pytype="str">barbie</fun_words>
      <fun_words py:pytype="str">sheila</fun_words>
      <contact>
        <name>
          <last py:pytype="str">Hogan</last>
          <first py:pytype="str">Paul</first>
        </name>
      </contact>
    </australia>

zif.sedna.sednaobject.SednaObjectifiedElement
----------------------------------

SednaObjectifiedElement is a thin wrapper around lxml.objectify.

Initialize a SednaObjectifiedElement with a cursor and an XPath expression.
Path must refer to a single element that already exists in the database.

This class provides functionality similar to working with a record in SQL.

We'll pull in the item from the last example in SednaContainer:
    >>> from zif.sedna.sednaobject import SednaObjectifiedElement
    >>> q = u"doc('testx_region')/regions/"
    >>> q += '*[city="Canberra" and fun_words[contains(.,"barbie")]]'
    >>> t = SednaObjectifiedElement(curs,q)

Since this is just a wrapper around lxml.objectify, we do modifications as in
objectify. "_cursor", "_path", and "_element" are used internally by
SednaObjectifiedElement and cannot be set into the first level of the database
object.  The same problem exists for python reserved keywords.  The work-around
is to use dict notation.

    >>> t['years'] = 3
    >>> t.condition = 'Fine'
    >>> t['condition']
    'Fine'
    >>> t.years
    3
    >>> t.fun_words = ['one<>', 'two{}']
    >>> t.contact['name']['first'] = 'Fred'
    >>> t.store = 'Wal-Mart'
    >>> del t.store
    >>> t._cursor = 'Bob'
    Traceback (most recent call last):
    ...
    ValueError: Oops. _cursor, _path, _nsmap, and _element are used internally.
    >>> t['_cursor'] = 'Bob'

Important: save modifications.

    >>> t.save()

Now, we verify that the save persisted the modifications.

    >>> q = q.replace('barbie','two')
    >>> t = SednaContainer(curs,q, pretty_print=True)
    >>> print(t) #doctest: +NORMALIZE_WHITESPACE
    <australia xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
     <region_id xsi:type="xsd:string">aus</region_id>
     <city xsi:type="xsd:string">Canberra</city>
     <fun_words xsi:type="xsd:string">one&lt;&gt;</fun_words>
     <fun_words xsi:type="xsd:string">two{}</fun_words>
     <contact>
      <name>
       <last xsi:type="xsd:string">Hogan</last>
       <first xsi:type="xsd:string">Fred</first>
      </name>
     </contact>
     <years xsi:type="xsd:integer">3</years>
     <condition xsi:type="xsd:string">Fine</condition>
     <_cursor xsi:type="xsd:string">Bob</_cursor>
    </australia>


Check to assure that it round-trips OK.

    >>> t = SednaObjectifiedElement(curs,q)
    >>> t.tag
    'australia'
    >>> t.city
    'Canberra'
    >>> t.fun_words[:]
    ['one<>', 'two{}']
    >>> t.years
    3
    >>> t.contact.name.last
    'Hogan'
    >>> t['_cursor']
    'Bob'

Cleanup.  We delete the previously-created document and close the connection.

    >>> for doc in ['testx_region']:
    ...    rs = conn.execute('DROP DOCUMENT "%s"' % doc, pretty_print=True)
    >>> conn.commit()
    True
    >>> conn.close()
