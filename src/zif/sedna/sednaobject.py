#sednaobject
from lxml import objectify
from lxml.etree import _Element, tounicode, tostring,fromstring, XMLSyntaxError
from lxml.doctestcompare import norm_whitespace
from dbapiexceptions import DatabaseError

def checkobj(obj):
    if isinstance(obj,_Element):
        item = tounicode(obj)
    elif isinstance(obj,SednaElement):
        item = str(obj)
    else:
        item = obj
    return item

class SednaXPath(object):
    """class for read-only xpath queries.  Makes the query result sequence-like.
      slices and stuff...
    """
    def __init__(self,cursor,path):
        self.cursor = cursor
        while path.endswith('/'):
            path = path[:-1]
        self.path = path
        self._count = None
        self._attrib = None

    def count(self):
        if self._count is not None:
            return self._count
        q = u'let $i := %s' % (self.path)
        q += u' return <i><c>{count($i)}</c></i>'
        s = self.cursor.execute(q)
        f = objectify.fromstring(s.value)
        self._count = int(f.c)
        return self._count

    def xpath(self,path):
        if path.startswith('/'):
            base = self.path.split('/')[0]
            return SednaXPath(self.cursor,base+path)
        else:
            return SednaXPath(self.cursor,self.path + '/' + path)

    def _localKey(self,key):
        local = key+1
        count = self.count()
        if local < 1:
            local = count + local
        if local > count or local < 1:
            raise IndexError('list index out of range')
        return local

    def __getitem__(self,key):
        #key = self._localKey(key)
        if key < 0:
            key += self.count()
        return self[key:key+1][0]

    def index(self,obj):
        item = checkobj(obj)
        normed = norm_whitespace(item)
        q = u'declare namespace se = "http://www.modis.ispras.ru/sedna";'
        q += 'declare option se:output "indent=no";'
        q += u' %s' % self.path
        s = self.cursor.execute(q)
        count = -1
        found = False
        for k in s:
            count += 1
            if norm_whitespace(k) == normed:
                found = True
                break
        if found:
            return count
        else:
            raise ValueError('item not in list')

    def xenumerate(self):
        for idx,value in enumerate(self):
            yield idx+1, value

    def xindex(self,obj):
        return self.index(obj) + 1

    def __contains__(self,obj):
        try:
            s = self.index(obj)
            return True
        except ValueError:
            return False


    def __getslice__(self,start,stop):
        #start,stop,step = key.indices(self.count())
        #if step <> 1:
        #    raise NotImplementedError('cannot do steps')
        rlen = stop - start
        rstart = self._localKey(start)
        q =  u'declare namespace se = "http://www.modis.ispras.ru/sedna";'
        q += u'declare option se:output "indent=no";'
        q += u'for $i in subsequence(%s,%s,%s) ' % (self.path,rstart,rlen)
        q += u'return $i'
        s = self.cursor.execute(q)
        return list(s)

    def __iter__(self):
        q = u' %s' % self.path
        s = self.cursor.execute(q)
        return s

    def __str__(self):
        q = u'%s' % self.path
        s = self.cursor.execute(q)
        return s.value

    def __len__(self):
        return self.count()


class SednaElement(SednaXPath):
    """a class to emulate read-write ElementTree functionality on an element in
       the Sedna database.  This is very handy for containerish elements.

    initialize with a cursor and a path to the element.

    """
    def __init__(self,cursor,path,check=True):
        """
        init the class with cursor and path
        set check to false to eliminate a server request, but only if you
        know what you are doing...
        """
        super(SednaElement,self).__init__(cursor,path)
        if check:
            self._checkElement()

    def _checkElement(self):
        """
        do a check to see that this is a single element
        """
        q = u'let $i := %s' % (self.path,)
        q += u' return <i><c>{count($i)}</c></i>'
        s = self.cursor.execute(q)
        f = objectify.fromstring(s.value)
        c = int(f.c)
        if c == 1:
            return
        elif c == 0:
            raise ValueError(
            'The path did not return an element. ([0] might need to be [1]?)')
        else:
            raise ValueError(
        'Cannot init SednaElement with multiple elements.')

    def getparent(self):
        """
        return parent as a SednaElement or None if at root
        """
        c = self.path + '/..'
        t = SednaElement(self.cursor,c)
        if t.tag is None:
            return None
        return t

    parent = property(getparent)

    def count(self,tag=None):
        if self._count is not None and tag is None:
            return self._count
        if tag:
            pt = tag
        else:
            pt = '*'
        q = u'let $i := %s/%s' % (self.path,pt)
        q += u' return <i><c>{count($i)}</c></i>'
        s = self.cursor.execute(q)
        f = objectify.fromstring(s.value)
        self._count = int(f.c)
        return self._count

    def append(self,obj):
        item = checkobj(obj)
        if self.count() > 0:
            q = u'update insert %s following %s/*[last()]' % (item,self.path)
        else:
            q = u'update insert %s into %s' % (item,self.path)
        self.cursor.execute(q)
        self._count = None

    def index(self,obj):
        item = checkobj(obj)
        normed = norm_whitespace(item)
        q = u'declare namespace se = "http://www.modis.ispras.ru/sedna";'
        q += 'declare option se:output "indent=no";'
        q += u' %s/*' % self.path
        s = self.cursor.execute(q)
        count = -1
        found = False
        for k in s:
            count += 1
            if norm_whitespace(k) == normed:
                found = True
                break
        if found:
            return count
        else:
            raise ValueError('item not in list')

    def extend(self,items):
        for item in items:
            self.append(item)
        self._count = None

    def insert(self,key,item):
        local = key+1
        count = self.count()
        self._count = None
        if local < 1:
            local = count + local
        if local > count:
            self.append(item)
            return
        elif local < 1:
            local = 1
        item = checkobj(item)
        if count > 0:
            q = u'update insert %s preceding %s/*[%s]' % (item,self.path,local)
            self.cursor.execute(q)
        else:
            self.append(item)


    def remove(self,obj):
        index = self.index(obj) + 1
        q = u'update delete %s/*[%s]' % (self.path,index)
        self.cursor.execute(q)
        self._count = None

    def __iter__(self):
        q = u' %s/*' % self.path
        s = self.cursor.execute(q)
        return s

    def __delitem__(self,key):
        local = self._localKey(key)
        q = u'update delete %s/*[%s]' % (self.path,local)
        self.cursor.execute(q)
        self._count = None

    @property
    def tag(self):
        q = u"let $i := %s return <t>{$i/name()}</t>" % self.path
        t1 = self.cursor.execute(q)
        r = fromstring(t1.value)
        return r.text
        #t = self.path.split('/')[-1]
        #t1 = t.split('[')[0]
        #return t1.strip()

    def __getitem__(self,key):
        """we get the item at index inside the element"""
        if isinstance(key,slice):
            start,stop,step = key.indices(self.count())
            if step <> 1:
                raise NotImplementedError('cannot do steps')
            return self.__getslice__(start,stop)
        local = self._localKey(key)
        q = u'declare namespace se = "http://www.modis.ispras.ru/sedna";'
        q += 'declare option se:output "indent=no";'
        q += u' %s/*[%s]' % (self.path,local)
        s = self.cursor.execute(q)
        return self.cursor.fetchone()

    def __setitem__(self,key,value):
        item = checkobj(value)
        local = self._localKey(key)
        q = u'update replace $i in %s/*[%s] ' % (self.path,local)
        q += ' with %s' % item
        try:
            s = self.cursor.execute(q)
        except DatabaseError:
            raise ValueError('Update failed.  Is the item well-formed?')

    def set(self,key,value):
        q = u'%s' % self.path
        s = self.cursor.execute(q)
        fromdb = self.cursor.fetchone()
        item = objectify.fromstring(fromdb)
        item.set(key,value)
        q = u'update replace $i in %s ' % (self.path,)
        q += ' with %s' % tounicode(item)
        self.cursor.execute(q)
        self._attrib = None

    def get(self,key):
        q = u'%s/data(@%s)' % (self.path,key)
        s = self.cursor.execute(q)
        t = s.value
        if t:
            return t
        else:
            raise KeyError("KeyError %s" % key)

    def replace(self,obj):
        """ replace item at self.path with the object"""
        item = checkobj(obj)
        q = u'update replace $i in %s ' % (self.path,)
        q += ' with %s' % (item,)
        try:
            self.cursor.execute(q)
        except DatabaseError:
            raise ValueError('Update failed.  Is the item well-formed?')
        self._attrib = None
        self._count = None

    def __getslice__(self,start,stop):
        #start,stop,step = key.indices(self.count())
        #if step <> 1:
        #    raise NotImplementedError('cannot do steps')
        rlen = stop - start
        rstart = self._localKey(start)
        q = u'declare namespace se = "http://www.modis.ispras.ru/sedna";'
        q += 'declare option se:output "indent=no";'
        q += u'for $i in subsequence(%s/*,%s,%s) ' % (self.path,rstart,rlen)
        q += 'return $i'
        s = self.cursor.execute(q)
        return list(s)

    @property
    def attrib(self):
        """get the attributes dict for the element
        """
        if self._attrib is not None:
            return self._attrib
        q = u' for $i in %s/@* ' % (self.path)
        q += u' let $nm := node-name($i), '
        q += u' $vl:= data($i)'
        q += u' return <d><k>{$nm}</k><v>{$vl}</v></d>'
        s = self.cursor.execute(q)
        attrs = {}
        for k in s:
            t = objectify.fromstring(k)
            attrs[str(t.k)] = str(t.v)
        self._attrib = attrs
        return self._attrib

    def keys(self):
        """ get the keys of the attributes
        """
        return self.attrib.keys()

    def values(self):
        return self.attrib.values()

    def items(self):
        return self.attrib.items()

