import sys

from decimal import Decimal

from copy_reg import _slotnames, dispatch_table
from types import ClassType, FunctionType, BuiltinFunctionType, TypeType, \
    NoneType, InstanceType
from base64 import b64encode, b64decode
from copy import copy,deepcopy

import gzip
from lxml.etree import Element, SubElement, tostring, fromstring, parse, \
    XMLSyntaxError, XMLParser
from cStringIO import StringIO
from persistent.interfaces import IPersistent
from zope.interface import implements, providedBy

class PickleError(Exception):
    """A common base class for the other pickling exceptions."""
    pass

class PicklingError(PickleError):
    """This exception is raised when an unpicklable object is passed to the
    dump() method.

    """
    pass

class UnpicklingError(PickleError):
    """This exception is raised when there is a problem unpickling an object,
    such as a security violation.

    Note that other exceptions may also be raised during unpickling, including
    (but not necessarily limited to) AttributeError, EOFError, ImportError,
    and IndexError.

    """
    pass

PKL_NAMESPACE="http://namespaces.zope.org/pickle"
OBJ_NAMESPACE="http://namespaces.zope.org/pyobj"
Z = '{%s}' % PKL_NAMESPACE
OBJ= '{%s}' % OBJ_NAMESPACE
PMAP = {'z':PKL_NAMESPACE}
NAMESPACES = {'z':PKL_NAMESPACE,None:OBJ_NAMESPACE}
OMAP={'o':OBJ_NAMESPACE}

base_element_name = 'pickle'

#_binary_char = re.compile("[^\n\t\r -\x7e]").search

from zope.interface import Interface

parser = XMLParser(ns_clean=True)

class IItemPickler(Interface):
    def dumps():
        """return an XML representation of item"""

class IItemUnpickler(Interface):
    def loads(str):
        """return an object represented by str"""

def strConvert(s):
    """
    if not valid in xml text, convert to base64
    """
    b64=False
    t = Element('x')
    try:
        t.text = s
    except AssertionError:
        s = b64encode(s)
        b64=True
    return s, b64

#localModules = ('__builtin__','__main__')
handled_modules = ('__builtin__',)
base_classes = set(['dict','list','str','unicode','tuple',
    'int','long','float','Decimal','complex','NoneType','bool'])
handled_types=(basestring,list,dict,tuple,float,complex,int)

def refgen():
    lst = 'abcdefghijklmnopqrstuvwxyz'
    lst += lst.upper()
    for a in lst:
        for b in lst:
            for c in lst:
                for d in lst:
                    yield '%s%s%s%s' % (a,b,c,d)

class XMLPickler(object):
    dispatch = {}
    def __init__(self,f=None):
        if f:
            self.file = f
        self.memo = {}
        self.refs = refgen()

    def memoize(self,obj,tag):
        try:
            ref = self.refs.next()
        except StopIteration:
            self.refs = refgen()
            ref = refs.next()
        objid = id(obj)
        self.memo[objid] = (ref,tag)

    def shouldMemoize(self,obj,tag):
        pass

    def asRef(self,obj_id, name=None,parent=None):
        if not name:
            name = Z+base_element_name
        ref_id,source = self.memo[obj_id]
        source.set(Z+'ref',str(ref_id))
        if parent is not None:
            elt = SubElement(parent,name,{Z+'refto':str(ref_id)},nsmap=PMAP)
        else:
            elt = Element(name,{Z+'refto':str(ref_id)},nsmap=PMAP)
        return elt

    def asClassElement(self,obj,name=None,parent=None):
        if not name:
            name = Z+base_element_name
        module = obj.__module__
        classname = obj.__name__
        if parent is not None:
            elt = SubElement(parent,name,{Z+'kls':classname},nsmap=PMAP)
        else:
            elt = Element(name,{Z+'kls':classname},nsmap=PMAP)
        if not module in handled_modules:
            elt.set(Z+'module',module)
        return elt

    def asFunctionElement(self,obj,name=None,parent=None):
        if not name:
            name = Z+base_element_name
        fname = obj.__name__
        # borrowed from gnosis utils
        for module_name,module in sys.modules.items():
            if name != '__main__' and \
                hasattr(module, fname) and \
                getattr(module, fname) is obj:
                break
        else:
            module_name = '__main__'
        module = module_name
        if parent is not None:
            elt = SubElement(parent,name,{Z+'fn':fname},nsmap=PMAP)
        else:
            elt = Element(name,{Z+'fn':fname},nsmap=PMAP)
        if not module in handled_modules:
            elt.set(Z+'module',module)
        return elt

    def asElement(self,obj,name=None,parent=None):
        """return an element representing obj.

        if name is provided, use name for the tag, else 'pickle' is used.
        """
        #if not isinstance(obj,InstanceType):
        if isinstance(obj,(ClassType)):
            return self.asClassElement(obj,name,parent=parent)
        elif isinstance(obj,(FunctionType, BuiltinFunctionType)):
            return self.asFunctionElement(obj,name,parent=parent)
        elif isinstance(obj,(TypeType)) and not obj.__name__ in base_classes:
            return self.asClassElement(obj,name,parent=parent)
        if name is None:
            name = Z + base_element_name
            nmap = NAMESPACES
        else:
            name = name
            nmap = PMAP
        needReduce = False
        try:
            klass = obj.__class__
        except AttributeError:
            # no class for this item.  use type and get reduction from
            # copy_reg
            klass = type(obj)
            needReduce = True

        klass_name = klass.__name__
        module = klass.__module__

        attrs = {}
        if not module in handled_modules:
            attrs[Z+'module']=module
        attrs[Z+'cls']=klass_name

        if parent is not None:
            elt = SubElement(parent,name,attrs,nsmap=nmap)
        else:
            elt = Element(name,attrs,nsmap=nmap)

        if klass_name in base_classes and module in handled_modules:
            self.dispatch[klass_name](self,obj,elt)
            return elt

        #if IPersistent.providedBy(obj):
            ## we handle Persistent items differently, I think, though
            ## this maybe will be handled in __getstate__/__setstate__
            #oid = obj._p_oid
            #if oid:
                #newId, b64 = strConvert(oid)
                #elt.set(Z+'oid',newId)
                #if b64:
                    #elt.set(Z+'oid_b64','true')
                #if not obj._p_changed:
                    #return elt
        if needReduce:
            self.getReduction(obj,elt)
            return elt
        d={}
        state=None

        if hasattr(obj,'__getstate__'):
            state = obj.__getstate__()
            if hasattr(obj,'__setstate__'):
                outtag = self.asElement(state,Z+'_state',parent=elt)
                self.memoize(state,outtag)
            else:
                d.update(state)
        if not (d or state):
            try:
                objdict = obj.__dict__
                d.update(objdict)
            except AttributeError:
                pass

        if not klass_name in base_classes:
            try:
                newArgs = obj.__getnewargs__()
            except AttributeError:
                newArgs = None
            if newArgs:
                outtag = self.asElement(newArgs,Z+'_newargs',parent=elt)
            else:
                try:
                    newArgs = obj.__getinitargs__()
                except AttributeError:
                    newArgs = None
                if newArgs:
                    outtag = self.asElement(newArgs,Z+'_newargs',parent=elt)
            # this may be a bad idea, but we'll set the contents of slots into
            # dict just to expose them for search.
            # actual pickle state for anything with __slots__ should be in
            # __getstate/__setstate__
            objslots=None
            try:
                objslots=_slotnames(klass)
                slotd = {}
                for key in objslots:
                    try:
                        value = getattr(obj,key)
                        slotd[key] = value
                    except AttributeError:
                        pass
                if objslots:
                    d.update(slotd)
            except AttributeError:
                pass

            if hasattr(obj,'__reduce__') and not (objslots or state):
                if not isinstance(obj,handled_types):
                    self.getReduction(obj,elt)
                # throw some useful text into date/datetime objects
                if klass_name in ('date','datetime'):
                    if module == 'datetime':
                        t = obj.isoformat()
                        elt.text = t
                if not issubclass(klass,handled_types):
                    return elt

        if d or state:
            # memoize instances, lists, and dicts.  tuples can cause trouble
            # inside reductions
            self.memoize(obj,elt)

        for key, value in d.items():
            vid = id(value)
            if vid in self.memo:
                self.asRef(vid,name=key,parent=elt)
            else:
                outputtag = self.asElement(value,key,parent=elt)
                if hasattr(value,'__dict__'):
                    self.memoize(value,outputtag)

        if isinstance(obj,basestring):
            self.handle_basestring(obj,elt)

        elif hasattr(obj,'__getitem__'):
            self.handle_sequence(obj,elt)

        elif isinstance(obj,bool):
            self.handle_bool(obj,elt)

        elif isinstance(obj,NoneType):
            self.handle_none(obj,elt)

        elif isinstance(obj,(int,long,float, Decimal)):
            self.handle_number(obj,elt)

        elif isinstance(obj,complex):
            self.handle_complex(obj,elt)

        obj = None
        return elt

    def handle_basestring(self,obj,elt):
        txt,b64 = strConvert(obj)
        if b64:
            elt.set(Z+'enc','base64')
        elt.text = txt
    dispatch['str'] = handle_basestring
    dispatch['unicode'] = handle_basestring

    def handle_sequence(self,obj,elt):
        if hasattr(obj,'keys') and hasattr(obj,'values'):
            self.getDictItems(obj,elt)
        else:
            self.getSequenceItems(obj,elt)

    def handle_bool(self,obj,elt):
        elt.text = str(obj).lower()
    dispatch['bool'] = handle_bool

    def handle_none(self,obj,elt):
        elt.text = str(obj).lower()
    dispatch['NoneType'] = handle_none

    def handle_number(self,obj,elt):
        elt.text = str(obj)
    dispatch['int'] = handle_number
    dispatch['long'] = handle_number
    dispatch['Decimal'] = handle_number
    dispatch['float'] = handle_number

    def handle_complex(self,obj,elt):
        self.asElement(obj.real,Z+'real',parent=elt)
        self.asElement(obj.imag,Z+'imag',parent=elt)
    dispatch['complex'] = handle_complex

    def getSequenceItems(self,obj,elt):
        if isinstance(obj,list):
            # memoize lists, not tuples
            self.memoize(obj,elt)
        for listitem in obj:
            vid = id(listitem)
            if vid in self.memo:
                self.asRef(vid,Z+'item',parent=elt)
            else:
                self.asElement(listitem,Z+'item',parent=elt)
    dispatch['list'] = getSequenceItems
    dispatch['tuple'] = getSequenceItems

    def getDictItems(self,obj,elt):
        self.memoize(obj,elt)
        for akey,avalue in obj.items():
            key = self.asElement(akey,Z+'key',parent=elt)
            vid = id(avalue)
            if vid in self.memo:
                self.asRef(vid,Z+'val',parent=key)
            else:
                self.asElement(avalue,Z+'val',parent=key)
    dispatch['dict'] = getDictItems

    def getReduction(self,obj,elt):
        try:
            reduction = obj.__reduce__()
        except AttributeError:
            reduce_method = dispatch_table.get(type(obj))
            if reduce_method:
                reduction = reduce_method(obj)
            else:
                raise PicklingError('%s item cannot be pickled' % type(obj))
        if isinstance(reduction,basestring):
            reduction = globals().get(reduction)
        outtag = self.asElement(reduction,Z+'_reduction',parent=elt)
        obj = None

    def dumps(self,obj,compress=False,pretty_print=False,xml_declaration=False,
            encoding='ASCII'):
        self.memo.clear()
        elt = self.asElement(obj)
        xml = tostring(elt, pretty_print=pretty_print,
            xml_declaration=xml_declaration,encoding=encoding)
        if compress:
            f = StringIO()
            gzfile = gzip.GzipFile(mode='w',fileobj=f)
            gzfile.write(xml)
            gzfile.close()
            xml = f.getvalue()
            f.close()
        return xml

    def dumpsp(self,obj,xml_declaration=False, encoding='ASCII'):
        self.memo.clear()
        return self.dumps(obj,pretty_print=True,xml_declaration=False,
            encoding='ASCII')

    def dump(self,obj,compress=False,pretty_print=False,xml_declaration=True,
            encoding='ASCII'):
        self.memo.clear()
        elt = self.asElement(obj)
        xml = tostring(elt.getroottree(), pretty_print=pretty_print,
             xml_declaration=xml_declaration,encoding=encoding)
        if compress:
            gzfile = gzip.GzipFile(mode='w',fileobj=self.file)
            gzfile.write(xml)
            gzfile.close()
        else:
            self.file.write(xml)

    def persistent_id(self,obj):
        if IPersistent.providedBy(obj):
            return b64encode(obj._p_oid)

builtins = set(['NoneType','bool', 'complex', 'dict', 'float','frozenset',
 'int', 'list', 'long', 'set', 'tuple', 'decimal', 'str', 'unicode',
 'datetime','date'])

class Unpickler(object):

    def __init__(self, s):
        """init with a string. or file-like
        """
        self.setdata(s)

    def setdata(self,s):
        mode = None
        if hasattr(s,'read'):
            s.seek(0)
            tst = s.read(2)
            s.seek(0)
        else:
            tst = s[:2]
            mode = 'S'
        if tst == '\037\213':
            if mode == 'S':
                s = StringIO(s)
            gzfile = gzip.GzipFile(mode='r',fileobj=s)
            s = gzfile.read()
            mode = 'S'
        if mode == 'S':
            self.data = fromstring(s,parser)
        else:
            self.data = parse(s,parser).getroot()
        self.refs = {}

    def load(self):
        """
        Read a pickled object representation from self.data.

        Return the reconstituted object hierarchy.
        """
        out = self.reconstitute(self.data)
        self.refs.clear()
        return out

    def reconstitute(self,item):
        """
        reconstitute objects recursively descending from this XML tag
        """
        refid = item.get(Z+'refto')
        if refid:
            try:
                return self.refs[refid]
            except KeyError:
                # reconstitute the referenced object
                itm = self.data.xpath("//*[@z:ref='%s']" % refid,
                    namespaces=PMAP)[0]
                self.reconstitute(itm)
                # now, we can return the thing we wanted
                return self.refs[refid]

        # if it's reduced, return that
        reduction = self.getReduction(item)
        if reduction:
            return self.handleReduction(reduction)

        tag = item.tag
        tag = tag[len(Z):]
        klass = item.get(Z+'cls')
        module = item.get(Z+'module')

        if klass is None:
            # class reference
            klassobj = item.get(Z+'kls')

            if klassobj:
                ret = self.find_class(module,klassobj)

            # function reference
            fnobj = item.get(Z+'fn')
            if fnobj:
                __import__(module)
                mod = sys.modules.get(module,None)
                if mod:
                    ret= getattr(mod,fnobj)

            if item.get(Z+'ref'):
                    self.refs[item.get(Z+'ref')] = ret
            return ret

        if klass and module:
            # class instance
            itemclass = self.find_class(module,klass)
            newargs = self.getnewargs(item)
            ret = self._instantiate(itemclass,newargs)
            if item.get(Z+'ref'):
                    self.refs[item.get(Z+'ref')] = ret
            if hasattr(ret,'__setstate__'):
                if hasattr(ret,'__getstate__'):
                    state = self.getstate(item)
                    if state:
                        ret.__setstate__(state)

            # populate attributes
            for attr in item.xpath('o:*',namespaces=OMAP):
                tag = attr.tag
                name = tag[len(OBJ):]
                #if not hasattr(ret,name):
                setattr(ret,name,self.reconstitute(attr))

            if not newargs:
                # install collection data - list-like or dict-like stuff
                if hasattr(ret,'__getitem__'):
                    if hasattr(ret,'keys') and hasattr(ret,'values'):
                        # dict data
                        dictitems = self.getdict(item)
                        ret.update(dictitems)
                    else:
                        # list data
                        if not len(ret):
                            # populate only if not populated already
                            # sometimes list data is actually in an attribute
                            listitems = self.getlist(item)
                            ret[:]= listitems

            return ret

        elif klass in builtins:
            if klass in ('str', 'unicode'):
                ret = item.text
                if item.get(Z+'enc') == 'base64':
                    ret = b64decode(ret)
                if isinstance(ret,str):
                    if klass == 'unicode':
                        ret = unicode(ret)
                else:
                    if klass == 'str':
                        ret = str(ret)
                return ret
            elif klass == 'list':
                ret = []
                if item.get(Z+'ref'):
                    self.refs[item.get(Z+'ref')] = ret
                lst = self.getlist(item)
                ret[:] = lst
                return ret
            elif klass == 'dict':
                ret = {}
                if item.get(Z+'ref'):
                    self.refs[item.get(Z+'ref')] = ret
                dct = self.getdict(item)
                ret.clear()
                ret.update(dct)
                return ret
            elif klass == 'tuple':
                ret = self.getlist(item)
                ret = tuple(ret)
                if item.get(Z+'ref'):
                    self.refs[item.get(Z+'ref')] = ret
                return ret
            elif klass == 'bool':
                if item.text == 'true':
                    return True
                else:
                    return False
            elif klass == 'NoneType':
                return None
            elif klass == 'int':
                return int(item.text)
            elif klass == 'long':
                return long(item.text)
            elif klass == 'float':
                return float(item.text)
            elif klass == 'complex':
                rl = self.reconstitute(item.xpath('z:real',
                    namespaces=PMAP)[0])
                img = self.reconstitute(item.xpath('z:imag',
                    namespaces=PMAP)[0])
                return complex(rl,img)
            elif klass == 'Decimal':
                return Decimal(item.text)
        else:
            raise UnpicklingError('could not unpickle')
    def getnewargs(self,data):
        newargs = data.xpath('z:_newargs', namespaces=PMAP)
        if newargs:
            newargs = newargs[0]
        else:
            return None
        return self.reconstitute(newargs)

    def getReduction(self,data):
        s = data.xpath('z:_reduction', namespaces=PMAP)
        if s:
            s = s[0]
        else:
            return None
        return self.reconstitute(s)

    def getstate(self,data):
        s = data.xpath('z:_state', namespaces=PMAP)
        if s:
            s = s[0]
        else:
            return None
        return self.reconstitute(s)

    def getlist(self,data):
        t = []
        for k in data.xpath('z:item',namespaces=PMAP):
            t.append(self.reconstitute(k))
        return t

    def getdict(self,data):
        d = {}
        for k in data.xpath('z:key',namespaces=PMAP):
            key = self.reconstitute(k)
            value = self.reconstitute(k[0])
            d[key] = value
        return d

    # the following two methods are borrowed and adapted a bit from python's
    # pickle.py

    def _instantiate(self, klass, args=None):
        instantiated = False
        #old-style classes
        if (args is None and
                type(klass) is ClassType and
                not hasattr(klass, "__getinitargs__")):
            try:
                value = _EmptyClass()
                value.__class__ = klass
                instantiated = True
            except RuntimeError:
                # In restricted execution, assignment to inst.__class__ is
                # prohibited
                pass
        # new-style classes or old-style with initargs
        if not instantiated:
            #if args is not None:
            if args:
                value = klass(*args)
            else:
                try:
                    value = klass()
                except TypeError:
                    class NewEmpty(klass):
                        def __init__(self):
                            pass
                    value = NewEmpty()
                    value.__class__ = klass
        return value

    def find_class(self, module, name):
        # Subclasses may override this
        if module:
            __import__(module)
            mod = sys.modules[module]
        else:
            mod = sys.modules['__builtin__']
        klass = getattr(mod, name)
        return klass

    def handleReduction(self,reduction):
        if isinstance(reduction,tuple):
            tlen = len(reduction)
            assert tlen >= 2
            c = reduction[0]
            assert callable(c)
            if tlen > 1:
                params = reduction[1]
                if params is None:
                    params = ()
                #print c, params
                obj = c(*params)
            else:
                obj =  c()
            if tlen > 2:
                state = reduction[2]
                if state:
                    if hasattr(obj,'__setstate__'):
                        obj.__setstate__(state)
                    else:
                        try:
                            obj.__dict__.update(state)
                        except AttributeError:
                            obj.__dict__ = state
            if tlen > 3:
                listitems = reduction[3]
                if listitems is not None:
                    if hasattr(obj,'extend'):
                        obj.extend(list(listitems))
                    else:
                        for itm in listitems:
                            obj.append(itm)
            if tlen > 4:
                dictitems = reduction[4]
                if dictitems is not None:
                    for key,value in dictitems:
                        obj[key] = value
            return obj

        else:
            c = globals().get(reduction)
            assert callable(c)
            return c()

def loads(strx):
    """
    return the unpickled object from the string
    """
    u = Unpickler(strx)

    return u.load()

def dumps(obj,compress=False,pretty_print=False,encoding='ASCII',
        xml_declaration=False):
    """
    return a string with an XML representation of the object

    set pretty_print to True for nicely-indented XML
    """
    p = XMLPickler()
    return p.dumps(obj,compress=compress,pretty_print=pretty_print,
        encoding=encoding,xml_declaration=xml_declaration)

def dump(obj,file,compress=False,pretty_print=False,encoding='ASCII',
        xml_declaration=True):
    """
    put the pickled representation of obj in a file
    """
    p = XMLPickler(file)
    p.dump(obj,compress=compress,pretty_print=pretty_print,encoding=encoding,
        xml_declaration=xml_declaration)

def load(file):
    """
    return the object whose XML representation is in file
    """
    u = Unpickler(file)
    return u.load()

def dumpsp(obj):
    """return pretty-printed xml from an object"""
    p = XMLPickler()
    return p.dumpsp(obj)
# Helper class for load_inst/load_obj

class _EmptyClass:
    pass
