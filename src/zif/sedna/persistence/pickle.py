import sys

from decimal import Decimal
from copy_reg import _slotnames, dispatch_table
from types import ClassType, FunctionType, BuiltinFunctionType, TypeType, \
    NoneType, InstanceType
from base64 import b64encode, b64decode
from copy import copy,deepcopy

import gzip
from lxml.etree import Element, SubElement, tostring, fromstring, parse, \
    XMLSyntaxError, XMLParser, XPath
from cStringIO import StringIO
from persistent.interfaces import IPersistent
from zope.interface import implements, providedBy

import cPickle
import zlib


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
P_PREFIX = '{%s}' % PKL_NAMESPACE
OBJ_PREFIX= '{%s}' % OBJ_NAMESPACE
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
    """
    Pickle python objects in a particular XML format.
    
    This format tries to 
    """
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

    def asRef(self,obj_id, name=None,parent=None):
        if not name:
            name = P_PREFIX+base_element_name
        ref_id,source = self.memo[obj_id]
        source.set(P_PREFIX+'ref',str(ref_id))
        if parent is not None:
            elt = SubElement(parent,name,{P_PREFIX+'refto':str(ref_id)},
                nsmap=PMAP)
        else:
            elt = Element(name,{P_PREFIX+'refto':str(ref_id)},nsmap=PMAP)
        return elt

    def asClassElement(self,obj,name=None,parent=None):
        if not name:
            name = P_PREFIX+base_element_name
        module = obj.__module__
        classname = obj.__name__
        if parent is not None:
            elt = SubElement(parent,name,{P_PREFIX+'kls':classname},nsmap=PMAP)
        else:
            elt = Element(name,{P_PREFIX+'kls':classname},nsmap=PMAP)
        if not module in handled_modules:
            elt.set(P_PREFIX+'module',module)
        return elt

    def asFunctionElement(self,obj,name=None,parent=None):
        if not name:
            name = P_PREFIX+base_element_name
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
            elt = SubElement(parent,name,{P_PREFIX+'fn':fname},nsmap=PMAP)
        else:
            elt = Element(name,{P_PREFIX+'fn':fname},nsmap=PMAP)
        if not module in handled_modules:
            elt.set(P_PREFIX+'module',module)
        return elt

    def asElement(self,obj,name=None,parent=None):
        """return an element representing obj.

        if name is provided, use name for the tag, else 'pickle' is used.
        """
        
        if not isinstance(obj,InstanceType):
            if isinstance(obj,(ClassType)):
                return self.asClassElement(obj,name,parent=parent)
            elif isinstance(obj,(FunctionType, BuiltinFunctionType)):
                return self.asFunctionElement(obj,name,parent=parent)
            elif isinstance(obj,(TypeType))and not obj.__name__ in base_classes:
                return self.asClassElement(obj,name,parent=parent)
        if name is None:
            name = P_PREFIX + base_element_name
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

        # make an XML attributes dict with class (and module)
        attrs = {}
        if not module in handled_modules:
            attrs[P_PREFIX+'module']=module
        attrs[P_PREFIX+'cls']=klass_name

        # create the element
        if parent is not None:
            elt = SubElement(parent,name,attrs,nsmap=nmap)
        else:
            elt = Element(name,attrs,nsmap=nmap)

        # handle and return element for basic python objects
        if klass_name in base_classes and module in handled_modules:
            self.dispatch[klass_name](self,obj,elt)
            return elt

        # handle and return element for extension objects that use __reduce__
        if needReduce:
            self.getReduction(obj,elt)
            return elt

        # Handle instances.  Set-up dict and state.

        # d is what we will use for obj.__dict__
        d={}
        # state is whatever we get from __getstate__
        state=None
        # at this point obj is an always an instance, so memoize
        self.memoize(obj,elt)
        
        # we're pickling to XML for visiblility, so __dict__ needs to go in,
        # even if __getstate___ wants something different
        if hasattr(obj,'__dict__'):
            objdict = obj.__dict__
            d.update(objdict)

        if hasattr(obj,'__getstate__'):
            state = obj.__getstate__()
            if hasattr(obj,'__setstate__'):
                # object has a __setstate__ method, which probably provides
                # a dense representation that will be useless in XML.  let's 
                # just pickle that.  It will show up in the XML as a base64
                # string.
                pstate = cPickle.dumps(state,-1)
                outtag = self.asElement(pstate,P_PREFIX+'_state',parent=elt)
            else:
                # there is no __setstate__, so put state in __dict__
                d.update(state)
                # in case state wants something weird, we need to make sure
                # the dict does not have anything not in state
                for key in d.keys():
                    if not key in state.keys():
                        del d[key]

        # __getnewargs__ and __getinitargs__
        new_args = None
        if hasattr(obj,'__getnewargs__'):
            new_args = obj.__getnewargs__()
        if not new_args and hasattr(obj,'__getinitargs__'):
            new_args = obj.__getinitargs__()
        if new_args:
            outtag = self.asElement(new_args,P_PREFIX+'_newargs', parent=elt)

        # set the contents of slots into
        # dict just to expose them for search.
        # usually, anything with __slots__ should be use
        # __getstate__ and __setstate__
        object_slots=_slotnames(klass)
        if object_slots:
            slotd = {}
            for key in object_slots:
                try:
                    value = getattr(obj,key)
                    slotd[key] = value
                except AttributeError:
                    pass
            if slotd:
                d.update(slotd)

        # not sure if this will handle every possible __reduce__ output
        # reduce is mostly for extension classes
        # prefer object_slots or state to reduce if available
        if hasattr(obj,'__reduce__') and not (object_slots or state):
            if not isinstance(obj,handled_types):
                self.getReduction(obj,elt)
            # expose some useful text for date/datetime objects
            if klass_name in ('date','datetime'):
                if module == 'datetime':
                    t = obj.isoformat()
                    elt.text = t
            
        # apply the __dict__ information.
        for key, value in d.items():
            vid = id(value)
            if vid in self.memo:
                self.asRef(vid,name=key,parent=elt)
            else:
                outputtag = self.asElement(value,key,parent=elt)

        # do no more unless we have an obj subclassing base python objects         
        if not issubclass(klass,handled_types):
                return elt
        
        # these are for the case where obj subclasses base python objects
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
            elt.set(P_PREFIX+'enc','base64')
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
        self.asElement(obj.real,P_PREFIX+'real',parent=elt)
        self.asElement(obj.imag,P_PREFIX+'imag',parent=elt)
    dispatch['complex'] = handle_complex

    def getSequenceItems(self,obj,elt):
        if isinstance(obj,list):
            # only memoize lists, not tuples
            self.memoize(obj,elt)
        for listitem in obj:
            vid = id(listitem)
            if vid in self.memo:
                self.asRef(vid,P_PREFIX+'item',parent=elt)
            else:
                self.asElement(listitem,P_PREFIX+'item',parent=elt)
    dispatch['list'] = getSequenceItems
    dispatch['tuple'] = getSequenceItems

    def getDictItems(self,obj,elt):
        self.memoize(obj,elt)
        for akey,avalue in obj.items():
            key = self.asElement(akey,P_PREFIX+'key',parent=elt)
            vid = id(avalue)
            if vid in self.memo:
                self.asRef(vid,P_PREFIX+'val',parent=key)
            else:
                self.asElement(avalue,P_PREFIX+'val',parent=key)
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
        outtag = self.asElement(reduction,P_PREFIX+'_reduction',parent=elt)
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


    dispatch = {}
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

        Return the reconstituted object hi_erarchy.
        """
        out = self.reconstitute(self.data)
        self.refs.clear()
        return out

    def reconstitute(self,item):
        """
        reconstitute objects recursively descending from this XML tag
        """
        refid = item.get(P_PREFIX+'refto',None)
        if refid is not None:
            try:
                return self.refs[refid]
            except KeyError:
                # reconstitute the referenced object
                itm = refid_xpath(self.data,refid=refid)[0]
                self.reconstitute(itm)
                # now, we can return the thing we wanted
                return self.refs[refid]

        # if it's reduced, return that
        reduction = self.getReduction(item)
        if reduction:
            return self.handleReduction(reduction)

        tag = item.tag
        tag = tag[len(P_PREFIX):]
        klass = item.get(P_PREFIX+'cls')
        module = item.get(P_PREFIX+'module')

        if klass is None:
            # class reference
            klassobj = item.get(P_PREFIX+'kls')

            if klassobj:
                ret = self.find_class(module,klassobj)

            # function reference
            fnobj = item.get(P_PREFIX+'fn')
            if fnobj:
                __import__(module)
                mod = sys.modules.get(module,None)
                if mod:
                    ret= getattr(mod,fnobj)

            ref = item.get(P_PREFIX+'ref')
            if ref:
                self.refs[ref] = ret
            return ret

        if klass and module:
            # we have a class instance
            itemclass = self.find_class(module,klass)
            newargs = self.getnewargs(item)
            ret = self._instantiate(itemclass,newargs)
            ref = item.get(P_PREFIX+'ref',None)

            if ref is not None:
                self.refs[ref] = ret

            # do getstate if desired
            if hasattr(ret,'__setstate__') and hasattr(ret,'__getstate__'):
                state = self.getstate(item)
                if state:
                    ret.__setstate__(state)

            # populate attributes
            for attr in attr_xpath(item):
                tag = attr.tag
                name = tag[len(OBJ_PREFIX):]
                #if not hasattr(ret,name):
                setattr(ret,name,self.reconstitute(attr))

            
            if not newargs:
            #if 1:
                # represent collection data - list-like or dict-like stuff
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
            return self.dispatch[klass](self,item)
        else:
            raise UnpicklingError('could not unpickle %s' % item)
            
    def get_str(self,item):
        ret = item.text
        if item.get(P_PREFIX+'enc') == 'base64':
            ret = b64decode(ret)
        ret = str(ret)
        return ret
    dispatch['str'] = get_str

    def get_unicode(self,item):
        ret = item.text
        if item.get(P_PREFIX+'enc') == 'base64':
            ret = b64decode(ret)
        ret = unicode(ret)
        return ret
    dispatch['unicode'] = get_unicode

    def get_list(self,item):
        ret = []
        if item.get(P_PREFIX+'ref'):
            self.refs[item.get(P_PREFIX+'ref')] = ret
        ret[:] = self.getlist(item)
        return ret
    dispatch['list'] = get_list
    
    def get_dict(self,item):
        ret = {}
        if item.get(P_PREFIX+'ref'):
            self.refs[item.get(P_PREFIX+'ref')] = ret
        ret.update(self.getdict(item))
        return ret
    dispatch['dict'] = get_dict

    def get_tuple(self,item):  
        ret = tuple(self.getlist(item))
#        if item.get(Z+'ref'):
#            self.refs[item.get(Z+'ref')] = ret
        return ret
    dispatch['tuple'] = get_tuple
    
    def get_bool(self,item):
        if item.text.lower() == 'true':
            return True
        return False
    dispatch['bool'] = get_bool
    
    def get_none(self,item):
        return None
    dispatch['NoneType'] = get_none
    
    def get_int(self,item):
        return int(item.text)
    dispatch['int'] = get_int
    
    def get_long(self,item):
        return long(item.text)
    dispatch['long'] = get_long
    
    def get_float(self,item):
        return float(item.text)
    dispatch['float'] = get_float
    
    def get_decimal(self,item):
        return Decimal(item.text)
    dispatch['Decimal'] = get_decimal
    
    def get_complex(self,item):
        rl = self.reconstitute(complex_real_xpath(item)[0])
        imag = self.reconstitute(complex_imag_xpath(item)[0])
        return complex(rl,imag)
    dispatch['complex'] = get_complex
  
    def getnewargs(self,data):
        newargs = newargs_xpath(data)
        if not newargs:
            return None
        return self.reconstitute(newargs[0])

    def getReduction(self,data):
        s = reduction_xpath(data)
        if not s:
            return None
        return self.reconstitute(s[0])

    def getstate(self,data):
        s = state_xpath(data)
        if not s:
            return None
        return cPickle.loads(self.reconstitute(s[0]))

    def getlist(self,data):
        reconst = self.reconstitute
        return [reconst(k) for k in item_xpath(data)]
   
    def getdict(self,data):
        d = {}
        reconst = self.reconstitute
        for key,val in ((reconst(k),reconst(k[0])) for k in key_xpath(data)):
            d[key] = val
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

item_xpath = XPath('z:item',namespaces=PMAP)
key_xpath = XPath('z:key',namespaces=PMAP)
reduction_xpath = XPath('z:_reduction',namespaces=PMAP)
attr_xpath = XPath('o:*',namespaces=OMAP)
newargs_xpath = XPath('z:_newargs',namespaces=PMAP)
refid_xpath = XPath("//*[@z:ref=$refid]",namespaces=PMAP)
state_xpath = XPath('z:_state',namespaces=PMAP)
complex_real_xpath = XPath('z:real',namespaces=PMAP)
complex_imag_xpath = XPath('z:imag',namespaces=PMAP)

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
