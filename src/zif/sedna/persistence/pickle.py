import sys

from decimal import Decimal
from copy_reg import _slotnames, dispatch_table
from types import ClassType, FunctionType, BuiltinFunctionType, TypeType, \
    NoneType, InstanceType
from base64 import b64encode, b64decode
from copy import copy, deepcopy

import gzip
from lxml.etree import Element, SubElement, tostring, fromstring, parse, \
    XMLSyntaxError, XMLParser, XPath
from cStringIO import StringIO
from persistent.interfaces import IPersistent
from zope.interface import implements, providedBy
from random import choice

try:
    from uuid import uuid4
except ImportError:
    try:
        from py_uuid import uuid4
    except ImportError:
        uuid4 = None

import cPickle
import zlib

from zope.interface import Interface

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
PKL_PREFIX = '{%s}' % PKL_NAMESPACE
OBJ_PREFIX= '{%s}' % OBJ_NAMESPACE
PMAP = {'p':PKL_NAMESPACE}
NAMESPACES = {None:PKL_NAMESPACE}
OMAP={'o':OBJ_NAMESPACE}

pprefixlen = len(PKL_PREFIX)

base_element_name = 'Pickle'

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
    # if lxml balks at setting s as text of an element, convert to base64.
    t = Element('x')
    try:
        t.text = s
    except AssertionError:
        s = b64encode(s)
        b64=True
    return s, b64

handled_modules = ('__builtin__', )
base_classes = set(['dict', 'list', 'str', 'unicode', 'tuple',
    'int', 'long', 'float', 'Decimal', 'complex', 'NoneType', 'bool'])
handled_types=(basestring, list, dict, tuple, float, complex, int)

def refgen(uuid=False):
    if uuid and uuid4:
        while 1:
            yield str(uuid4())
    elif uuid:
        #we'll make a random-enough uuid that looks kinda like a uuid
        lst = 'abcdef0123456789'
        lst += lst.upper()
        while 1:
            z = ['X']
            for m in (8,4,4,4,12):
                z.append('-')
                z.extend([choice(lst) for x in xrange(m)])
            yield ''.join(z)
    else:
        lst = 'abcdef0123456789'
        lst += lst.upper()
        for a in lst:
            for b in lst:
                for c in lst:
                    for d in lst:
                        for e in lst:
                            yield u'%s%s%s%s%s' % (a, b, c, d, e)

class XMLPickler(object):
    """
    Pickle python objects as a particular XML format.

    This format tries to serialize python objects into an xml-friendly,
    searchable representation.

    We use two namespaces for the object representation.

     the pickle namespace is http://namespaces.zope.org/pickle
       used for pickle internals
     the (default) object namespace is http://namespaces.zope.org/pyobj
       used for names in the client object

    As picklers go, it is pretty generic, and round-trips objects faithfully
    with Unpickler.  It is important to note that tuples are maintained
    immutably.  Tuples that "look the same" will be duplicated, not
    handled as references to the first one.
    """
    dispatch = {}
    def __init__(self, f=None, omit_attrs=None, want_uuid=False):
        """omit_attrs is a list of beginnings of attribute names that should
        not be included.  For example if omit_attrs is ['_v_'], any object
        attribute starting with _v_ will be omitted for serialization."""
        if f:
            self.file = f
        self.memo = {}
        self.want_uuid = want_uuid
        self.refs = refgen(want_uuid)
        self.omit_attrs = omit_attrs

    def persistent_id(self, obj):
        # for subclasses to use
        return None

    def memoize(self, obj, tag):
        pid = self.persistent_id(obj)
        if pid:
            ref = pid
        else:
            try:
                ref = self.refs.next()
            except StopIteration:
                self.refs = refgen(self.want_uuid)
                ref = refs.next()
        objid = id(obj)
        self.memo[objid] = (ref, tag)

    def asRef(self, obj_id, name=None, parent=None, element_name=None):
        if element_name is None:
            element_name = PKL_PREFIX + base_element_name
        ref_id, source = self.memo[obj_id]
        source.set('id', str(ref_id))
        attrs = {'idref':str(ref_id)}
        if name:
            attrs['name'] = name
        if parent is not None:
            elt = SubElement(parent, element_name, attrs,
                nsmap=NAMESPACES)
        else:
            elt = Element(element_name, attrs, nsmap=NAMESPACES)
        return elt

    def as_class_element(self, obj, name=None, parent=None, element_name=None):
        if element_name is None:
            element_name = PKL_PREFIX + base_element_name
        module = obj.__module__
        classname = obj.__name__
        attrs = {'cls':classname}
        if name is not None:
            attrs['name'] = name
        if parent is not None:
            elt = SubElement(parent, element_name, attrs, nsmap=NAMESPACES)
        else:
            elt = Element(element_name, attrs, nsmap=NAMESPACES)
        if not module in handled_modules:
            elt.set('module', module)
        return elt

    def as_function_element(self, obj, name=None, parent=None, element_name=None):
        if element_name is None:
            element_name = PKL_PREFIX + base_element_name
        fname = obj.__name__
        # borrowed from gnosis utils
        if not name == '__main__':
            for module_name, module in sys.modules.items():
                if  hasattr(module, fname) and \
                    getattr(module, fname) is obj:
                    break
        else:
            module_name = '__main__'
        module = module_name
        attrs = {'fn':fname}
        if name:
            attrs['name'] = name
        if parent is not None:
            elt = SubElement(parent, element_name, attrs, nsmap=NAMESPACES)
        else:
            elt = Element(element_name, attrs, nsmap=NAMESPACES)
        if not module in handled_modules:
            elt.set('module', module)
        return elt

    def as_element(self, obj, name=None, parent=None, element_name=None):
        """Return an element representing obj.

        If name is provided, use name for the tag, else 'pickle' is used.

        Parent is used internally as a pointer to the parent XML tag.

        """

        if not isinstance(obj, InstanceType):
            if isinstance(obj, (ClassType)):
                return self.as_class_element(obj, name, parent=parent, element_name=element_name)
            elif isinstance(obj, (FunctionType, BuiltinFunctionType)):
                return self.as_function_element(obj, name, parent=parent, element_name=element_name)
            elif isinstance(obj, (TypeType))and \
                    not obj.__name__ in base_classes:
                return self.as_class_element(obj, name, parent=parent, element_name=element_name)
        if element_name is None:
            element_name = PKL_PREFIX + base_element_name
        nmap = NAMESPACES
        needReduce = False
        try:
            class_ = obj.__class__
        except AttributeError:
            # no class for this item.  use type and get reduction from
            # copy_reg
            class_ = type(obj)
            needReduce = True

        class_name = class_.__name__
        module = class_.__module__

        # make a dict for XML attributes with class (and module)
        attrs = {}
        if not module in handled_modules:
            attrs['module']=module
        attrs['class']=class_name
        if name is not None:
            attrs['name'] = name

        #print parent,name,attrs,nmap
        # create the element
        if parent is not None:
            elt = SubElement(parent, element_name, attrs, nsmap=nmap)
        else:
            elt = Element(element_name, attrs, nsmap=nmap)

        # return element for basic python objects
        if class_name in base_classes and module in handled_modules:
            self.dispatch[class_name](self, obj, elt)
            return elt

        # persistent ID
        p_id = self.persistent_id(obj)
        if p_id:
            elt.set('id', p_id)

        # return element for extension class objects that use __reduce__
        if needReduce:
            self.get_reduction(obj, elt)
            return elt

        # Handle instances.  Set-up dict and state.

        # d is what we will use for obj.__dict__
        d={}
        # state is whatever we get from __getstate__
        state=None
        # at this point obj is an always an instance, so memoize
        self.memoize(obj, elt)

        # we're pickling to XML to be searchable, so we want __dict__ to go in,
        # even if __getstate___ wants something different
        if hasattr(obj, '__dict__'):
            objdict = obj.__dict__
            d.update(objdict)

        if hasattr(obj, '__getstate__'):
            state = obj.__getstate__()
            if hasattr(obj, '__setstate__'):
                # object has a __setstate__ method, which means __getstate__
                # probably provides a dense representation that will be useless
                # in XML.  let's just pickle that.  It will show up in the XML
                # as a base64 string.
                pstate = cPickle.dumps(state, -1)
                #pstate = state
                outtag = self.as_element(pstate, parent=elt,
                    element_name=PKL_PREFIX+'State')
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
        if hasattr(obj, '__getnewargs__'):
            new_args = obj.__getnewargs__()
        if not new_args and hasattr(obj, '__getinitargs__'):
            new_args = obj.__getinitargs__()
        if new_args:
            outtag = self.as_element(new_args,
                parent=elt, element_name=PKL_PREFIX+'NewArgs')

        # set the contents of slots into dict, though
        # classes with __slots__ should be use __getstate__ and __setstate__
        object_slots=_slotnames(class_)
        if object_slots:
            slot_dict = {}
            for key in object_slots:
                try:
                    value = getattr(obj, key)
                    slot_dict[key] = value
                except AttributeError:
                    pass
            if slot_dict:
                d.update(slot_dict)

        # not sure if this will handle every possible __reduce__ output
        # reduce is mostly for extension classes
        # prefer object_slots or __dict__  or state instead of reduce
        # if available
        if not (d or state) and hasattr(obj, '__reduce__'):
            if not isinstance(obj, handled_types):
                self.get_reduction(obj, elt)
            # expose some useful text for date/datetime objects
            if class_name in ('date', 'datetime'):
                if module == 'datetime':
                    t = obj.isoformat()
                    t = str(obj)
                    repres = SubElement(elt,PKL_PREFIX+"Repr")
                    repres.text = t

        # now, write the __dict__ information.
        attributes = Element(PKL_PREFIX+'Attributes')
        for key, value in d.items():
            # persist is true if the object attribute should not be omitted
            persist = True
            if self.omit_attrs:
                for start in self.omit_attrs:
                    if key.startswith(start):
                        persist=False
                        break
            if persist:
                value_id = id(value)
                if value_id in self.memo:
                    self.asRef(value_id, name=key, parent=attributes, element_name=PKL_PREFIX+'Attribute')
                else:
                    outputtag = self.as_element(value, key,
                        parent=attributes, element_name=PKL_PREFIX+'Attribute')
        if len(attributes):
            elt.append(attributes)
        # do no more unless we have an obj subclassing base python objects
        if not issubclass(class_, handled_types):
            return elt

        # these are for the case where obj subclasses base python objects
        if isinstance(obj, basestring):
            self.handle_basestring(obj, elt)

        elif hasattr(obj, '__getitem__'):
            self.handle_sequence(obj, elt)

        elif isinstance(obj, bool):
            self.handle_bool(obj, elt)

        elif isinstance(obj, NoneType):
            self.handle_none(obj, elt)

        elif isinstance(obj, (int, long, float, Decimal)):
            self.handle_number(obj, elt)

        elif isinstance(obj, complex):
            self.handle_complex(obj, elt)

        obj = None
        return elt

    def handle_basestring(self, obj, elt):
        txt, b64 = strConvert(obj)
        if b64:
            elt.set('enc', 'base64')
        elt.text = txt
    dispatch['str'] = handle_basestring
    dispatch['unicode'] = handle_basestring

    def handle_sequence(self, obj, elt):
        if hasattr(obj, 'keys') and hasattr(obj, 'values'):
            self.get_dict_items(obj, elt)
        else:
            self.get_sequence_items(obj, elt)

    def handle_bool(self, obj, elt):
        elt.text = str(obj).lower()
    dispatch['bool'] = handle_bool

    def handle_none(self, obj, elt):
        # XXX do we want text here?
        elt.text = 'None'
    dispatch['NoneType'] = handle_none

    def handle_number(self, obj, elt):
        elt.text = str(obj)
    dispatch['int'] = handle_number
    dispatch['long'] = handle_number
    dispatch['Decimal'] = handle_number
    dispatch['float'] = handle_number

    def handle_complex(self, obj, elt):
        self.as_element(obj.real,parent=elt, element_name=PKL_PREFIX  + 'Real')
        self.as_element(obj.imag, parent=elt,element_name=PKL_PREFIX + 'Imag')
    dispatch['complex'] = handle_complex

    def get_sequence_items(self, obj, elt):
        if isinstance(obj, list):
            # only memoize lists, not tuples
            self.memoize(obj, elt)
        collection = SubElement(elt,PKL_PREFIX+'Collection')
        collection.set('type','sequence')
        for listitem in obj:
            value_id = id(listitem)
            if value_id in self.memo:
                self.asRef(value_id, parent=collection, element_name=PKL_PREFIX + 'Item')
            else:
                self.as_element(listitem, parent=collection,element_name=PKL_PREFIX + 'Item')
    dispatch['list'] = get_sequence_items
    dispatch['tuple'] = get_sequence_items

    def get_dict_items(self, obj, elt):
        self.memoize(obj, elt)
        collection = SubElement(elt,PKL_PREFIX+'Collection')
        collection.set('type','mapping')
        for akey, avalue in obj.items():
            item = SubElement(collection,PKL_PREFIX+'Item')
            self.as_element(akey, parent=item, element_name=PKL_PREFIX + 'Key')
            value_id = id(avalue)
            if value_id in self.memo:
                self.asRef(value_id, parent=item, element_name=PKL_PREFIX + 'Value')
            else:
                self.as_element(avalue, parent=item, element_name=PKL_PREFIX + 'Value')
    dispatch['dict'] = get_dict_items

    def get_reduction(self, obj, elt):
        try:
            reduction = obj.__reduce__()
        except AttributeError:
            reduce_method = dispatch_table.get(type(obj))
            if reduce_method:
                reduction = reduce_method(obj)
            else:
                raise PicklingError('%s item cannot be pickled' % type(obj))
        if isinstance(reduction, basestring):
            reduction = globals().get(reduction)
        outtag = self.as_element(reduction, element_name=PKL_PREFIX + 'Reduction',
            parent=elt)
        obj = None

    def dumps(self, obj, compress=False, pretty_print=False,
            xml_declaration=False, encoding='ASCII'):
        self.memo.clear()
        elt = self.as_element(obj)
        xml = tostring(elt, pretty_print=pretty_print,
            xml_declaration=xml_declaration, encoding=encoding)
        if compress:
            f = StringIO()
            gzfile = gzip.GzipFile(mode='w', fileobj=f)
            gzfile.write(xml)
            gzfile.close()
            xml = f.getvalue()
            f.close()
        return xml

    def dumpsp(self, obj, xml_declaration=False, encoding='ASCII'):
        self.memo.clear()
        return self.dumps(obj, pretty_print=True, xml_declaration=False,
            encoding='ASCII')

    def dump(self, obj, compress=False, pretty_print=False,
            xml_declaration=True, encoding='ASCII'):
        self.memo.clear()
        elt = self.as_element(obj)
        xml = tostring(elt.getroottree(), pretty_print=pretty_print,
             xml_declaration=xml_declaration, encoding=encoding)
        if compress:
            gzfile = gzip.GzipFile(mode='w', fileobj=self.file)
            gzfile.write(xml)
            gzfile.close()
        else:
            self.file.write(xml)

builtins = set(['NoneType', 'bool', 'complex', 'dict', 'float', 'frozenset',
 'int', 'list', 'long', 'set', 'tuple', 'decimal', 'str', 'unicode',
 'datetime', 'date'])

class Unpickler(object):
    dispatch = {}
    def __init__(self, s):
        """init with a string. or file-like
        """
        self.setdata(s)
        self.refs = {}

    def setdata(self, s):
        mode = None
        if hasattr(s, 'read'):
            s.seek(0)
            tst = s.read(2)
            s.seek(0)
        else:
            tst = s[:2]
            mode = 'S'
        if tst == '\037\213':
            if mode == 'S':
                s = StringIO(s)
            gzfile = gzip.GzipFile(mode='r', fileobj=s)
            s = gzfile.read()
            mode = 'S'
        if mode == 'S':
            self.data = fromstring(s, parser)
        else:
            self.data = parse(s, parser).getroot()

    def load(self):
        """
        Read a pickled object representation from self.data.

        Return the reconstituted object hierarchy.
        """

        out = self.reconstitute(self.data)
        return out

    def reconstitute(self, item):
        """
        reconstitute objects recursively descending from this XML element
        """
        idref = item.get('idref', None)
        if idref:
            try:
                return self.refs[idref]
            except KeyError:
                # reconstitute the referenced object
                itm = self.idref_xpath(self.data, idref=idref)[0]
                self.reconstitute(itm)
                # now, we can return the thing we wanted
                return self.refs[idref]

        # if it's reduced, return that
        reduction = self.get_reduction(item)
        if reduction:
            return self.handleReduction(reduction)

        tag = item.tag
        tag = tag[pprefixlen:]
        class_ = item.get('class')
        module = item.get('module')
        #print class_,module
        if class_ is None:
            # class reference
            class_obj = item.get('cls')

            if class_obj:
                ret = self.find_class(module, class_obj)

            # function reference
            fnobj = item.get('fn')
            if fnobj:
                __import__(module)
                mod = sys.modules.get(module, None)
                if mod:
                    ret= getattr(mod, fnobj)

            ref = item.get('id')
            if ref:
                self.refs[ref] = ret
            return ret

        if class_ and module:
            # we have a class instance
            itemclass = self.find_class(module, class_)
            newargs = self.getnewargs(item)
            ret = self._instantiate(itemclass, newargs)

            ref = item.get('id')
            if ref:
                self.refs[ref] = ret

            # do getstate if desired
            if hasattr(ret, '__setstate__') and hasattr(ret, '__getstate__'):
                state = self.getstate(item)
                if state:
                    ret.__setstate__(state)

            # populate attributes
            for attr in attr_xpath(item):
                tag = attr.tag
                #name = tag[len(OBJ_PREFIX):]
                name = attr.get('name')
                #print name
                #if not hasattr(ret, name):
                setattr(ret, name, self.reconstitute(attr))

            if not newargs:
                # populate collection data - list-like or dict-like stuff
                if hasattr(ret, '__getitem__'):
                    if hasattr(ret, 'keys') and hasattr(ret, 'values'):
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

        elif class_ in builtins:
            return self.dispatch[class_](self, item)
        else:
            raise UnpicklingError('could not unpickle %s' % item)

    def get_str(self, item):
        ret = item.text
        if item.get('enc') == 'base64':
            ret = b64decode(ret)
        ret = str(ret)
        return ret
    dispatch['str'] = get_str

    def get_unicode(self, item):
        ret = item.text
        if item.get('enc') == 'base64':
            ret = b64decode(ret)
        ret = unicode(ret)
        return ret
    dispatch['unicode'] = get_unicode

    def get_list(self, item):
        ret = []
        refid = item.get('id')
        if refid:
            self.refs[refid] = ret
        ret[:] = self.getlist(item)
        return ret
    dispatch['list'] = get_list

    def get_dict(self, item):
        ret = {}
        refid = item.get('id')
        if refid:
            self.refs[refid] = ret
        ret.update(self.getdict(item))
        return ret
    dispatch['dict'] = get_dict

    def get_tuple(self, item):
        ret = tuple(self.getlist(item))
#        if item.get('id'):
#            self.refs[item.get('id')] = ret
        return ret
    dispatch['tuple'] = get_tuple

    def get_bool(self, item):
        t = item.text
        return 'r' in t or (t.isdigit() and int(t) == 1)
    dispatch['bool'] = get_bool

    def get_none(self, item):
        return None
    dispatch['NoneType'] = get_none

    def get_int(self, item):
        return int(item.text)
    dispatch['int'] = get_int

    def get_long(self, item):
        return long(item.text)
    dispatch['long'] = get_long

    def get_float(self, item):
        return float(item.text)
    dispatch['float'] = get_float

    def get_decimal(self, item):
        return Decimal(item.text)
    dispatch['Decimal'] = get_decimal

    def get_complex(self, item):
        real = self.reconstitute(complex_real_xpath(item)[0])
        imag = self.reconstitute(complex_imag_xpath(item)[0])
        return complex(real, imag)
    dispatch['complex'] = get_complex

    def getnewargs(self, data):
        newargs = newargs_xpath(data)
        if not newargs:
            return None
        return self.reconstitute(newargs[0])

    def get_reduction(self, data):
        s = reduction_xpath(data)
        if not s:
            return None
        return self.reconstitute(s[0])

    def getstate(self, data):
        s = state_xpath(data)
        if not s:
            return None
        return cPickle.loads(self.reconstitute(s[0]))

    def getlist(self, data):
        reconst = self.reconstitute
        return (reconst(k) for k in item_xpath(data))

    def getdict(self, data):
        d = {}
        reconst = self.reconstitute
        for item in item_xpath(data):
            key = reconst(key_xpath(item)[0])
            val = reconst(value_xpath(item)[0])
            d[key] = val
        #for key, val in ((reconst(k), reconst(k[0])) for k in key_xpath(data)):
            #d[key] = val
        return d

    # the following two methods are borrowed and adapted a bit from python's
    # pickle.py

    def _instantiate(self, class_, args=None):
        instantiated = False
        #old-style classes
        if (args is None and
                type(class_) is ClassType and
                not hasattr(class_, "__getinitargs__")):
            try:
                value = _EmptyClass()
                value.__class__ = class_
                instantiated = True
            except RuntimeError:
                # In restricted execution, assignment to inst.__class__ is
                # prohibited
                pass
        # new-style classes or old-style with initargs
        if not instantiated:
            if args:
                value = class_(*args)
            else:
                try:
                    value = class_()
                except TypeError:
                    class NewEmpty(class_):
                        def __init__(self):
                            pass
                    value = NewEmpty()
                    value.__class__ = class_
        return value

    def find_class(self, module, name):
        # Subclasses may override this
        if module:
            __import__(module)
            mod = sys.modules[module]
        else:
            mod = sys.modules['__builtin__']
        class_ = getattr(mod, name)
        return class_

    def handleReduction(self, reduction):
        if isinstance(reduction, tuple):
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
                    if hasattr(obj, '__setstate__'):
                        obj.__setstate__(state)
                    else:
                        try:
                            obj.__dict__.update(state)
                        except AttributeError:
                            obj.__dict__ = state
            if tlen > 3:
                listitems = reduction[3]
                if listitems is not None:
                    if hasattr(obj, 'extend'):
                        obj.extend(list(listitems))
                    else:
                        for itm in listitems:
                            obj.append(itm)
            if tlen > 4:
                dictitems = reduction[4]
                if dictitems is not None:
                    for key, value in dictitems:
                        obj[key] = value
            return obj

        else:
            c = globals().get(reduction)
            assert callable(c)
            return c()

item_xpath = XPath('p:Collection/p:Item', namespaces=PMAP)
key_xpath = XPath('p:Key', namespaces=PMAP)
value_xpath = XPath('p:Value', namespaces=PMAP)
reduction_xpath = XPath('p:Reduction', namespaces=PMAP)
attr_xpath = XPath('p:Attributes/p:Attribute', namespaces=PMAP)
newargs_xpath = XPath('p:NewArgs', namespaces=PMAP)
state_xpath = XPath('p:State', namespaces=PMAP)
complex_real_xpath = XPath('p:Real', namespaces=PMAP)
complex_imag_xpath = XPath('p:Imag', namespaces=PMAP)
idref_xpath = XPath("//*[@idref=$idref]")

def loads(s):
    """
    return the unpickled object from the string
    """
    u = Unpickler(s)

    return u.load()

def dumps(obj, compress=False, pretty_print=False, encoding='ASCII',
        xml_declaration=False):
    """
    return a string with an XML representation of the object

    set pretty_print to True for nicely-indented XML
    """
    p = XMLPickler()
    return p.dumps(obj, compress=compress, pretty_print=pretty_print,
        encoding=encoding, xml_declaration=xml_declaration)

def dump(obj, file, compress=False, pretty_print=False, encoding='ASCII',
        xml_declaration=True):
    """
    put the pickled representation of obj in a file
    """
    p = XMLPickler(file)
    p.dump(obj, compress=compress, pretty_print=pretty_print, encoding=encoding,
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
