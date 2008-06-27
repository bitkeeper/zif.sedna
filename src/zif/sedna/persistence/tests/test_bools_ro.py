
# read-only version of bool test.
# show that bools are converted to the "best" value, depending
# on Python version being used. --fpm

import zif.sedna.persistence.pickle as xmp
#from gnosis.xml.pickle.util import setVerbose, setParser, setParanoia
from funcs import unlink
#import gnosis.pyconfig as pyconfig

from types import *

# standard test harness setup
#set_parser()

# allow unpickler to load my classes
#setParanoia(0)

# NOTE: The XML below was created by running test_bools.py with
# Python >= 2.3 and grabbing the XML output.

x1 = """
<Pickle xmlns="http://namespaces.zope.org/pickle" class="foo" module="__main__">
  <Attributes>
    <Attribute class="bool" name="a">false</Attribute>
    <Attribute class="NoneType" name="c">None</Attribute>
    <Attribute class="bool" name="b">true</Attribute>
    <Attribute name="k" cls="a_test_class" module="__main__"/>
    <Attribute name="f" fn="a_test_function" module="__main__"/>
  </Attributes>
</Pickle>

"""

x2 = """
<Pickle xmlns="http://namespaces.zope.org/pickle" class="tuple">
  <Sequence>
    <Item class="bool">true</Item>
    <Item class="bool">false</Item>
  </Sequence>
</Pickle>

"""

x3 = """
<Pickle xmlns="http://namespaces.zope.org/pickle" class="bool">true</Pickle>
"""

# copied the data types from test_bools also
class a_test_class:
    def __init__(self):
        pass

def a_test_function():
    pass

class foo:
    def __init__(self):

        self.a = False
        self.b = True
        self.c = None
        self.f = a_test_function
        self.k = a_test_class

# If this Python doesn't have True/False, then the unpickler
# will return 1/0 instead
#if not pyconfig.Have_TrueFalse():
#    True = 1
#    False = 0

# the tests are portable versions of those in test_bools.py

# bools inside an object
x = xmp.loads(x1)

# check it
if x.a != False or x.b != True or x.c != None or \
   x.f != a_test_function or x.k != a_test_class:
    #print x.__dict__
    raise "ERROR(1)"

# bools inside a toplevel bltin
x = xmp.loads(x2)
if x[0] != True or x[1] != False:
    raise "ERROR(2)"

# bool as toplevel
x = xmp.loads(x3)
if x != True:
    raise "ERROR(3)"

print "** OK **"

