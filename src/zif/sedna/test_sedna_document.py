from sednadocument import SednaDocument, SednaCollection

from lxml.etree import Element, SubElement, tostring

#from dbapi import connect
from protocol import SednaProtocol

#conn = connect('dbi://SYSTEM:MANAGER@localhost:5050/test')
conn = SednaProtocol('localhost',5050,'SYSTEM','MANAGER','test')
import logging
import sys
logging.basicConfig(stream=sys.stdout)
log = logging.getLogger()
log.setLevel(logging.DEBUG)
conn.traceOn()

j = SednaCollection(conn,'test_collection')
try:
    j.create()
    #j.commit()
except conn.Warning:
    pass
j.addDocumentFromString('hello',u'<hello/>')

s = Element('test1')
for k in range(10):
    y = SubElement(s,'test2')
    for z in range(5):
        SubElement(y,'test3')

j.addDocumentFromString('test2',s)
doc = SednaDocument(conn,'hello','test_collection')

d = doc.xpath('//hello')
hello = d[0]
t = SubElement(hello,'world')
for k in range(1,10):
    y = SubElement(t,'index')
    y.text = str(k)
print tostring(hello)
doc.update(hello)

z = doc.xpath('//index')
item = z[3]
m = SubElement(item,'newindex')
m.set('test_attrib','Hello!')
doc.update(item)

print doc.xquery('//hello').value
j.drop()
conn.commit()
conn.close()