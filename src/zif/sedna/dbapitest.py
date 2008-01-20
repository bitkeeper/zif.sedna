from dbapi import connect

from lxml.etree import parse

s = connect('dbi://SYSTEM:MANAGER@localhost:5050/test')

g = s.cursor()

g.execute('for $item in doc("ot")//v'+\
    ' where contains($item,"begat") return $item')
items = []
for z in g.fetchall():
    items.append(parse(z))
g = z[0]
for name,value in g.docinfo.__dict__.items():
    print name,value

