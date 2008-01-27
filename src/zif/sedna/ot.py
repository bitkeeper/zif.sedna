import protocol
import sys

username = 'SYSTEM'
password = 'MANAGER'
database = 'test'
port = 5050
host = 'localhost'

from lxml.etree import fromstring

import logging
logging.basicConfig(stream=sys.stdout)
log = logging.getLogger()
log.setLevel(logging.DEBUG)

conn = protocol.SednaProtocol(host,database,username,password,trace=True)

docs = conn.documents

qry2 = "doc('ot')//v[contains(., 'begat')]/text()"
qry1 = 'for $item in doc("ot")//v'+\
    ' where contains($item,"begat") return $item'

if not 'ot' in docs:
    # looking for a file from Jon Bosa
    conn.loadFile('/home/jwashin/Desktop/ot/ot.xml', 'ot')
begat_verses = conn.query(qry2)
print begat_verses.time
conn.traceOff()
count = 0
for k in begat_verses:
    count += 1
    #z = fromstring(k)
    #print count,z.text.strip()
    print count, k.strip()
conn.commit()
conn.close()