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

conn = protocol.SednaProtocol(host,port,username,password,database)

conn.traceOn()

docs = conn.documents

# looking for a file from Jon Bosa

if not 'ot' in docs:
    conn.loadFile('/some_path/ot/ot.xml', 'ot')
begat_verses = conn.query('for $item in doc("ot")//v'+\
    ' where contains($item,"begat") return $item')
conn.traceOff()
count = 0
for k in begat_verses:
    count += 1
    z = fromstring(k)
    print count,z.text.strip()
conn.commit()
conn.close()