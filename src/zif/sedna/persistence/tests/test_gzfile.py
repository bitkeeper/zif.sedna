
# test binary pickling

import zif.sedna.persistence.pickle as xml_pickle
import gzip
from StringIO import StringIO
import tempfile

# --- Make an object to play with ---
class C: pass
o = C()
o.lst, o.dct = [1,2], {'spam':'eggs'}

x = xml_pickle.dumps(o,1,xml_declaration=True)

# make sure xml_pickle really gzipped it
sio = StringIO(x)
gz = gzip.GzipFile('dummy','rb',9,sio)
if gz.read(5) != '<?xml':
    raise "ERROR(1)"

# reload object
o2 = xml_pickle.loads(x)

# check it
if o.lst != o2.lst or o.dct != o2.dct:
    raise "ERROR(2)"

temp = tempfile.TemporaryFile()

p = xml_pickle.dump(o,temp,1)

o2 = xml_pickle.load(temp)
if o.lst != o2.lst or o.dct != o2.dct:
    raise "ERROR(3)"

print "** OK **"

