from zlib import compress
import cPickle
import zif.sedna.persistence.pickle as xml_pickle

class C: pass

#-----------------------------------------------------------------------
# First: look at the results of compression on a large object
#-----------------------------------------------------------------------
o = C()
from os import sep
fname = sep.join(xml_pickle.__file__.split(sep)[:-1])+sep+'pickle.py'
o.lst = open(fname).readlines()
o.tup = tuple(o.lst)
o.dct = {}
for i in range(500):
    o.dct[i] = i

print "Size of standard cPickle:    ", len(cPickle.dumps(o))
print "Size of binary cPickle:      ", len(cPickle.dumps(o,1))

print "gzip'd standard cPickle:     ", len(compress(cPickle.dumps(o)))
print "gzip'd binary cPickle:       ", len(compress(cPickle.dumps(o,1)))

print "Size of standard xml_pickle: ", len(xml_pickle.dumps(o))
print "Size of gzip'd xml_pickle:   ", len(xml_pickle.dumps(o,1))

#print xml_pickle.dumpsp(o)
#-----------------------------------------------------------------------
# Second: look at actual compressed and uncompressed (small) pickles
#-----------------------------------------------------------------------
o = C()
o.lst = [1,2]
o.dct = {'spam':'eggs'}

s = xml_pickle.dumps(o)
print "--------------- Uncompressed xml_pickle: (%s bytes)---------------" % len(s)
print s

s = xml_pickle.dumps(o,1)
print "--------------- Compressed xml_pickle: (%s bytes)-----------------" % len(s)
print `s`

#-----------------------------------------------------------------------
# Third: make sure compressed pickle makes it through restore cycle
#-----------------------------------------------------------------------
print "------------------------------------------------------"
try:
    xml_pickle.loads(xml_pickle.dumps(o,1))
    print "Pickle/restore cycle with compression:  OK"
except:
    print "Pickle/restore cycle with compression:  FAILED!"

try:
    xml_pickle.loads('CORRUPT'+xml_pickle.dumps(o))
    print "Pickle/restore corrupt data with compression:  OK"
except:
    print "Pickle/restore corrupt data with compression:  FAILED!"

