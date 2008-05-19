"""
Show that __setstate__ works by pickling the random.Random class --fpm

Note: python-2.0 lacks random, so I need another testcase for completeness
"""
import zif.sedna.persistence.pickle as xml_pickle
import sys, random, pickle
import funcs


class foo: pass

f = foo()

# create random sequences with known seed values
# so we know what to expect after unpickling

r1 = random.Random(1234)
t1 = r1.random()
r1 = random.Random(1234)

r2 = random.Random(4567)
t2 = r2.random()
r2 = random.Random(4567)

r3 = random.Random(8910)
t3 = r3.random()
r3 = random.Random(8910)

r4 = random.Random(1112)
t4 = r4.random()
r4 = random.Random(1112)

f.r = r1
f.l1 = [[1,2,3],r2]
d = {'a':1}
f.l2 = [r3,[4,5,6],r3]
f.d = {'One': 1, 'Two': r4}


# let xml_pickle pull in our imports

# do a side-by-side comparison w/regular pickle, to
# show we do the same thing

sp = pickle.dumps(f)

sx = xml_pickle.dumpsp(f)
#del f
f=None
#import time
#time.sleep(1)
#j = open('setstate_xml.xml','w')
#j.write(sx)
#j.close()

print sx

g = xml_pickle.loads(sx)
h = pickle.loads(sp)

#print sx
print t1, t2, t3, t4
p1 = g.r.random()
p2 = g.l1[1].random()
p3 = g.l2[0].random()
p4 = g.d['Two'].random()

print p1,p2,p3,p4

if t1!=p1 or t2!=p2 or t3!=p3 or t4!=p4:
    raise "ERROR(1)"

#print t1, t2, t3, t4
p1 = h.r.random()
p2 = h.l1[1].random()
p3 = h.l2[0].random()
p4 = h.d['Two'].random()

print p1,p2,p3,p4

if t1!=p1 or t2!=p2 or t3!=p3 or t4!=p4:
    raise "ERROR(2)"

print "** OK **"


