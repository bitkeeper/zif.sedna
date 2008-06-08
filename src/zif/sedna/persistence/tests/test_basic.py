#-- Hand generated test object
#
# if you change this, you have to sync w/the checks below

# module should be zif.sedna.persistence.tests.example1
test_xml = """
<pickle xmlns="http://namespaces.zope.org/pickle" xmlns:o="http://namespaces.zope.org/pyobj" class="Automobile" module="zif.sedna.persistence.tests.example1">
  <o:engine class="Engine" module="zif.sedna.persistence.tests.example1">
    <o:cylinders class="int">4</o:cylinders>
    <o:manufacturer class="str">Ford</o:manufacturer>
  </o:engine>
  <o:repairs class="list">
    <item class="str">June 1, 1999: Fixed radiator</item>
    <item class="Swindle" module="zif.sedna.persistence.tests.example1">
      <o:date class="date" module="datetime">1999-07-01<_reduction class="tuple"><item cls="date" module="datetime"/><item class="tuple"><item class="str" enc="base64">B88HAQ==</item></item></_reduction></o:date>
      <o:swindler class="str">Ed's Auto</o:swindler>
      <o:purport class="str">Fix A/C</o:purport>
    </item>
  </o:repairs>
  <o:doors class="int">4</o:doors>
  <o:prev_owners class="tuple">
    <item class="str">Jane Smith</item>
    <item class="tuple">
      <item class="str">John Doe</item>
      <item class="str">Betty Doe</item>
    </item>
    <item class="str">Charles Ng</item>
  </o:prev_owners>
  <o:tow_hitch class="NoneType">None</o:tow_hitch>
  <o:make class="str">Honda</o:make>
  <o:options class="dict">
    <key class="str">Cup Holders<val class="int">4</val></key>
    <key class="str">Custom Wheels<val class="str">Chrome Spoked</val></key>
  </o:options>
</pickle>
"""

if __name__=='__main__':
    #from gnosis.xml.pickle import XML_Pickler
    #import gnosis.xml.pickle as xml_pickle	
    #from gnosis.xml.pickle.util import add_class_to_store
    import funcs

    import zif.sedna.persistence.pickle as xml_pickle
    import datetime
    class MyClass: pass
    o = MyClass()
    o.num = 37
    o.children = None
    o.married = True
    o.divorced = False
    o.c_names = []
    o.pet_names = ()
    o.str = "Hello World \n Special Chars: \t \000 <> & ' \207"
    o.lst = [1, 3.5, 2, 4+7j]
    o.lst2 = o.lst
    o.date = datetime.date(2008,5,18)
    o.dtm = datetime.datetime.now()
    o2 = MyClass()
    o2.tup = ("x", "y", "z")
    o2.tup2 = o2.tup
    o2.num = 2+2j
    o2.dct = { "this": "that", "spam": "eggs", 3.14: "about PI" }
    o2.dct2 = o2.dct
    o.obj = o2
    
    #print '------* Print python-defined pickled object *-----'
    # pickle it
    s = xml_pickle.dumpsp(o)
    #print '------* Load it and print it again *-----'
    #print '------ should look approximately the same ---'
    print s
    # unpickle it
    #for attr in ['num','str','lst','lst2','dtm','date']:
        #print attr, getattr(o,attr)
    t = xml_pickle.loads(s)

    # sanity, can't possibly happen
    if id(o) == id(t) or id(o.obj) == id(t.obj):
        raise "ERROR(0)"

    # check that it is the same
    for attr in ['num','str','lst','lst2','dtm','children','married','divorced',
            'c_names','pet_names','date','dtm']:
        if getattr(o,attr) != getattr(t,attr):
            #print getattr(o,attr), getattr(t,attr)
            raise "ERROR(1)"

    for attr in ['tup','tup2','num','dct','dct2']:
        if getattr(o.obj,attr) != getattr(t.obj,attr):
            raise "ERROR(2)"

    #print t.dumps()
    #print '-----* Load a test xml_pickle object, and print it *-----'
    u = xml_pickle.loads(test_xml)

    # check it
    if u.engine.__dict__ != {'cylinders': 4, 'manufacturer': 'Ford'}:
        raise "ERROR(4)"
    if u.repairs[0] != 'June 1, 1999: Fixed radiator':
        raise "ERROR(4)"
    if u.repairs[1].__dict__ != {'date': datetime.date(1999,7,1),
            'swindler': "Ed's Auto", 'purport': 'Fix A/C'}:
        print u.repairs[1].date
        raise "ERROR(4)"
    if u.make != 'Honda':
        raise "ERROR(4)"
    if u.prev_owners != ('Jane Smith', ('John Doe', 'Betty Doe'), 'Charles Ng'):
        raise "ERROR(4)"
    if u.doors != 4:
        raise "ERROR(4)"
    if u.tow_hitch != None:
        raise "ERROR(4)"
    if u.options != {'Cup Holders': 4, 'Custom Wheels': 'Chrome Spoked'}:
            raise "ERROR(4)"

    print "** OK **"



