# duplicating this guy

"""<?xml version="1.0"?>
<!DOCTYPE PyObject SYSTEM "PyObjects.dtd">

<PyObject class="Automobile">
   <attr name="doors" type="numeric" value="4" />
   <attr name="make" type="string" value="Honda" />
   <attr name="tow_hitch" type="None" />
   <attr name="prev_owners" type="tuple">
      <item type="string" value="Jane Smith" />
      <item type="tuple">
         <item type="string" value="John Doe" />
         <item type="string" value="Betty Doe" />
      </item>
      <item type="string" value="Charles Ng" />
   </attr>
   <attr name="repairs" type="list">
      <item type="string" value="June 1, 1999:  Fixed radiator" />
      <item type="PyObject" class="Swindle">
         <attr name="date" type="string" value="July 1, 1999" />
         <attr name="swindler" type="string" value="Ed\'s Auto" />
         <attr name="purport" type="string" value="Fix A/C" />
      </item>
   </attr>
   <attr name="options" type="dict">
      <entry>
         <key type="string" value="Cup Holders" />
         <val type="numeric" value="4" />
      </entry>
      <entry>
         <key type="string" value="Custom Wheels" />
         <val type="string" value="Chrome Spoked" />
      </entry>
   </attr>
   <attr name="engine" type="PyObject" class="Engine">
      <attr name="cylinders" type="numeric" value="4" />
      <attr name="manufacturer" type="string" value="Ford" />
   </attr>
</PyObject>"""

from datetime import date

class Automobile(object):
    pass
class Swindle(object):
    pass
class Engine(object):
    pass
def getAuto():
    a = Automobile()
    a.doors = 4
    a.make='Honda'
    a.tow_hitch = None
    a.prev_owners = ("Jane Smith",('John Doe','Betty Doe'),'Charles Ng')
    a.repairs = []
    swindle = Swindle()
    swindle.date = date(1999,7,1)
    swindle.swindler = "Ed's Auto"
    swindle.purport = "Fix A/C"
    a.repairs.append("June 1, 1999: Fixed radiator")
    a.repairs.append(swindle)
    a.options = {}
    a.options['Cup Holders'] = 4
    a.options['Custom Wheels'] = 'Chrome Spoked'
    a.engine = Engine()
    a.engine.cylinders = 4
    a.engine.manufacturer = 'Ford'
    return a

if __name__ == '__main__':
    from zif.sedna.persistence import pickle
    print pickle.dumpsp(getAuto())
    
    

