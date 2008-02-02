#Sedna Document and Collection classes

# these are probably useless in production, but probably OK
# for administration.

from lxml.etree import Element, fromstring, tounicode, _Element


class SednaItem(object):
    def __init__(self,IQ,idx,text):
        self.value = text
        self.index = idx
        self.query = IQ
    def replace(self,newValue):
        self.query.connection.execute(u'UPDATE replace $v in %s with %s' % (
            self.query+'['+self.index+']',tounicode(newValue)))

class SednaResult(object):
    def __init__(self,connection,query):
        self.connection = connection
        self.query = query
        result = self.connection.xquery(query)
        self.result = []
        for index,item in enumerate(result):
            self.result.append(IndexedItem(self,index+1,item))


   def indexedQuery(self,query):
        """return the results of a query.

        each item returned from the query will be placed inside an <xpathitem>
        element, This element has an 'xpath' attribute with the query
        and an index indicating the item's index inside the query.  Presumably,
        this xpath may be used to identify the item for updating.
        """
        result = self.connection.execute("%s%s" % (self._sid,query))
        theList = []
        count = 0
        for item in result:
            count += 1
            item = fromstring(item)
            item.set('sedna_xpath',"%s%s[%s]" % (self._sid,query,count))
            theList.append(item)
        return theList

    def update(self,item):
        conn = self.connection
        xpath = item.get('sedna_xpath',None)
        if not xpath:
            raise self.connection.ProgrammingError(
                'May only update xpath results')
        del item.attrib['sedna_xpath']
        conn.execute(u'UPDATE replace $v in %s with %s' % (
            xpath,tounicode(item)))
















class SednaCollection(object):
    """
    a collection of documents in a Sedna database
    """
    def __init__(self,connection,collection_name):
        self.connection = connection
        self.collection = collection_name
        self.document = None
    def create(self):
        conn = self.connection
        coll = self.collection
        if self.collection:
            collections = self.connection.collections
            if not coll in collections:
                conn.execute(u'CREATE COLLECTION "%s"' % coll)
            else:
                raise conn.Warning('Collection %s already exists' % coll)
    def drop(self):
        """
        the opposite of create

        Be careful with this. This will also drop any contained documents.
        """
        conn = self.connection
        coll = self.collection
        if coll:
            collections = self.connection.collections
            if coll in collections:
                conn.execute('DROP COLLECTION "%s"' % coll)
            else:
                raise conn.Warning('Collection %s does not exist')
        else:
            raise conn.ProgrammingError('No name to drop')
    @property
    def _sid(self):
        """
        the first part of a query relating to this entity.

        sid is Sedna ID?
        """
        return u'collection("%s")' % (self.collection)

    def indexedQuery(self,query):
        """return the results of a query.

        each item returned from the query will be placed inside an <xpathitem>
        element, This element has an 'xpath' attribute with the query
        and an index indicating the item's index inside the query.  Presumably,
        this xpath may be used to identify the item for updating.
        """
        result = self.connection.execute("%s%s" % (self._sid,query))
        theList = []
        count = 0
        for item in result:
            count += 1
            item = fromstring(item)
            item.set('sedna_xpath',"%s%s[%s]" % (self._sid,query,count))
            theList.append(item)
        return theList

    def update(self,item):
        conn = self.connection
        xpath = item.get('sedna_xpath',None)
        if not xpath:
            raise self.connection.ProgrammingError(
                'May only update xpath results')
        del item.attrib['sedna_xpath']
        conn.execute(u'UPDATE replace $v in %s with %s' % (
            xpath,tounicode(item)))

    def xquery(self,query):
        """
        Perform an xquery on this document or collection.

        If the query returns data, Output will be a iterable of unicode
        strings.
        """
        return self.connection.query('%s%s' % (self._sid,query))

    def addDocumentFromString(self,doc_name,s):
        if isinstance(s,_Element):
            s = tounicode(s)
        if not s:
            raise self.conn.ProgrammingError('expected XML, got (%s)' % s)
        self.connection.loadText(s,doc_name,self.collection)

    def addDocumentFromFile(self,doc_name,file_path):
        self.connection.loadFile(file_path,doc_name,self.collection)

    def dropDocument(self,doc_name):
        conn = self.connection
        coll = self.collection
        doc = doc_name
        conn.execute('DROP DOCUMENT "%s" IN COLLECTION "%s"' % (doc,coll))

    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()



class SednaDocument(SednaCollection):
    def __init__(self,connection,document_name,collection_name=None):
        self.connection = connection
        self.document = document_name
        self.collection = collection_name

    @property
    def _sid(self):
        """
        the first part of a query relating to this entity.

        sid is Sedna ID?
        """
        if self.collection:
            return u'document("%s","%s")' % (self.document, self.collection)
        return u'document("%s")' % (self.document)

    def drop(self):
        """
        the opposite of create

        Be careful with this. If doc is in a collection, we delete it from
        there.
        """
        conn = self.connection
        coll = self.collection
        doc = self.document
        if coll:
            conn.execute('DROP DOCUMENT "%s" IN COLLECTION "%s"' % (doc,coll))
        else:
            conn.execute('DROP DOCUMENT "%s"' % (doc,))
