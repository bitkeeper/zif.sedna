import doctest
import unittest


def doTests():
    doctest.testfile('README.txt')
    doctest.testfile('README_sednaobject.txt')

def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(doctest.DocFileSuite('README.txt'))
    suite.addTest(doctest.DocFileSuite('README_sednaobject.txt'))
    #suite.addTest(doctest.DocFileSuite('rtestpath.txt'))
    return suite

    #return unittest.Suite((
        #unittest.DocFileSuite('README.txt'),
        #unittest.DocFileSuite('README_sednaobject.txt')))

if __name__ == '__main__':
    runner = unittest.TextTestRunner()
    suite = test_suite()
    runner.run(suite)

