import doctest

def doTests():
    doctest.testfile('README.txt')
    doctest.testfile('README_sednaobject.txt')

if __name__ == '__main__':
    doTests()

