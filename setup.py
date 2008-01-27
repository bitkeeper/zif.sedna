"""

zif.sedna
Copyright (c) 2007, Virginia Polytechnic Institute and State University
All rights reserved. Refer to LICENSE.txt for details of distribution and use.

Distutils setup

"""

import os
from setuptools import setup, find_packages

def read(*rnames):
    return open(os.path.join(os.path.dirname(__file__), *rnames)).read()

setup(
    name = 'zif.sedna',
    version = '0.2dev',
    license = 'BSD',
    description = 'Sedna connector from the zif collective',
    long_description = (
        read('README.txt')
        + '\n' +
        'Detailed Documentation\n'
        '**********************\n'
        + '\n' +
        read('src', 'zif', 'sedna', 'README.txt')
        + '\n' +
        'Download\n'
        '**********************\n'
        ),
    keywords = 'sedna xml database',
    author = 'Jim Washington',
    author_email = 'jwashin@vt.edu',
    url = 'http://zif.svn.sourceforge.net/viewvc/zif/zif.sedna',
    packages = find_packages('src'),
    package_dir = {'':'src'},
    namespace_packages = ['zif'],
    install_requires = ['setuptools','elementtree'],
    include_package_data = True,
    zip_safe = False,
    )
