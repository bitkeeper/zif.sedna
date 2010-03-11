"""

zif.sedna
Copyright (c) 2007, Virginia Polytechnic Institute and State University
All rights reserved. Refer to LICENSE.txt for details of distribution and use.

Namespace package
 
"""

try:
    __import__('pkg_resources').declare_namespace(__name__)
except ImportError:
    from pkgutil import extend_path
    __path__ = extend_path(__path__, __name__)
