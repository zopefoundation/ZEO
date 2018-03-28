##############################################################################
#
# Copyright (c) 2002 Zope Foundation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE
#
##############################################################################
"""Python versions compatiblity
"""
import sys
import platform

PY3 = sys.version_info[0] >= 3
PY32 = sys.version_info[:2] == (3, 2)
PYPY = getattr(platform, 'python_implementation', lambda: None)() == 'PyPy'
WIN = sys.platform.startswith('win')

if PY3:
    from zodbpickle.pickle import Pickler, Unpickler as _Unpickler, dump, dumps, loads
    class Unpickler(_Unpickler):
        # Py3: Python 3 doesn't allow assignments to find_global,
        # instead, find_class can be overridden

        find_global = None

        def find_class(self, modulename, name):
            if self.find_global is None:
                return super(Unpickler, self).find_class(modulename, name)
            return self.find_global(modulename, name)
else:
    try:
        import zodbpickle.fastpickle as cPickle
    except ImportError:
        import zodbpickle.pickle as cPickle
    Pickler = cPickle.Pickler
    Unpickler = cPickle.Unpickler
    dump = cPickle.dump
    dumps = cPickle.dumps
    loads = cPickle.loads

# String and Bytes IO
from ZODB._compat import BytesIO

if PY3:

    import _thread as thread
    if PY32:
        from threading import _get_ident as get_ident
    else:
        from threading import get_ident


else:

    import thread
    from thread import get_ident

try:
    from cStringIO import StringIO
except:
    from io import StringIO
