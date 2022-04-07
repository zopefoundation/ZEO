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

from ZODB._compat import BytesIO  # NOQA: F401 unused import

PY3 = sys.version_info[0] >= 3
PY32 = sys.version_info[:2] == (3, 2)
PYPY = getattr(platform, 'python_implementation', lambda: None)() == 'PyPy'
WIN = sys.platform.startswith('win')

if PY3:
    from zodbpickle.pickle import dump
    from zodbpickle.pickle import dumps
    from zodbpickle.pickle import loads
    from zodbpickle.pickle import Pickler
    from zodbpickle.pickle import Unpickler as _Unpickler

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

if PY3:
    import _thread as thread  # NOQA: F401 unused import
    if PY32:
        from threading import _get_ident as get_ident  # NOQA: F401 unused
    else:
        from threading import get_ident  # NOQA: F401 unused import
else:
    import thread  # NOQA: F401 unused import
    from thread import get_ident  # NOQA: F401 unused import

try:
    from cStringIO import StringIO  # NOQA: F401 unused import
except ImportError:
    from io import StringIO  # NOQA: F401 unused import
