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

PYPY = getattr(platform, 'python_implementation', lambda: None)() == 'PyPy'
WIN = sys.platform.startswith('win')

from zodbpickle.pickle import Pickler, Unpickler as _Unpickler, dump, dumps, loads
class Unpickler(_Unpickler):
    # Python 3 doesn't allow assignments to find_global,
    # instead, find_class can be overridden

    find_global = None

    def find_class(self, modulename, name):
        if self.find_global is None:
            return super(Unpickler, self).find_class(modulename, name)
        return self.find_global(modulename, name)

# String and Bytes IO
from ZODB._compat import BytesIO

import _thread as thread
from threading import get_ident

try:
    from cStringIO import StringIO
except:
    from io import StringIO
