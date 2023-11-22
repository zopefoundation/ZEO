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
import platform
import sys

from zodbpickle.pickle import Unpickler as _Unpickler


PYPY = getattr(platform, 'python_implementation', lambda: None)() == 'PyPy'
WIN = sys.platform.startswith('win')


class Unpickler(_Unpickler):
    # Python 3 doesn't allow assignments to find_global,
    # instead, find_class can be overridden

    find_global = None

    def find_class(self, modulename, name):
        if self.find_global is None:
            return super().find_class(modulename, name)
        return self.find_global(modulename, name)
