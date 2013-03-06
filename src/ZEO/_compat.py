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

PY3 = sys.version_info[0] >= 3

# Pickle support
from ZODB._compat import Pickler, Unpickler, dump, dumps, loads

# String and Bytes IO
from ZODB._compat import BytesIO

if PY3:

    import _thread as thread
    from threading import get_ident

else:

    import thread
    from thread import get_ident

try:
    from cStringIO import StringIO
except:
    from io import StringIO
