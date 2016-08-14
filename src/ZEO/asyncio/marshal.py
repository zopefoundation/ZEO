##############################################################################
#
# Copyright (c) 2001, 2002 Zope Foundation and Contributors.
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
"""Support for marshaling ZEO messages

Not to be confused with marshaling objects in ZODB.

We currently use pickle. In the future, we may use a
Python-independent format, or possibly a minimal pickle subset.
"""

import logging

from .._compat import Unpickler, Pickler, BytesIO, PY3, PYPY
from ..shortrepr import short_repr

logger = logging.getLogger(__name__)

def encoder():
    """Return a non-thread-safe encoder
    """

    if PY3 or PYPY:
        f = BytesIO()
        getvalue = f.getvalue
        seek = f.seek
        truncate = f.truncate
        pickler = Pickler(f, 3 if PY3 else 1)
        pickler.fast = 1
        dump = pickler.dump
        def encode(*args):
            seek(0)
            truncate()
            dump(args)
            return getvalue()
    else:
        pickler = Pickler(1)
        pickler.fast = 1
        dump = pickler.dump
        def encode(*args):
            return dump(args, 2)

    return encode

def encode(*args):

    return encoder()(*args)

def decode(msg):
    """Decodes msg and returns its parts"""
    unpickler = Unpickler(BytesIO(msg))
    unpickler.find_global = find_global
    try:
        # PyPy, zodbpickle, the non-c-accelerated version
        unpickler.find_class = find_global
    except AttributeError:
        pass
    try:
        return unpickler.load() # msgid, flags, name, args
    except:
        logger.error("can't decode message: %s" % short_repr(msg))
        raise

def server_decode(msg):
    """Decodes msg and returns its parts"""
    unpickler = Unpickler(BytesIO(msg))
    unpickler.find_global = server_find_global
    try:
        # PyPy, zodbpickle, the non-c-accelerated version
        unpickler.find_class = server_find_global
    except AttributeError:
        pass

    try:
        return unpickler.load() # msgid, flags, name, args
    except:
        logger.error("can't decode message: %s" % short_repr(msg))
        raise

_globals = globals()
_silly = ('__doc__',)

exception_type_type = type(Exception)

def find_global(module, name):
    """Helper for message unpickler"""
    try:
        m = __import__(module, _globals, _globals, _silly)
    except ImportError as msg:
        raise ImportError("import error %s: %s" % (module, msg))

    try:
        r = getattr(m, name)
    except AttributeError:
        raise ImportError("module %s has no global %s" % (module, name))

    safe = getattr(r, '__no_side_effects__', 0)
    if safe:
        return r

    # TODO:  is there a better way to do this?
    if type(r) == exception_type_type and issubclass(r, Exception):
        return r

    raise ImportError("Unsafe global: %s.%s" % (module, name))

def server_find_global(module, name):
    """Helper for message unpickler"""
    try:
        if module != 'ZopeUndo.Prefix':
            raise ImportError
        m = __import__(module, _globals, _globals, _silly)
    except ImportError as msg:
        raise ImportError("import error %s: %s" % (module, msg))

    try:
        r = getattr(m, name)
    except AttributeError:
        raise ImportError("module %s has no global %s" % (module, name))

    return r
