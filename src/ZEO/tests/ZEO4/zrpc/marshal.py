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
import logging

from ZEO._compat import Unpickler, Pickler, BytesIO, PY3, PYPY
from .error import ZRPCError
from .log import log, short_repr

PY2 = not PY3

def encode(*args): # args: (msgid, flags, name, args)
    # (We used to have a global pickler, but that's not thread-safe. :-( )

    # It's not thread safe if, in the couse of pickling, we call the
    # Python interpeter, which releases the GIL.

    # Note that args may contain very large binary pickles already; for
    # this reason, it's important to use proto 1 (or higher) pickles here
    # too.  For a long time, this used proto 0 pickles, and that can
    # bloat our pickle to 4x the size (due to high-bit and control bytes
    # being represented by \xij escapes in proto 0).
    # Undocumented:  cPickle.Pickler accepts a lone protocol argument;
    # pickle.py does not.
    # XXX: Py3: Needs optimization.
    f = BytesIO()
    pickler = Pickler(f, 3)
    pickler.fast = 1
    pickler.dump(args)
    res = f.getvalue()
    return res



if PY3:
    # XXX: Py3: Needs optimization.
    fast_encode = encode
elif PYPY:
    # can't use the python-2 branch, need a new pickler
    # every time, getvalue() only works once
    fast_encode = encode
else:
    def fast_encode():
        # Only use in cases where you *know* the data contains only basic
        # Python objects
        pickler = Pickler(1)
        pickler.fast = 1
        dump = pickler.dump
        def fast_encode(*args):
            return dump(args, 1)
        return fast_encode
    fast_encode = fast_encode()

def decode(msg):
    """Decodes msg and returns its parts"""
    unpickler = Unpickler(BytesIO(msg))
    unpickler.find_global = find_global
    try:
        unpickler.find_class = find_global # PyPy, zodbpickle, the non-c-accelerated version
    except AttributeError:
        pass
    try:
        return unpickler.load() # msgid, flags, name, args
    except:
        log("can't decode message: %s" % short_repr(msg),
            level=logging.ERROR)
        raise

def server_decode(msg):
    """Decodes msg and returns its parts"""
    unpickler = Unpickler(BytesIO(msg))
    unpickler.find_global = server_find_global
    try:
        unpickler.find_class = server_find_global # PyPy, zodbpickle, the non-c-accelerated version
    except AttributeError:
        pass

    try:
        return unpickler.load() # msgid, flags, name, args
    except:
        log("can't decode message: %s" % short_repr(msg),
            level=logging.ERROR)
        raise

_globals = globals()
_silly = ('__doc__',)

exception_type_type = type(Exception)

_SAFE_MODULE_NAMES = (
    'ZopeUndo.Prefix', 'zodbpickle',
    'builtins', 'copy_reg', '__builtin__',
)

def find_global(module, name):
    """Helper for message unpickler"""
    try:
        m = __import__(module, _globals, _globals, _silly)
    except ImportError as msg:
        raise ZRPCError("import error %s: %s" % (module, msg))

    try:
        r = getattr(m, name)
    except AttributeError:
        raise ZRPCError("module %s has no global %s" % (module, name))

    safe = getattr(r, '__no_side_effects__', 0) or (PY2 and module in _SAFE_MODULE_NAMES)
    if safe:
        return r

    # TODO:  is there a better way to do this?
    if type(r) == exception_type_type and issubclass(r, Exception):
        return r

    raise ZRPCError("Unsafe global: %s.%s" % (module, name))

def server_find_global(module, name):
    """Helper for message unpickler"""
    if module not in _SAFE_MODULE_NAMES:
        raise ImportError("Module not allowed: %s" % (module,))

    try:
        m = __import__(module, _globals, _globals, _silly)
    except ImportError as msg:
        raise ZRPCError("import error %s: %s" % (module, msg))

    try:
        r = getattr(m, name)
    except AttributeError:
        raise ZRPCError("module %s has no global %s" % (module, name))

    return r
