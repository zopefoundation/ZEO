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
from io import BytesIO

from zodbpickle.pickle import Pickler

from .._compat import Unpickler
from ..shortrepr import short_repr


logger = logging.getLogger(__name__)


def encoder(protocol, server=False):
    """Return a non-thread-safe encoder
    """

    if protocol[:1] == b'M':
        from msgpack import packb

        default = server_default if server else None

        def encode(*args):
            return packb(
                args, use_bin_type=True, default=default)

        return encode
    else:
        assert protocol[:1] == b'Z'

    f = BytesIO()
    getvalue = f.getvalue
    seek = f.seek
    truncate = f.truncate
    pickler = Pickler(f, 3)
    pickler.fast = 1
    dump = pickler.dump

    def encode(*args):
        seek(0)
        truncate()
        dump(args)
        return getvalue()

    return encode


def encode(*args):

    return encoder(b'Z')(*args)


def decoder(protocol):
    if protocol[:1] == b'M':
        from msgpack import unpackb

        def msgpack_decode(data):
            """Decodes msg and returns its parts"""
            return unpackb(data, raw=False, use_list=False)

        return msgpack_decode
    else:
        assert protocol[:1] == b'Z'
        return pickle_decode


def pickle_decode(msg):
    """Decodes msg and returns its parts"""
    unpickler = Unpickler(BytesIO(msg))
    unpickler.find_global = find_global
    try:
        # PyPy, zodbpickle, the non-c-accelerated version
        unpickler.find_class = find_global
    except AttributeError:
        pass
    try:
        return unpickler.load()  # msgid, flags, name, args
    except:  # NOQA: E722 bare except
        logger.error("can't decode message: %s" % short_repr(msg))
        raise


def server_decoder(protocol):
    if protocol[:1] == b'M':
        return decoder(protocol)
    else:
        assert protocol[:1] == b'Z'
        return pickle_server_decode


def pickle_server_decode(msg):
    """Decodes msg and returns its parts"""
    unpickler = Unpickler(BytesIO(msg))
    unpickler.find_global = server_find_global
    try:
        # PyPy, zodbpickle, the non-c-accelerated version
        unpickler.find_class = server_find_global
    except AttributeError:
        pass

    try:
        return unpickler.load()  # msgid, flags, name, args
    except:  # NOQA: E722 bare except
        logger.error("can't decode message: %s" % short_repr(msg))
        raise


def server_default(obj):
    if isinstance(obj, Exception):
        return reduce_exception(obj)
    else:
        return obj


def reduce_exception(exc):
    class_ = exc.__class__
    class_ = f'{class_.__module__}.{class_.__name__}'
    return class_, exc.__dict__ or exc.args


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
        raise ImportError(f'import error {module}: {msg}')

    try:
        r = getattr(m, name)
    except AttributeError:
        raise ImportError(f'module {module} has no global {name}')

    safe = getattr(r, '__no_side_effects__', 0)
    if safe:
        return r

    # TODO:  is there a better way to do this?
    if type(r) == exception_type_type and issubclass(r, Exception):
        return r

    raise ImportError(f'Unsafe global: {module}.{name}')


def server_find_global(module, name):
    """Helper for message unpickler"""
    if module not in _SAFE_MODULE_NAMES:
        raise ImportError(f'Module not allowed: {module}')

    try:
        m = __import__(module, _globals, _globals, _silly)
    except ImportError as msg:
        raise ImportError(f'import error {module}: {msg}')

    try:
        r = getattr(m, name)
    except AttributeError:
        raise ImportError(f'module {module} has no global {name}')

    return r
