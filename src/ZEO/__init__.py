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
"""ZEO -- Zope Enterprise Objects.

See the file README.txt in this directory for an overview.

ZEO is now part of ZODB; ZODB's home on the web is

    http://wiki.zope.org/ZODB

"""


def client(*args, **kw):
    """
    Shortcut for :class:`ZEO.ClientStorage.ClientStorage`.
    """
    import ZEO.ClientStorage
    return ZEO.ClientStorage.ClientStorage(*args, **kw)


def DB(*args, **kw):
    """
    Shortcut for creating a :class:`ZODB.DB` using a ZEO :func:`~ZEO.client`.
    """
    s = client(*args, **kw)
    try:
        import ZODB
        return ZODB.DB(s)
    except Exception:
        s.close()
        raise


def connection(*args, **kw):
    db = DB(*args, **kw)
    try:
        return db.open_then_close_db_when_connection_closes()
    except Exception:
        db.close()
        raise


def server(path=None, blob_dir=None, storage_conf=None, zeo_conf=None,
           port=0, threaded=True, **kw):
    """Convenience function to start a server for interactive exploration

    This fuction starts a ZEO server, given a storage configuration or
    a file-storage path and blob directory.  You can also supply a ZEO
    configuration string or a port.  If neither a ZEO port or
    configuration is supplied, a port is chosen randomly.

    The server address and a stop function are returned. The address
    can be passed to ZEO.ClientStorage.ClientStorage or ZEO.DB to
    create a client to the server. The stop function can be called
    without arguments to stop the server.

    Arguments:

    path
       A file-storage path. This argument is ignored if a storage
       configuration is supplied.

    blob_dir
       A blob directory path. This argument is ignored if a storage
       configuration is supplied.

    storage_conf
       A storage configuration string.  If none is supplied, then at
       least a file-storage path must be supplied and the storage
       configuration will be generated from the file-storage path and
       the blob directory.

    zeo_conf
       A ZEO server configuration string.

    port
       If no ZEO configuration is supplied, the one will be computed
       from the port.  If no port is supplied, one will be chosedn
       dynamically.

    """
    import ZEO._forker as forker
    if storage_conf is None and path is None:
        storage_conf = '<mappingstorage>\n</mappingstorage>'

    return forker.start_zeo_server(
        storage_conf, zeo_conf, port, keep=True, path=path,
        blob_dir=blob_dir, suicide=False, threaded=threaded, **kw)
