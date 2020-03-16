==========================================================
ZEO - Single-server client-server database server for ZODB
==========================================================

ZEO is a client-server storage for `ZODB <http://www.zodb.org>`_ for
sharing a single storage among many clients. When you use ZEO, a
lower-level storage, typically a file storage, is opened in the ZEO
server process.  Client programs connect to this process using a ZEO
ClientStorage.  ZEO provides a consistent view of the database to all
clients.  The ZEO client and server communicate using a custom
protocol layered on top of TCP.

ZEO can be installed with pip like most python packages:

.. code-block:: bash

    pip install zeo

Some alternatives to ZEO:

- `NEO <http://www.neoppod.org/>`_ is a distributed-server
  client-server storage.

- `RelStorage <http://relstorage.readthedocs.io/en/latest/>`_
  leverages the RDBMS servers to provide a client-server storage.

Content
=======

.. toctree::
    :maxdepth: 2

    introduction
    server
    clients
    protocol
    nagios
    ordering
    client-cache
    client-cache-tracing
    blob-nfs
    changelog
    reference

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
