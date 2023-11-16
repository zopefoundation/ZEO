=================
ZEO Nagios plugin
=================

ZEO includes a script that provides a nagios monitor plugin:

    >>> import pkg_resources, time
    >>> nagios = pkg_resources.load_entry_point(
    ...     'ZEO', 'console_scripts', 'zeo-nagios')

In it's simplest form, the script just checks if it can get status:

    >>> import ZEO
    >>> addr, stop = ZEO.server('test.fs', threaded=False)
    >>> saddr = ':'.join(map(str, addr)) # (host, port) -> host:port

    >>> nagios([saddr])
    Empty storage '1'
    1

The storage was empty. In that case, the monitor warned as much.

Let's add some data:

    >>> ZEO.DB(addr).close()
    >>> nagios([saddr])
    OK

If we stop the server, we'll error:

    >>> stop()
    >>> nagios([saddr])
    Can't connect [Errno 61] Connection refused
    2

Metrics
=======

The monitor will optionally output server metric data. There are 2
kinds of metrics it can output, level and rate metric. If we use the
-m/--output-metrics option, we'll just get rate metrics:

    >>> addr, stop = ZEO.server('test.fs', threaded=False)
    >>> saddr = ':'.join(map(str, addr)) # (host, port) -> host:port
    >>> nagios([saddr, '-m'])
    OK|active_txns=0
    | connections=0
     waiting=0

We only got the metrics that are levels, like current number of
connections.  If we want rate metrics, we need to be able to save
values from run to run.  We need to use the -s/--status-path option to
specify the name of a file for status information:

    >>> nagios([saddr, '-m', '-sstatus'])
    OK|active_txns=0
    | connections=0
     waiting=0

We still didn't get any rate metrics, because we've only run once.
Let's actually do something with the database and then make another
sample.

    >>> db = ZEO.DB(addr)
    >>> nagios([saddr, '-m', '-sstatus'])
    OK|active_txns=0
    | connections=1
     waiting=0
     aborts=0.0
     commits=0.0
     conflicts=0.0
     conflicts_resolved=0.0
     loads=81.226297803
     stores=0.0

Note that this time, we saw that there was a connection.

The ZEO.nagios module provides a check function that can be used by
other monitors (e.g. that get address data from ZooKeeper). It takes:

- Address string,

- Metrics flag.

- Status file name (or None), and

- Time units for rate metrics

::

    >>> import ZEO.nagios
    >>> ZEO.nagios.check(saddr, True, 'status', 'seconds')
    OK|active_txns=0
    | connections=1
     waiting=0
     aborts=0.0
     commits=0.0
     conflicts=0.0
     conflicts_resolved=0.0
     loads=0.0
     stores=0.0

    >>> db.close()
    >>> stop()

Multi-storage servers
=====================

A ZEO server can host multiple servers.  (This is a feature that will
likely be dropped in the future.)  When this is the case, the monitor
profixes metrics with a storage id.

    >>> addr, stop = ZEO.server(
    ...     storage_conf = """
    ... <mappingstorage first>
    ... </mappingstorage>
    ... <mappingstorage second>
    ... </mappingstorage>
    ... """, threaded=False)
    >>> saddr = ':'.join(map(str, addr)) # (host, port) -> host:port
    >>> nagios([saddr, '-m', '-sstatus'])
    Empty storage 'first'|first:active_txns=0
    Empty storage 'second'
    | first:connections=0
     first:waiting=0
     second:active_txns=0
     second:connections=0
     second:waiting=0
    1
    >>> nagios([saddr, '-m', '-sstatus'])
    Empty storage 'first'|first:active_txns=0
    Empty storage 'second'
    | first:connections=0
     first:waiting=0
     second:active_txns=0
     second:connections=0
     second:waiting=0
     first:aborts=0.0
     first:commits=0.0
     first:conflicts=0.0
     first:conflicts_resolved=0.0
     first:loads=42.42
     first:stores=0.0
     second:aborts=0.0
     second:commits=0.0
     second:conflicts=0.0
     second:conflicts_resolved=0.0
     second:loads=42.42
     second:stores=0.0
    1

    >>> stop()
