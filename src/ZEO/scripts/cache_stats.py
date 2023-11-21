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
"""Trace file statistics analyzer.

File format:

Each record is 26 bytes, plus a variable number of bytes to store an oid,
with the following layout.  Numbers are big-endian integers.

Offset  Size  Contents

0       4     timestamp (seconds since 1/1/1970)
4       3     data size, in 256-byte increments, rounded up
7       1     code (see below)
8       2     object id length
10      8     start tid
18      8     end tid
26  variable  object id

The code at offset 7 packs three fields:

Mask    bits  Contents

0x80    1     set if there was a non-empty version string
0x7e    6     function and outcome code
0x01    1     current cache file (0 or 1)

The "current cache file" bit is no longer used; it refers to a 2-file
cache scheme used before ZODB 3.3.

The function and outcome codes are documented in detail at the end of
this file in the 'explain' dictionary.  Note that the keys there (and
also the arguments to _trace() in ClientStorage.py) are 'code & 0x7e',
i.e. the low bit is always zero.
"""
import argparse
import gzip
import struct
import sys
import time
# we assign ctime locally to facilitate test replacement!
from time import ctime


def add_interval_argument(parser):
    def _interval(a):
        interval = int(60 * float(a))
        if interval <= 0:
            interval = 60
        elif interval > 3600:
            interval = 3600
        return interval
    parser.add_argument(
        "--interval", "-i",
        default=15*60, type=_interval,
        help="summarizing interval in minutes (default 15; max 60)")


def add_tracefile_argument(parser):

    class GzipFileType(argparse.FileType):
        def __init__(self):
            super().__init__(mode='rb')

        def __call__(self, s):
            f = super().__call__(s)
            if s.endswith(".gz"):
                f = gzip.GzipFile(filename=s, fileobj=f)
            return f

    parser.add_argument("tracefile", type=GzipFileType(),
                        help="The trace to read; may be gzipped")


def main(args=None):
    if args is None:
        args = sys.argv[1:]
    # Parse options
    parser = argparse.ArgumentParser(
                description="Trace file statistics analyzer",
                # Our -h, short for --load-histogram
                # conflicts with default for help, so we handle
                # manually.
                add_help=False)
    verbose_group = parser.add_mutually_exclusive_group()
    verbose_group.add_argument('--verbose', '-v',
                               default=False, action='store_true',
                               help="Be verbose; print each record")
    verbose_group.add_argument('--quiet', '-q',
                               default=False, action='store_true',
                               help="Reduce output; don't print summaries")
    parser.add_argument("--sizes", '-s',
                        default=False, action="store_true",
                        dest="print_size_histogram",
                        help="print histogram of object sizes")
    parser.add_argument("--no-stats", '-S',
                        default=True, action="store_false", dest="dostats",
                        help="don't print statistics")
    parser.add_argument("--load-histogram", "-h",
                        default=False, action="store_true",
                        dest="print_histogram",
                        help="print histogram of object load frequencies")
    parser.add_argument("--check", "-X",
                        default=False, action="store_true", dest="heuristic",
                        help=" enable heuristic checking for misaligned "
                             "records: oids > 2**32"
                             " will be rejected; this requires the tracefile "
                             "to be seekable")
    add_interval_argument(parser)
    add_tracefile_argument(parser)

    if '--help' in args:
        parser.print_help()
        sys.exit(2)

    options = parser.parse_args(args)

    f = options.tracefile

    rt0 = time.time()
    bycode = {}      # map code to count of occurrences
    byinterval = {}  # map code to count in current interval
    records = 0      # number of trace records read
    versions = 0     # number of trace records with versions
    datarecords = 0  # number of records with dlen set
    datasize = 0     # sum of dlen across records with dlen set
    oids = {}        # map oid to number of times it was loaded
    bysize = {}      # map data size to number of loads
    bysizew = {}     # map data size to number of writes
    total_loads = 0
    t0 = None        # first timestamp seen
    te = None        # most recent timestamp seen
    h0 = None        # timestamp at start of current interval
    he = None        # timestamp at end of current interval
    thisinterval = None  # generally te//interval
    f_read = f.read
    unpack = struct.unpack
    FMT = ">iiH8s8s"
    FMT_SIZE = struct.calcsize(FMT)
    assert FMT_SIZE == 26
    # Read file, gathering statistics, and printing each record if verbose.
    print(' '*16, "%7s %7s %7s %7s" % (
            'loads', 'hits', 'inv(h)', 'writes'), end=' ')
    print('hitrate')
    try:
        while 1:
            r = f_read(FMT_SIZE)
            if len(r) < FMT_SIZE:
                break
            ts, code, oidlen, start_tid, end_tid = unpack(FMT, r)
            if ts == 0:
                # Must be a misaligned record caused by a crash.
                if not options.quiet:
                    print("Skipping 8 bytes at offset", f.tell() - FMT_SIZE)
                    f.seek(f.tell() - FMT_SIZE + 8)
                continue
            oid = f_read(oidlen)
            if len(oid) < oidlen:
                break
            records += 1
            if t0 is None:
                t0 = ts
                thisinterval = t0 // options.interval
                h0 = he = ts
            te = ts
            if ts // options.interval != thisinterval:
                if not options.quiet:
                    dumpbyinterval(byinterval, h0, he)
                byinterval = {}
                thisinterval = ts // options.interval
                h0 = ts
            he = ts
            dlen, code = (code & 0x7fffff00) >> 8, code & 0xff
            if dlen:
                datarecords += 1
                datasize += dlen
            if code & 0x80:
                version = 'V'
                versions += 1
            else:
                version = '-'
            code &= 0x7e
            bycode[code] = bycode.get(code, 0) + 1
            byinterval[code] = byinterval.get(code, 0) + 1
            if dlen:
                if code & 0x70 == 0x20:  # All loads
                    bysize[dlen] = d = bysize.get(dlen) or {}
                    d[oid] = d.get(oid, 0) + 1
                elif code & 0x70 == 0x50:  # All stores
                    bysizew[dlen] = d = bysizew.get(dlen) or {}
                    d[oid] = d.get(oid, 0) + 1
            if options.verbose:
                print("%s %02x %s %016x %016x %c%s" % (
                    ctime(ts)[4:-5],
                    code,
                    oid_repr(oid),
                    U64(start_tid),
                    U64(end_tid),
                    version,
                    dlen and (' '+str(dlen)) or ""))
            if code & 0x70 == 0x20:
                oids[oid] = oids.get(oid, 0) + 1
                total_loads += 1
            elif code == 0x00:  # restart
                if not options.quiet:
                    dumpbyinterval(byinterval, h0, he)
                byinterval = {}
                thisinterval = ts // options.interval
                h0 = he = ts
                if not options.quiet:
                    print(ctime(ts)[4:-5], end=' ')
                    print('='*20, "Restart", '='*20)
    except KeyboardInterrupt:
        print("\nInterrupted.  Stats so far:\n")

    end_pos = f.tell()
    f.close()
    rte = time.time()
    if not options.quiet:
        dumpbyinterval(byinterval, h0, he)

    # Error if nothing was read
    if not records:
        print("No records processed", file=sys.stderr)
        return 1

    # Print statistics
    if options.dostats:
        print()
        print("Read {} trace records ({} bytes) in {:.1f} seconds".format(
            addcommas(records), addcommas(end_pos), rte-rt0))
        print("Versions:   %s records used a version" % addcommas(versions))
        print("First time: %s" % ctime(t0))
        print("Last time:  %s" % ctime(te))
        print("Duration:   %s seconds" % addcommas(te-t0))
        print("Data recs:  %s (%.1f%%), average size %d bytes" % (
            addcommas(datarecords),
            100.0 * datarecords / records,
            datasize / datarecords))
        print("Hit rate:   %.1f%% (load hits / loads)" % hitrate(bycode))
        print()
        codes = sorted(bycode.keys())
        print("%13s %4s %s" % ("Count", "Code", "Function (action)"))
        for code in codes:
            print("%13s  %02x  %s" % (
                addcommas(bycode.get(code, 0)),
                code,
                explain.get(code) or "*** unknown code ***"))

    # Print histogram.
    if options.print_histogram:
        print()
        print("Histogram of object load frequency")
        total = len(oids)
        print("Unique oids: %s" % addcommas(total))
        print("Total loads: %s" % addcommas(total_loads))
        s = addcommas(total)
        width = max(len(s), len("objects"))
        fmt = "%5d %" + str(width) + "s %5.1f%% %5.1f%% %5.1f%%"
        hdr = "%5s %" + str(width) + "s %6s %6s %6s"
        print(hdr % ("loads", "objects", "%obj", "%load", "%cum"))
        cum = 0.0
        for binsize, count in histogram(oids):
            obj_percent = 100.0 * count / total
            load_percent = 100.0 * count * binsize / total_loads
            cum += load_percent
            print(fmt % (binsize, addcommas(count),
                         obj_percent, load_percent, cum))

    # Print size histogram.
    if options.print_size_histogram:
        print()
        print("Histograms of object sizes")
        print()
        dumpbysize(bysizew, "written", "writes")
        dumpbysize(bysize, "loaded", "loads")


def dumpbysize(bysize, how, how2):
    print()
    print(f"Unique sizes {how}: {addcommas(len(bysize))}")
    print("%10s %6s %6s" % ("size", "objs", how2))
    sizes = sorted(bysize.keys())
    for size in sizes:
        loads = 0
        for n in bysize[size].values():
            loads += n
        print("%10s %6d %6d" % (addcommas(size),
                                len(bysize.get(size, "")),
                                loads))


def dumpbyinterval(byinterval, h0, he):
    loads = hits = invals = writes = 0
    for code in byinterval:
        if code & 0x20:
            n = byinterval[code]
            loads += n
            if code in (0x22, 0x26):
                hits += n
        elif code & 0x40:
            writes += byinterval[code]
        elif code & 0x10:
            if code != 0x10:
                invals += byinterval[code]

    if loads:
        hr = "%5.1f%%" % (100.0 * hits / loads)
    else:
        hr = 'n/a'

    print("%s-%s %7s %7s %7s %7s %7s" % (
        ctime(h0)[4:-8], ctime(he)[14:-8],
        loads, hits, invals, writes, hr))


def hitrate(bycode):
    loads = hits = 0
    for code in bycode:
        if code & 0x70 == 0x20:
            n = bycode[code]
            loads += n
            if code in (0x22, 0x26):
                hits += n
    if loads:
        return 100.0 * hits / loads
    else:
        return 0.0


def histogram(d):
    bins = {}
    for v in d.values():
        bins[v] = bins.get(v, 0) + 1
    L = sorted(bins.items())
    return L


def U64(s):
    return struct.unpack(">Q", s)[0]


def oid_repr(oid):
    if isinstance(oid, bytes) and len(oid) == 8:
        return '%16x' % U64(oid)
    else:
        return repr(oid)


def addcommas(n):
    sign, s = '', str(n)
    if s[0] == '-':
        sign, s = '-', s[1:]
    i = len(s) - 3
    while i > 0:
        s = s[:i] + ',' + s[i:]
        i -= 3
    return sign + s


explain = {
    # The first hex digit shows the operation, the second the outcome.
    # If the second digit is in "02468" then it is a 'miss'.
    # If it is in "ACE" then it is a 'hit'.

    0x00: "_setup_trace (initialization)",

    0x10: "invalidate (miss)",
    0x1A: "invalidate (hit, version)",
    0x1C: "invalidate (hit, saving non-current)",
    # 0x1E can occur during startup verification.
    0x1E: "invalidate (hit, discarding current or non-current)",

    0x20: "load (miss)",
    0x22: "load (hit)",
    0x24: "load (non-current, miss)",
    0x26: "load (non-current, hit)",

    0x50: "store (version)",
    0x52: "store (current, non-version)",
    0x54: "store (non-current)",
    }

if __name__ == "__main__":
    sys.exit(main())
