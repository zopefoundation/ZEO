#!/usr/bin/env python2.3

"""Parse the BLATHER logging generated by ZEO, and optionally replay it.

Usage: zeointervals.py [options]

Options:

    --help / -h
        Print this message and exit.

    --replay=storage
    -r storage
        Replay the parsed transactions through the new storage

    --maxtxn=count
    -m count
        Parse no more than count transactions.

    --report / -p
        Print a report as we're parsing.

Unlike parsezeolog.py, this script generates timestamps for each transaction,
and sub-command in the transaction.  We can use this to compare timings with
synthesized data.
"""

import re
import sys
import time
import getopt
import operator
# ZEO logs measure wall-clock time so for consistency we need to do the same
# from time import clock as now
from time import time as now

from ZODB.FileStorage import FileStorage
# from BDBStorage.BDBFullStorage import BDBFullStorage
# from Standby.primary import PrimaryStorage
# from Standby.config import RS_PORT
from ZODB.Connection import TransactionMetaData
from ZODB.utils import p64
from functools import reduce

datecre = re.compile(r'(\d\d\d\d-\d\d-\d\d)T(\d\d:\d\d:\d\d)')
methcre = re.compile(r"ZEO Server (\w+)\((.*)\) \('(.*)', (\d+)")


class StopParsing(Exception):
    pass


def usage(code, msg=''):
    print(__doc__)
    if msg:
        print(msg)
    sys.exit(code)


def parse_time(line):
    """Return the time portion of a zLOG line in seconds or None."""
    mo = datecre.match(line)
    if mo is None:
        return None
    date, time_ = mo.group(1, 2)
    date_l = [int(elt) for elt in date.split('-')]
    time_l = [int(elt) for elt in time_.split(':')]
    return int(time.mktime(date_l + time_l + [0, 0, 0]))


def parse_line(line):
    """Parse a log entry and return time, method info, and client."""
    t = parse_time(line)
    if t is None:
        return None, None, None
    mo = methcre.search(line)
    if mo is None:
        return None, None, None
    meth_name = mo.group(1)
    meth_args = mo.group(2)
    meth_args = [s.strip() for s in meth_args.split(',')]
    m = meth_name, tuple(meth_args)
    c = mo.group(3), mo.group(4)
    return t, m, c


class StoreStat:
    def __init__(self, when, oid, size):
        self.when = when
        self.oid = oid
        self.size = size

    # Crufty
    def __getitem__(self, i):
        if i == 0:
            return self.oid
        if i == 1:
            return self.size
        raise IndexError


class TxnStat:
    def __init__(self):
        self._begintime = None
        self._finishtime = None
        self._aborttime = None
        self._url = None
        self._objects = []

    def tpc_begin(self, when, args, client):
        self._begintime = when
        # args are txnid, user, description (looks like it's always a url)
        self._url = args[2]

    def storea(self, when, args, client):
        oid = int(args[0])
        # args[1] is "[numbytes]"
        size = int(args[1][1:-1])
        s = StoreStat(when, oid, size)
        self._objects.append(s)

    def tpc_abort(self, when):
        self._aborttime = when

    def tpc_finish(self, when):
        self._finishtime = when


# Mapping oid -> revid
_revids = {}


class ReplayTxn(TxnStat):
    def __init__(self, storage):
        self._storage = storage
        self._replaydelta = 0
        TxnStat.__init__(self)

    def replay(self):
        ZERO = '\0'*8
        t0 = now()
        t = TransactionMetaData()
        self._storage.tpc_begin(t)
        for obj in self._objects:
            oid = obj.oid
            revid = _revids.get(oid, ZERO)
            # BAW: simulate a pickle of the given size
            data = 'x' * obj.size
            # BAW: ignore versions for now
            newrevid = self._storage.store(p64(oid), revid, data, '', t)
            _revids[oid] = newrevid
        if self._aborttime:
            self._storage.tpc_abort(t)
            origdelta = self._aborttime - self._begintime
        else:
            self._storage.tpc_vote(t)
            self._storage.tpc_finish(t)
            origdelta = self._finishtime - self._begintime
        t1 = now()
        # Shows how many seconds behind (positive) or ahead (negative) of the
        # original reply our local update took
        self._replaydelta = t1 - t0 - origdelta


class ZEOParser:
    def __init__(self, maxtxns=-1, report=1, storage=None):
        self.__txns = []
        self.__curtxn = {}
        self.__skipped = 0
        self.__maxtxns = maxtxns
        self.__finishedtxns = 0
        self.__report = report
        self.__storage = storage

    def parse(self, line):
        t, m, c = parse_line(line)
        if t is None:
            # Skip this line
            return
        name = m[0]
        meth = getattr(self, name, None)
        if meth is not None:
            meth(t, m[1], c)

    def tpc_begin(self, when, args, client):
        txn = ReplayTxn(self.__storage)
        self.__curtxn[client] = txn
        meth = getattr(txn, 'tpc_begin', None)
        if meth is not None:
            meth(when, args, client)

    def storea(self, when, args, client):
        txn = self.__curtxn.get(client)
        if txn is None:
            self.__skipped += 1
            return
        meth = getattr(txn, 'storea', None)
        if meth is not None:
            meth(when, args, client)

    def tpc_finish(self, when, args, client):
        txn = self.__curtxn.get(client)
        if txn is None:
            self.__skipped += 1
            return
        meth = getattr(txn, 'tpc_finish', None)
        if meth is not None:
            meth(when)
        if self.__report:
            self.report(txn)
        self.__txns.append(txn)
        self.__curtxn[client] = None
        self.__finishedtxns += 1
        if self.__maxtxns > 0 and self.__finishedtxns >= self.__maxtxns:
            raise StopParsing

    def report(self, txn):
        """Print a report about the transaction"""
        if txn._objects:
            bytes = reduce(operator.add, [size for oid, size in txn._objects])
        else:
            bytes = 0
        print('%s %s %4d %10d %s %s' % (
            txn._begintime, txn._finishtime - txn._begintime,
            len(txn._objects),
            bytes,
            time.ctime(txn._begintime),
            txn._url))

    def replay(self):
        for txn in self.__txns:
            txn.replay()
        # How many fell behind?
        slower = []
        faster = []
        for txn in self.__txns:
            if txn._replaydelta > 0:
                slower.append(txn)
            else:
                faster.append(txn)
        print(len(slower), 'laggards,', len(faster), 'on-time or faster')
        # Find some averages
        if slower:
            sum = reduce(operator.add,
                         [txn._replaydelta for txn in slower], 0)
            print('average slower txn was:', float(sum) / len(slower))
        if faster:
            sum = reduce(operator.add,
                         [txn._replaydelta for txn in faster], 0)
            print('average faster txn was:', float(sum) / len(faster))


def main():
    try:
        opts, args = getopt.getopt(
            sys.argv[1:],
            'hr:pm:',
            ['help', 'replay=', 'report', 'maxtxns='])
    except getopt.error as e:
        usage(1, e)

    if args:
        usage(1)

    replay = 0
    maxtxns = -1
    report = 0
    storagefile = None
    for opt, arg in opts:
        if opt in ('-h', '--help'):
            usage(0)
        elif opt in ('-r', '--replay'):
            replay = 1
            storagefile = arg
        elif opt in ('-p', '--report'):
            report = 1
        elif opt in ('-m', '--maxtxns'):
            try:
                maxtxns = int(arg)
            except ValueError:
                usage(1, 'Bad -m argument: %s' % arg)

    if replay:
        storage = FileStorage(storagefile)
        # storage = BDBFullStorage(storagefile)
        # storage = PrimaryStorage('yyz', storage, RS_PORT)
    t0 = now()
    p = ZEOParser(maxtxns, report, storage)
    i = 0
    while 1:
        line = sys.stdin.readline()
        if not line:
            break
        i += 1
        try:
            p.parse(line)
        except StopParsing:
            break
        except:  # NOQA: E722 bare except
            print('input file line:', i)
            raise
    t1 = now()
    print('total parse time:', t1-t0)
    t2 = now()
    if replay:
        p.replay()
    t3 = now()
    print('total replay time:', t3-t2)
    print('total time:', t3-t0)


if __name__ == '__main__':
    main()
