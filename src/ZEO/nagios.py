##############################################################################
#
# Copyright (c) 2011 Zope Foundation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################
"""%prog [options] address

Where the address is an IPV6 address of the form: [addr]:port, an IPV4
address of the form: addr:port, or the name of a unix-domain socket file.
"""
import json
import optparse
import os
import re
import socket
import struct
import sys
import time


NO_TRANSACTION = '0'*16

nodiff_names = 'active_txns connections waiting'.split()
diff_names = 'aborts commits conflicts conflicts_resolved loads stores'.split()

per_times = dict(seconds=1.0, minutes=60.0, hours=3600.0, days=86400.0)


def new_metric(metrics, storage_id, name, value):
    if storage_id == '1':
        label = name
    else:
        if ' ' in storage_id:
            label = f"'{storage_id}:{name}'"
        else:
            label = f'{storage_id}:{name}'
    metrics.append(f'{label}={value}')


def result(messages, metrics=(), status=None):
    if metrics:
        messages[0] += '|' + metrics[0]
    if len(metrics) > 1:
        messages.append('| ' + '\n '.join(metrics[1:]))
    print('\n'.join(messages))
    return status


def error(message):
    return result((message, ), (), 2)


def warn(message):
    return result((message, ), (), 1)


def check(addr, output_metrics, status, per):
    m = re.match(r'\[(\S+)\]:(\d+)$', addr)
    if m:
        addr = m.group(1), int(m.group(2))
        s = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
    else:
        m = re.match(r'(\S+):(\d+)$', addr)
        if m:
            addr = m.group(1), int(m.group(2))
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        else:
            s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        s.connect(addr)
    except OSError as err:
        s.close()
        return error("Can't connect %s" % err)

    s.sendall(b'\x00\x00\x00\x04ruok')
    proto = s.recv(struct.unpack(">I", s.recv(4))[0])  # NOQA: F841 unused
    datas = s.recv(struct.unpack(">I", s.recv(4))[0])
    s.close()
    data = json.loads(datas.decode("ascii"))
    if not data:
        return warn("No storages")

    metrics = []
    messages = []
    level = 0
    if output_metrics:
        for storage_id, sdata in sorted(data.items()):
            for name in nodiff_names:
                new_metric(metrics, storage_id, name, sdata[name])

        if status:
            now = time.time()
            if os.path.exists(status):
                dt = now - os.stat(status).st_mtime
                if dt > 0:  # sanity :)
                    with open(status) as f:  # Read previous
                        old = json.loads(f.read())
                    dt /= per_times[per]
                    for storage_id, sdata in sorted(data.items()):
                        sdata['sameple-time'] = now
                        if storage_id in old:
                            sold = old[storage_id]
                            for name in diff_names:
                                v = (sdata[name] - sold[name]) / dt
                                new_metric(metrics, storage_id, name, v)
            with open(status, 'w') as f:  # save current
                f.write(json.dumps(data))

    for storage_id, sdata in sorted(data.items()):
        if sdata['last-transaction'] == NO_TRANSACTION:
            messages.append("Empty storage %r" % storage_id)
            level = max(level, 1)
    if not messages:
        messages.append('OK')
    return result(messages, metrics, level or None)


def main(args=None):
    if args is None:
        args = sys.argv[1:]

    parser = optparse.OptionParser(__doc__)
    parser.add_option(
        '-m', '--output-metrics', action="store_true",
        help="Output metrics.",
        )
    parser.add_option(
        '-s', '--status-path',
        help="Path to status file, needed to get rate metrics",
        )
    parser.add_option(
        '-u', '--time-units', type='choice', default='minutes',
        choices=['seconds', 'minutes', 'hours', 'days'],
        help="Time unit for rate metrics",
        )
    (options, args) = parser.parse_args(args)
    [addr] = args
    return check(
        addr, options.output_metrics, options.status_path, options.time_units)


if __name__ == '__main__':
    main()
