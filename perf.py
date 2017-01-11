import argparse
import logging
import multiprocessing
import os
import persistent
import re
import time

import ZODB
from BTrees.IOBTree import BTree

import ZEO

#logging.basicConfig(level='DEBUG')

class P(persistent.Persistent):

    children = ()


parser = argparse.ArgumentParser("zodbshootout-inspired performance exercise")

parser.add_argument('--concurrency', '-c', type=int, default=1)
parser.add_argument('--client-hosts', '-C', type=int, default=1)
parser.add_argument('--object-size', '-o', type=int, default=999)
parser.add_argument('--transaction-size', '-t', type=int, default=3)
parser.add_argument('--repetitions', '-r', type=int, default=1000)
parser.add_argument('--demo-storage-baseline', '-d', action='store_true')
parser.add_argument('--address', '-a')
parser.add_argument('--ssl', action='store_true')
parser.add_argument('--server-sync', action='store_true')
parser.add_argument('--profile')
parser.add_argument('--save', '-s', help='save results to file')
parser.add_argument('--name', '-n', help='Run name')
parser.add_argument('--read-only', '-W', action='store_true')

def shoot():
    """zodbshootout-inspired performance exercise.
    """

    options = parser.parse_args()
    concurrency = options.concurrency
    object_size = options.object_size
    transaction_size = options.transaction_size
    repetitions = options.repetitions

    if options.ssl:
        from ZEO.tests.testssl import server_config, client_ssl
    else:
        server_config = None
        client_ssl = lambda : None

    if options.demo_storage_baseline:
        db_factory = None
        headings = ('add', 'update', 'read')
        stop = lambda : None
    else:
        if options.address:
            addr = options.address
            if ':' in addr:
                host, port = addr.split(':')
                addr = host, int(port)
            else:
                addr = '127.0.0.1', int(addr)
            stop = lambda : None
        else:
            if os.path.exists('perf.fs'):
                os.remove('perf.fs')

            try:
                addr, stop = ZEO.server(
                    threaded=False, path='perf.fs', zeo_conf=server_config)
            except TypeError:
                # ZEO 4
                addr, stop = ZEO.server()

        db_factory = lambda : ZEO.DB(
            addr,
            ssl=client_ssl(),
            wait_timeout=9999,
            server_sync=options.server_sync)

        if options.read_only:
            headings = ('read', 'prefetch')
        else:
            headings = ('add', 'update', 'cached', 'read', 'prefetch')
            # Initialize database
            db = db_factory()
            with db.transaction() as conn:
                conn.root.speedtest = speedtest = BTree()
                for ic in range(concurrency):
                    speedtest[ic] = data = BTree()
                    for ir in range(repetitions):
                        data[ir] = P()

            db.pack()
            db.close()


    print('Times per operation in microseconds (o=%s, t=%s, c=%s)' % (
        object_size, transaction_size, concurrency))
    print(' %12s' * 5 % ('op', 'min', 'mean', 'median', 'max'))

    queues = [(multiprocessing.Queue(), multiprocessing.Queue())
              for ip in range(concurrency)]

    if options.save:
        save_file = open(options.save, 'a')
    else:
        save_file = None

    if concurrency > 1 or save_file:
        processes = [
            multiprocessing.Process(
                target=run_test,
                args=(db_factory, ip, queues[ip][0], queues[ip][1],
                      object_size, transaction_size, repetitions,
                      options.read_only),
                )
            for ip in range(concurrency)
            ]

        for p in processes:
            p.daemon = True
            p.start()

        for iqueue, oqueue in queues:
            oqueue.get(timeout=9) # ready?

        for name in headings:
            for iqueue, oqueue in queues:
                iqueue.put(None)
            data = [oqueue.get(timeout=999) / repetitions
                    for _, oqueue in queues]
            summarize(name, data)
            if save_file:
                save_file.write('\t'.join(
                    map(str,
                        (options.name,
                         object_size, transaction_size,
                         concurrency * options.client_hosts, repetitions,
                         options.server_sync, options.ssl,
                         options.demo_storage_baseline,
                         options.address,
                         name, sum(data)/len(data))
                        )
                    ) + '\n')

        for p in processes:
            p.join(1)

    else:
        [(iqueue, oqueue)] = queues
        for name in headings:
            iqueue.put(None)

        if options.profile:
            import cProfile
            profiler = cProfile.Profile()
            profiler.enable()
        else:
            profiler = None
        run_test(db_factory, 0, iqueue, oqueue,
                 object_size, transaction_size, repetitions,
                 options.read_only)
        oqueue.get(timeout=9) # ready?
        if profiler is not None:
            profiler.disable()
            profiler.dump_stats(options.profile)

        for name in headings:
            summarize(name, [oqueue.get(timeout=999) / repetitions])

    stop()

def summarize(name, results):
    results = [int(t*1000000) for t in results]
    l = len(results)
    if l > 1:
        print(' %12s' * 5 % (
            name, min(results), round(sum(results)/l), results[l//2],
            max(results)
            ))
    else:
        print(' %12s' * 2 % (name, results[0]))

def run_test(db_factory, process_index, iqueue, oqueue,
             object_size, transaction_size, repetitions, read_only):

    if db_factory is None:
        # No factory. Compute baseline numbers using DemoStorage
        db = ZODB.DB(None)
        with db.transaction() as conn:
            conn.root.speedtest = speedtest = BTree()
            speedtest[process_index] = data = BTree()
            for ir in range(repetitions):
                data[ir] = P()
    else:
        db = db_factory()

    conn = db.open()
    data = conn.root.speedtest[process_index]
    slots = list(data.values()) # Get data slots loaded
    oqueue.put('ready')

    if not read_only:
        # add test
        iqueue.get()
        start = time.time()
        for slot in slots:
            conn.transaction_manager.begin()
            for it in range(transaction_size):
                p = P()
                p.data = 'x' * object_size
                slot.children += (p, )
            conn.transaction_manager.commit()

        conn.transaction_manager.commit()
        oqueue.put(time.time() - start)

        iqueue.get()
        # update test
        start = time.time()
        for slot in slots:
            conn.transaction_manager.begin()
            for it in range(transaction_size):
                slot.children[it].data = 'y' * object_size
            conn.transaction_manager.commit()
        oqueue.put(time.time() - start)

        iqueue.get()

        # hot, With db cache, but not object cache
        conn.cacheMinimize()
        for slot in slots:
            _ = slot.children
        start = time.time()
        for slot in slots:
            conn.transaction_manager.begin()
            for it in range(transaction_size):
                assert slot.children[it].data == 'y' * object_size
            conn.transaction_manager.commit()
        oqueue.put(time.time() - start)

    if db_factory is None:
        return

    storage = db.storage

    # cold, With no cache
    storage._cache.clear()
    conn.cacheMinimize()
    for slot in slots[:repetitions]:
        _ = slot.children

    iqueue.get()
    start = time.time()
    for slot in slots[:repetitions]:
        conn.transaction_manager.begin()
        for it in range(transaction_size):
            assert slot.children[it].data == 'y' * object_size
        conn.transaction_manager.commit()
    oqueue.put(time.time() - start)

    # cold, With no cache, but with prefetch
    oids = []
    for slot in slots[:repetitions]:
        soids = []
        oids.append(soids)
        for it in range(transaction_size):
            soids.append(slot.children[it]._p_oid)

    storage._cache.clear()
    conn.cacheMinimize()
    for slot in slots[:repetitions]:
        _ = slot.children

    iqueue.get()
    start = time.time()
    for slot, soids in zip(slots[:repetitions], oids):
        conn.transaction_manager.begin()
        storage.prefetch(soids, conn._storage._start)
        for it in range(transaction_size):
            assert slot.children[it].data == 'y' * object_size
        conn.transaction_manager.commit()
    oqueue.put(time.time() - start)

    conn.close()
    db.close()

if __name__ == '__main__':
    shoot()
