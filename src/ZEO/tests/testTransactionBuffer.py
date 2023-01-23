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
import random
import unittest

from ZEO.TransactionBuffer import TransactionBuffer


def random_string(size):
    """Return a random string of size size."""
    lst = [chr(random.randrange(256)) for i in range(size)]
    return "".join(lst)


def new_store_data():
    """Return arbitrary data to use as argument to store() method."""
    return random_string(8), random_string(random.randrange(1000))


def store(tbuf, resolved=False):
    data = new_store_data()
    tbuf.store(*data)
    if resolved:
        tbuf.server_resolve(data[0])
    return data


class TransBufTests(unittest.TestCase):

    def checkTypicalUsage(self):
        tbuf = TransactionBuffer(0)
        store(tbuf)
        store(tbuf)
        for o in tbuf:
            pass
        tbuf.close()

    def checkOrderPreserved(self):
        tbuf = TransactionBuffer(0)
        data = []
        for i in range(10):
            data.append((store(tbuf), False))
            data.append((store(tbuf, True), True))

        for i, (oid, d, resolved) in enumerate(tbuf):
            self.assertEqual((oid, d), data[i][0])
            self.assertEqual(resolved, data[i][1])
        tbuf.close()


def test_suite():
    test_loader = unittest.TestLoader()
    test_loader.testMethodPrefix = 'check'
    return test_loader.loadTestsFromTestCase(TransBufTests)
