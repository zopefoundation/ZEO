##############################################################################
#
# Copyright (c) 2022 Zope Foundation and Contributors.
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
"""Module racetest provides infrastructure for tests that catch concurrency
problems in ZEO.

It complements ZODB.tests.racetest .
"""

import time

import zope.interface


# LoadDelayedStorage wraps base storage in injects delays after load*
# operations.
#
# It is useful to catch concurrency problems due to races in between load and
# other storage events, e.g. invalidations.
#
# See e.g. https://github.com/zopefoundation/ZEO/issues/209 for example where
# injecting such delays was effective to catch bugs that were otherwise hard to
# reproduce.
class LoadDelayedStorage:

    def __init__(self, base, tdelay=0.01):
        self.base = base
        self.tdelay = tdelay
        zope.interface.directlyProvides(self, zope.interface.providedBy(base))

    def _delay(self):
        time.sleep(self.tdelay)

    def __getattr__(self, name):
        return getattr(self.base, name)

    def __len__(self):          # ^^^ __getattr__ does not forward __<attr>
        return len(self.base)   # because they are mangled

    def load(self, *argv, **kw):
        _ = self.base.load(*argv, **kw)
        self._delay()
        return _

    def loadBefore(self, *argv, **kw):
        _ = self.base.loadBefore(*argv, **kw)
        self._delay()
        return _

    def loadSerial(self, *argv, **kw):
        _ = self.base.loadSerial(*argv, **kw)
        self._delay()
        return _


class ZConfigLoadDelayed:

    _factory = LoadDelayedStorage

    def __init__(self, config):
        self.config = config
        self.name = config.getSectionName()

    def open(self):
        base = self.config.base.open()
        return self._factory(base)
