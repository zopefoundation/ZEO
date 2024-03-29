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

"""Test suite for ZEO.runzeo.ZEOOptions."""

import os
import tempfile
import unittest

import ZODB.config
from zdaemon.tests.testzdoptions import TestZDOptions

from ZEO.runzeo import ZEOOptions


# When a hostname isn't specified in a socket binding address, ZConfig
# supplies the empty string.
DEFAULT_BINDING_HOST = ""


class TestZEOOptions(TestZDOptions):

    OptionsClass = ZEOOptions

    input_args = ["-f", "Data.fs", "-a", "5555"]
    output_opts = [("-f", "Data.fs"), ("-a", "5555")]
    output_args = []

    configdata = """
        <zeo>
          address 5555
        </zeo>
        <filestorage fs>
          path Data.fs
        </filestorage>
        """

    def setUp(self):
        self.tempfilename = tempfile.mktemp()
        with open(self.tempfilename, "w") as f:
            f.write(self.configdata)

    def tearDown(self):
        try:
            os.remove(self.tempfilename)
        except OSError:
            pass

    def test_configure(self):
        # Hide the base class test_configure
        pass

    def test_default_help(self): pass  # disable silly test w spurious failures

    def test_defaults_with_schema(self):
        options = self.OptionsClass()
        options.realize(["-C", self.tempfilename])
        self.assertEqual(options.address, (DEFAULT_BINDING_HOST, 5555))
        self.assertEqual(len(options.storages), 1)
        opener = options.storages[0]
        self.assertEqual(opener.name, "fs")
        self.assertEqual(opener.__class__, ZODB.config.FileStorage)
        self.assertEqual(options.read_only, 0)
        self.assertEqual(options.transaction_timeout, None)
        self.assertEqual(options.invalidation_queue_size, 100)

    def test_defaults_without_schema(self):
        options = self.OptionsClass()
        options.realize(["-a", "5555", "-f", "Data.fs"])
        self.assertEqual(options.address, (DEFAULT_BINDING_HOST, 5555))
        self.assertEqual(len(options.storages), 1)
        opener = options.storages[0]
        self.assertEqual(opener.name, "1")
        self.assertEqual(opener.__class__, ZODB.config.FileStorage)
        self.assertEqual(opener.config.path, "Data.fs")
        self.assertEqual(options.read_only, 0)
        self.assertEqual(options.transaction_timeout, None)
        self.assertEqual(options.invalidation_queue_size, 100)

    def test_commandline_overrides(self):
        options = self.OptionsClass()
        options.realize(["-C", self.tempfilename,
                         "-a", "6666", "-f", "Wisdom.fs"])
        self.assertEqual(options.address, (DEFAULT_BINDING_HOST, 6666))
        self.assertEqual(len(options.storages), 1)
        opener = options.storages[0]
        self.assertEqual(opener.__class__, ZODB.config.FileStorage)
        self.assertEqual(opener.config.path, "Wisdom.fs")
        self.assertEqual(options.read_only, 0)
        self.assertEqual(options.transaction_timeout, None)
        self.assertEqual(options.invalidation_queue_size, 100)


del TestZDOptions  # don't run ZDaemon tests
