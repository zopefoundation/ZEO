##############################################################################
#
# Copyright (c) 2003 Zope Foundation and Contributors.
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

import doctest
import tempfile
import unittest

import ZODB.config
import ZODB.tests.util
from ZODB.tests.testConfig import ConfigTestBase

class ZEOConfigTest(ConfigTestBase):

    def test_zeo_config(self):
        # We're looking for a port that doesn't exist so a
        # connection attempt will fail.  Instead of elaborate
        # logic to loop over a port calculation, we'll just pick a
        # simple "random", likely to not-exist port number and add
        # an elaborate comment explaining this instead.  Go ahead,
        # grep for 9.
        from ZEO.ClientStorage import ClientDisconnected
        import ZConfig
        from ZODB.config import getDbSchema
        from StringIO import StringIO
        cfg = """
        <zodb>
          <zeoclient>
            server localhost:56897
            wait false
          </zeoclient>
        </zodb>
        """
        config, handle = ZConfig.loadConfigFile(getDbSchema(), StringIO(cfg))
        self.assertEqual(config.database[0].config.storage.config.blob_dir,
                         None)
        self.assertRaises(ClientDisconnected, self._test, cfg)

        cfg = """
        <zodb>
          <zeoclient>
            blob-dir blobs
            server localhost:56897
            wait false
          </zeoclient>
        </zodb>
        """
        config, handle = ZConfig.loadConfigFile(getDbSchema(), StringIO(cfg))
        self.assertEqual(config.database[0].config.storage.config.blob_dir,
                         'blobs')
        self.assertRaises(ClientDisconnected, self._test, cfg)

def test_suite():
    return unittest.makeSuite(ZEOConfigTest)
