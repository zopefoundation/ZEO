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

# Some tests use ``unittest.mock``
# In Python 3.5 it lacks some functions -- patch
from unittest.mock import NonCallableMock

if not hasattr(NonCallableMock, "assert_called_once"):
    def assert_called_once(_mock_self):
        """assert that the mock was called only once.
        """
        self = _mock_self
        if not self.call_count == 1:
            msg = ("Expected '%s' to have been called once. Called %s times." %
                   (self._mock_name or 'mock', self.call_count))
            raise AssertionError(msg)

    NonCallableMock.assert_called_once = assert_called_once
    del assert_called_once

del NonCallableMock
