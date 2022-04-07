##############################################################################
#
# Copyright (c) 2004 Zope Foundation and Contributors.
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
from __future__ import print_function
import doctest
import re
import unittest

from zope.testing import renormalizing


def test_suite():
    return unittest.TestSuite((
        doctest.DocFileSuite(
            'zeopack.test',
            checker=renormalizing.RENormalizing([
                (re.compile('usage: Usage: '), 'Usage: '),  # Py 2.4
                (re.compile('options:'), 'Options:'),  # Py 2.4
                ]),
            globs={'print_function': print_function},
            ),
        ))
