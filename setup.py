##############################################################################
#
# Copyright (c) 2002, 2003 Zope Foundation and Contributors.
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

VERSION = "4.0.0dev"

from ez_setup import use_setuptools
use_setuptools()

from setuptools import setup, find_packages
from setuptools.extension import Extension
import os
import sys

if sys.version_info < (2, 6):
    print "This version of ZEO requires Python 2.6 or higher"
    sys.exit(0)

classifiers = """\
Intended Audience :: Developers
License :: OSI Approved :: Zope Public License
Programming Language :: Python
Topic :: Database
Topic :: Software Development :: Libraries :: Python Modules
Operating System :: Microsoft :: Windows
Operating System :: Unix
Framework :: ZODB
"""

def alltests():
    import logging
    import pkg_resources
    import unittest
    import ZEO.ClientStorage

    class NullHandler(logging.Handler):
        level = 50

        def emit(self, record):
            pass

    logging.getLogger().addHandler(NullHandler())

    suite = unittest.TestSuite()
    base = pkg_resources.working_set.find(
        pkg_resources.Requirement.parse('ZEO')).location
    for dirpath, dirnames, filenames in os.walk(base):
        if os.path.basename(dirpath) == 'tests':
            for filename in filenames:
                if filename != 'testZEO.py': continue
                if filename.endswith('.py') and filename.startswith('test'):
                    mod = __import__(
                        _modname(dirpath, base, os.path.splitext(filename)[0]),
                        {}, {}, ['*'])
                    suite.addTest(mod.test_suite())
    return suite

tests_require = ['zope.testing', 'manuel']

setup(name="ZEO",
      version=VERSION,
      maintainer="Zope Foundation and Contributors",
      maintainer_email="zodb-dev@zope.org",
      packages = find_packages('src'),
      package_dir = {'': 'src'},
      license = "ZPL 2.1",
      platforms = ["any"],
      # description = doclines[0],
      classifiers = filter(None, classifiers.split("\n")),
      # long_description = long_description,
      test_suite="__main__.alltests", # to support "setup.py test"
      tests_require = tests_require,
      extras_require = dict(test=tests_require),
      install_requires = [
          'ZODB',
          'transaction',
          'persistent',
          'zc.lockfile',
          'ZConfig',
          'zdaemon',
          'zope.interface',
          ],
      zip_safe = False,
      entry_points = """
      [console_scripts]
      zeopack = ZEO.scripts.zeopack:main
      runzeo = ZEO.runzeo:main
      zeopasswd = ZEO.zeopasswd:main
      zeoctl = ZEO.zeoctl:main
      """,
      include_package_data = True,
      )
