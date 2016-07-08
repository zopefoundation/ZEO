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
version = '5.0.0a0'

from setuptools import setup, find_packages
import os
import sys

if sys.version_info < (2, 7):
    print("This version of ZEO requires Python 2.7 or higher")
    sys.exit(0)

if (3, 0) < sys.version_info < (3, 4):
    print("This version of ZEO requires Python 3.4 or higher")
    sys.exit(0)

install_requires = [
    'ZODB >= 5.0.0a5',
    'six',
    'transaction >= 1.6.0',
    'persistent >= 4.1.0',
    'zc.lockfile',
    'ZConfig',
    'zdaemon',
    'zope.interface',
    ]

tests_require = ['zope.testing', 'manuel', 'random2', 'mock']

if sys.version_info[:2] < (3, ):
    install_requires.extend(('futures', 'trollius'))

classifiers = """
Intended Audience :: Developers
License :: OSI Approved :: Zope Public License
Programming Language :: Python :: 2
Programming Language :: Python :: 2.7
Programming Language :: Python :: 3
Programming Language :: Python :: 3.4
Programming Language :: Python :: 3.5
Programming Language :: Python :: Implementation :: CPython
Programming Language :: Python :: Implementation :: PyPy
Topic :: Database
Topic :: Software Development :: Libraries :: Python Modules
Operating System :: Microsoft :: Windows
Operating System :: Unix
Framework :: ZODB
""".strip().split('\n')

def _modname(path, base, name=''):
    if path == base:
        return name
    dirname, basename = os.path.split(path)
    return _modname(dirname, base, basename + '.' + name)

def _flatten(suite, predicate=lambda *x: True):
    from unittest import TestCase
    for suite_or_case in suite:
        if predicate(suite_or_case):
            if isinstance(suite_or_case, TestCase):
                yield suite_or_case
            else:
                for x in _flatten(suite_or_case):
                    yield x

def _no_layer(suite_or_case):
    return getattr(suite_or_case, 'layer', None) is None

def _unittests_only(suite, mod_suite):
    for case in _flatten(mod_suite, _no_layer):
        suite.addTest(case)

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
                    _unittests_only(suite, mod.test_suite())
    return suite

long_description = (
    open('README.rst').read()
    + '\n' +
    open('CHANGES.rst').read()
    )
setup(name="ZEO",
      version=version,
      description = long_description.split('\n', 2)[1],
      long_description = long_description,
      url = 'https://pypi.python.org/pypi/ZEO',
      maintainer="Zope Foundation and Contributors",
      maintainer_email="zodb-dev@zope.org",
      packages = find_packages('src'),
      package_dir = {'': 'src'},
      license = "ZPL 2.1",
      platforms = ["any"],
      classifiers = classifiers,
      test_suite="__main__.alltests", # to support "setup.py test"
      tests_require = tests_require,
      extras_require = dict(test=tests_require),
      install_requires = install_requires,
      zip_safe = False,
      entry_points = """
      [console_scripts]
      zeopack = ZEO.scripts.zeopack:main
      runzeo = ZEO.runzeo:main
      zeopasswd = ZEO.zeopasswd:main
      zeoctl = ZEO.zeoctl:main
      zeo-nagios = ZEO.nagios:main
      """,
      include_package_data = True,
      )
