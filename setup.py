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
version = '5.2.3'

from setuptools import setup, find_packages
import os

install_requires = [
    'ZODB >= 5.1.1',
    'six',
    'transaction >= 2.0.3',
    'persistent >= 4.1.0',
    'zc.lockfile',
    'ZConfig',
    'zdaemon',
    'zope.interface',
]

tests_require = [
    # We rely on implementation details of
    # test mocks. See https://github.com/zopefoundation/ZODB/pull/222
    'ZODB >= 5.5.1',
    'zope.testing',
    'manuel',
    'random2',
    'mock',
    'msgpack < 1',
    'zope.testrunner',
]

classifiers = """
Intended Audience :: Developers
License :: OSI Approved :: Zope Public License
Programming Language :: Python :: 2
Programming Language :: Python :: 2.7
Programming Language :: Python :: 3
Programming Language :: Python :: 3.5
Programming Language :: Python :: 3.6
Programming Language :: Python :: 3.7
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
      description=long_description.split('\n', 2)[1],
      long_description=long_description,
      url='https://github.com/zopefoundation/ZEO',
      author="Zope Foundation and Contributors",
      author_email="zodb@googlegroups.com",
      keywords=['database', 'zodb'],
      packages=find_packages('src'),
      package_dir={'': 'src'},
      license="ZPL 2.1",
      platforms=["any"],
      classifiers=classifiers,
      test_suite="__main__.alltests", # to support "setup.py test"
      tests_require=tests_require,
      extras_require={
          'test': tests_require,
          'uvloop': [
              'uvloop >=0.5.1'
          ],
          'msgpack': [
              'msgpack-python'
          ],
          ':python_version == "2.7"': [
              'futures',
              'trollius',
          ],
          'docs': [
              'Sphinx',
              'repoze.sphinx.autointerface',
              'sphinx_rtd_theme',
          ],
      },
      install_requires=install_requires,
      zip_safe=False,
      entry_points="""
      [console_scripts]
      zeopack = ZEO.scripts.zeopack:main
      runzeo = ZEO.runzeo:main
      zeoctl = ZEO.zeoctl:main
      zeo-nagios = ZEO.nagios:main
      """,
      include_package_data=True,
      python_requires='>=2.7.9,!=3.0.*,!=3.1.*,!=3.2.*,!=3.3.*,!=3.4.*',
)
