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

from setuptools import find_packages
from setuptools import setup


version = '6.0.0'

install_requires = [
    'ZODB >= 5.1.1',
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
    'ZConfig',
    'ZODB >= 5.5.1',
    'ZopeUndo',
    'zope.testing',
    'transaction',
    'msgpack',
    'zdaemon',
    'zope.testrunner',
]


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
      classifiers=[
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Zope Public License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: Implementation :: CPython",
        "Programming Language :: Python :: Implementation :: PyPy",
        "Topic :: Database",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: Unix",
        "Framework :: ZODB",
      ],
      tests_require=tests_require,
      extras_require={
          'test': tests_require,
          'uvloop': [
              'uvloop >=0.5.1'
          ],
          'msgpack': [
              'msgpack-python'
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
      python_requires='>=3.7',
      )
