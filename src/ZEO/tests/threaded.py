"""Test layer for threaded-server tests

uvloop currently has a bug,
https://github.com/MagicStack/uvloop/issues/39, that causes failure if
multiprocessing and threaded servers are mixed in the same
application, so we isolate the few threaded tests in their own layer.
"""
import ZODB.tests.util


threaded_server_tests = ZODB.tests.util.MininalTestLayer(
    'threaded_server_tests')
