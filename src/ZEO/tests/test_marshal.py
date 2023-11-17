import unittest

from ZEO.asyncio.marshal import encode
from ZEO.asyncio.marshal import pickle_server_decode


try:
    from ZopeUndo.Prefix import Prefix
except ImportError:
    _HAVE_ZOPE_UNDO = False
else:
    _HAVE_ZOPE_UNDO = True


class MarshalTests(unittest.TestCase):

    @unittest.skipUnless(_HAVE_ZOPE_UNDO, 'ZopeUndo is not installed')
    def testServerDecodeZopeUndoFilter(self):
        # this is an example (1) of Zope2's arguments for
        # undoInfo call. Arguments are encoded by ZEO client
        # and decoded by server. The operation must be idempotent.
        # (1) https://github.com/zopefoundation/Zope/blob/2.13/src/App/Undo.py#L111  # NOQA: E501 line too long
        args = (0, 20, {'user_name': Prefix('test')})
        # test against repr because Prefix __eq__ operator
        # doesn't compare Prefix with Prefix but only
        # Prefix with strings. see Prefix.__doc__
        self.assertEqual(
            repr(pickle_server_decode(encode(*args))),
            repr(args)
        )
