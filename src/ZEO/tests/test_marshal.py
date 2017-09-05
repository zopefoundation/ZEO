import unittest
from ZEO.asyncio.marshal import encode
from ZEO.asyncio.marshal import pickle_server_decode
from ZopeUndo.Prefix import Prefix


class MarshalTests(unittest.TestCase):

    def testServerDecodeZopeUndoFilter(self):
        # this is an example of Zope's 
        args = (0, 20, {'user_name': Prefix('test')})
        # test against repr because Prefix __eq__ operator
        # doesn't compare Prefix with Prefix but only
        # Prefix with strings. see Prefix.__doc__
        self.assertEqual(
            repr(pickle_server_decode(encode(*args))),
            repr(args)
        )


def test_suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(MarshalTests))
    return suite

