
import unittest

def suite():

    from datastoreutils.tests import record_mapper

    suite = unittest.TestSuite()
    suite.addTest(record_mapper.suite())
    return suite

if __name__ == '__main__':
    unittest.main(defaultTest='suite')
