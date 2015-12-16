import os
import unittest

from sstable2json_tests import *

if __name__ == "__main__":
    print "Integration test suite for Cassandra %s" % os.environ['CASSANDRA_VERSION']
    suite = unittest.TestLoader().loadTestsFromTestCase(TestKeysOnly)
    suite = unittest.TestLoader().loadTestsFromTestCase(ToJson)
    unittest.TextTestRunner(verbosity=2).run(suite)
