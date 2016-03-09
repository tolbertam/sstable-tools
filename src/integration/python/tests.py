import os
import unittest

if __name__ == "__main__":
    print "Integration test suite for Cassandra %s" % os.environ['CASSANDRA_VERSION']
    suite = unittest.TestSuite()
    unittest.TextTestRunner(verbosity=2).run(suite)
