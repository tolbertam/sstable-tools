import os
import tempfile
import shutil
import time
import glob

from unittest import TestCase

from ccmlib.cluster import Cluster
import ccmlib.node as nodelib

from cassandra.cluster import Cluster as PyCluster
from cassandra.cluster import NoHostAvailable
from cassandra.policies import WhiteListRoundRobinPolicy, RetryPolicy
from cassandra import ConsistencyLevel
from subprocess import *

def sh(args):
    print "Executing: %s" % " ".join(args)
    return Popen(args, stdout=PIPE).communicate()[0]

class IntegrationTest(TestCase):

    def setUp(self):
        self.test_path = tempfile.mkdtemp(prefix=self.name)
        print
        print "CCM using %s" % self.test_path

        # ccm setup
        self.session = None
        shutil.rmtree(self.test_path)
        os.makedirs(self.test_path)
        self.cluster = Cluster(self.test_path, "test", cassandra_version="3.0.0")

        # sstable tools setup
        self.uberjar_location = glob.glob("%s/../../../target/sstable-*.jar" % os.path.dirname(os.path.realpath(__file__)))[0]
        print "Using sstable build located at: %s" % self.uberjar_location


    def cql_connection(self, node, keyspace=None, timeout=60):
        if self.session:
            return self.session

        deadline = time.time() + timeout
        while True:
            try:
                self.connection_cluster = PyCluster([node.network_interfaces['binary'][0]],
                                                    port=node.network_interfaces['binary'][1])
                self.session = self.connection_cluster.connect()
                if keyspace:
                    self.session.execute(("CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = " +
                                         "{ 'class' : 'SimpleStrategy', 'replication_factor' : 1 };") % keyspace)
                    self.session.execute("USE %s" % keyspace)

                return self.session
            except NoHostAvailable:
                if time.time() > deadline:
                    raise
                else:
                    time.sleep(0.25)

    def tearDown(self):
        self.cluster.stop()

