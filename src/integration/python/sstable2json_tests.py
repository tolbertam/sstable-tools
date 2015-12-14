
import json
from common import *
from tempfile import NamedTemporaryFile

CREATE_USER_TABLE = """
    CREATE TABLE users (
        user_name varchar PRIMARY KEY,
        password varchar,
        gender varchar,
        state varchar,
        birth_year bigint
    );
"""


class TestKeysOnly(IntegrationTest):
    def __init__(self, methodName='runTest'):
        TestCase.__init__(self, methodName)
        self.name = 'TestKeysOnly'

    def test_single_key(self):
        self.cluster.populate(1).start()
        [node1] = self.cluster.nodelist()
        session = self.cql_connection(node1, "test")

        session.execute(CREATE_USER_TABLE)
        session.execute("INSERT INTO users (user_name, password, gender, state, birth_year) VALUES('frodo', 'pass@', 'male', 'CA', 1985);")

        node1.flush()
        node1.compact()
        sstable = node1.get_sstables("test", "users")[0]

        tempf = NamedTemporaryFile(delete=False)
        tempf.write(CREATE_USER_TABLE)
        tempf.flush()
        output = sh(["java", "-jar", self.uberjar_location, "toJson", sstable, "-c", tempf.name, "-e"])
        print output
        self.assertEqual(json.loads(output), [[{"name": "user_name", "value": "frodo"}]])

