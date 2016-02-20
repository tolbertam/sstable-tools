package com.csforge.sstable;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;

public class SelectTests {
    static {
        Config.setClientMode(true);

        // Partitioner is not set in client mode.
        if (DatabaseDescriptor.getPartitioner() == null)
            DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    private static String CQL1 =
            "    CREATE TABLE composites (\n" +
            "        key1 varchar,\n" +
            "        key2 varchar,\n" +
            "        ckey1 varchar,\n" +
            "        ckey2 varchar,\n" +
            "        value bigint,\n" +
            "        PRIMARY KEY((key1, key2), ckey1, ckey2)\n" +
            "    );";

    private static String CQL2 =
            "    CREATE TABLE blog.users (\n" +
            "        user_name varchar PRIMARY KEY,\n" +
            "        password varchar,\n" +
            "        gender varchar,\n" +
            "        state varchar,\n" +
            "        birth_year bigint\n" +
            "    );";

    private static String CQL3 = "CREATE TABLE IF NOT EXISTS test.wide ( key text, key2 text, val text, PRIMARY KEY (key, key2));";
    private static String CQL4 = "CREATE TABLE collections (key1 varchar, listval list<text>, mapval map<text, text>, setval set<text>, PRIMARY KEY (key1))";
    @Test
    public void testSelectAll() throws Exception {
        File path = Utils.getDB(2);
        System.err.println(path);
        String query = String.format("SELECT * FROM \"%s\"", path);
        CFMetaData cfdata = CassandraUtils.tableFromCQL(new ByteArrayInputStream(CQL2.getBytes()));
        Query q = new Query(query, path, cfdata);
        UnfilteredPartitionIterator scanner = q.getScanner();
        Assert.assertTrue(scanner.hasNext());
        UnfilteredRowIterator partition = scanner.next();
        Unfiltered row = partition.next();
        Assert.assertEquals("Row:  | birth_year=1985, gender=male, password=pass@, state=CA", row.toString(cfdata));
        Assert.assertFalse(partition.hasNext());
    }

    private int getLength(String query, File path, CFMetaData cfdata) throws Exception {
        Query q = new Query(query, path, cfdata);
        UnfilteredPartitionIterator scanner = q.getScanner();
        AtomicInteger i = new AtomicInteger();
        scanner.forEachRemaining(partition -> {
            partition.forEachRemaining(row -> {
                //System.out.println("[" + cfdata.getKeyValidator().getString(partition.partitionKey().getKey()) + "] " + row.toString(cfdata, false));
                i.incrementAndGet();
            });
        });
        return i.get();
    }

    @Test
    public void testSelectLimit() throws Exception {
        File path = Utils.getDB(3);
        CFMetaData cfdata = CassandraUtils.tableFromCQL(new ByteArrayInputStream(CQL3.getBytes()));
        Assert.assertEquals(3, getLength(String.format("SELECT * FROM \"%s\" limit 3", path), path, cfdata));
        Assert.assertEquals(1, getLength(String.format("SELECT * FROM \"%s\" limit 1", path), path, cfdata));
        Assert.assertEquals(0, getLength(String.format("SELECT * FROM \"%s\" limit 0", path), path, cfdata));
        Assert.assertEquals(36, getLength(String.format("SELECT * FROM \"%s\" limit 500", path), path, cfdata));
    }

    @Test
    public void testSelectCount() throws Exception {
        File path = Utils.getDB(3);
        String query = String.format("SELECT count(*) FROM \"%s\"", path);
        CFMetaData cfdata = CassandraUtils.tableFromCQL(new ByteArrayInputStream(CQL3.getBytes()));
        Query q = new Query(query, path, cfdata);
        ResultSet result = q.getResults();
        Assert.assertEquals(1, result.rows.size());
        Assert.assertEquals("count", result.metadata.names.get(0).name.toString());
    }

    @Test
    public void testSelectCollections() throws Exception {
        File path = Utils.getDB(4);
        String query = String.format("SELECT * FROM \"%s\"", path);
        CFMetaData cfdata = CassandraUtils.tableFromCQL(new ByteArrayInputStream(CQL4.getBytes()));
        Query q = new Query(query, path, cfdata);
        ResultSet result = q.getResults();
        Assert.assertEquals(1, result.rows.size());
        TableTransformer.dumpResults(cfdata, result, System.out);

    }
}
