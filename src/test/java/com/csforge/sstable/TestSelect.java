package com.csforge.sstable;

import java.nio.ByteBuffer;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

public class TestSelect {
    static {
        DatabaseDescriptor.clientInitialization(false);

        // Partitioner is not set in client mode.
        if (DatabaseDescriptor.getPartitioner() == null)
            DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    @Test
    public void testSelectAll() throws Exception {
        File path = Utils.getSSTable("ma", 2);
        String query = String.format("SELECT * FROM \"%s\"", path);
        CFMetaData cfdata = CassandraUtils.tableFromCQL(new ByteArrayInputStream(Utils.CQL2.getBytes()));
        Query q = new Query(query, Collections.singleton(path), cfdata);
        try(UnfilteredPartitionIterator scanner = q.getScanner()) {
            Assert.assertTrue(scanner.hasNext());
            UnfilteredRowIterator partition = scanner.next();
            Unfiltered row = partition.next();
            Assert.assertEquals("Row:  | birth_year=1985, gender=male, password=pass@, state=CA", row.toString(cfdata));
            Assert.assertFalse(partition.hasNext());
        }
    }

    @Test
    public void testSelectAllMB() throws Exception {
        File path = Utils.getSSTable("mb", 1);
        String query = String.format("SELECT * FROM \"%s\"", path);
        CFMetaData cfdata = SystemKeyspace.Batches;
        Query q = new Query(query, Collections.singleton(path), cfdata);
        try(UnfilteredPartitionIterator scanner = q.getScanner()) {
            Assert.assertTrue(scanner.hasNext());
            UnfilteredRowIterator partition = scanner.next();
            Unfiltered row = partition.next();
            Assert.assertEquals("Row:  | ", row.toString(cfdata));
            Assert.assertFalse(partition.hasNext());
        }
    }

    private int getLength(String query, File path, CFMetaData cfdata) throws Exception {
        Query q = new Query(query, Collections.singleton(path), cfdata);
        AtomicInteger i = new AtomicInteger();
        try(UnfilteredPartitionIterator scanner = q.getScanner()) {
            scanner.forEachRemaining(partition -> {
                partition.forEachRemaining(row -> {
                    //System.out.println("[" + cfdata.getKeyValidator().getString(partition.partitionKey().getKey()) + "] " + row.toString(cfdata, false));
                    i.incrementAndGet();
                });
            });
        }
        return i.get();
    }

    @Test
    public void testSelectLimit() throws Exception {
        File path = Utils.getSSTable("ma", 3);
        CFMetaData cfdata = CassandraUtils.tableFromCQL(new ByteArrayInputStream(Utils.CQL3.getBytes()));
        Assert.assertEquals(3, getLength(String.format("SELECT * FROM \"%s\" limit 3", path), path, cfdata));
        Assert.assertEquals(1, getLength(String.format("SELECT * FROM \"%s\" limit 1", path), path, cfdata));
        Assert.assertEquals(0, getLength(String.format("SELECT * FROM \"%s\" limit 0", path), path, cfdata));
        Assert.assertEquals(36, getLength(String.format("SELECT * FROM \"%s\" limit 500", path), path, cfdata));
    }

    @Test
    public void testSelectCount() throws Exception {
        File path = Utils.getSSTable("ma", 3);
        String query = String.format("SELECT count(*) FROM \"%s\"", path);
        CFMetaData cfdata = CassandraUtils.tableFromCQL(new ByteArrayInputStream(Utils.CQL3.getBytes()));
        Query q = new Query(query, Collections.singleton(path), cfdata);
        ResultSet result = q.getResults().getResultSet();
        Assert.assertEquals(1, result.rows.size());
        Assert.assertEquals("count", result.metadata.names.get(0).name.toString());
        TableTransformer.dumpResults(cfdata, result, System.out);
    }

    @Test
    public void testSelectGroupBy() throws Exception {
        File path = Utils.getSSTable("mc", 1);
        String query = String.format("SELECT weatherstation_id, date, AVG(temperature) AS avg FROM \"%s\" GROUP BY weatherstation_id, date", path);
        CFMetaData cfdata = CassandraUtils.tableFromCQL(new ByteArrayInputStream(Utils.CQL5.getBytes()));
        Query q = new Query(query, Collections.singleton(path), cfdata);
        ResultSet result = q.getResults().getResultSet();
        Assert.assertEquals(2, result.rows.size());

        Assert.assertEquals("2016-04-03", TableTransformer.colValue(result, result.rows.get(0), 1));
        Assert.assertEquals("71.5", TableTransformer.colValue(result, result.rows.get(0), 2));
        Assert.assertEquals("2016-04-04", TableTransformer.colValue(result, result.rows.get(1), 1));
        Assert.assertEquals("73.5", TableTransformer.colValue(result, result.rows.get(1), 2));
        TableTransformer.dumpResults(cfdata, result, System.out);
    }

    @Test
    public void testSelectCompositeFromSSTable() throws Exception {
        File path = Utils.getSSTable("ma", 1);
        String query = String.format("SELECT * FROM \"%s\"", path);
        CFMetaData cfdatatrue = CassandraUtils.tableFromCQL(new ByteArrayInputStream(Utils.CQL1.getBytes()));
        CFMetaData cfdata = CassandraUtils.tableFromSSTable(Utils.getSSTable("ma", 1));
        Query q = new Query(query, Collections.singleton(path), cfdata);
        ResultSet result = q.getResults().getResultSet();
        Assert.assertEquals(2, result.rows.size());
        TableTransformer.dumpResults(cfdata, result, System.out);
    }

    @Test
    public void testSelectCollections() throws Exception {
        File path = Utils.getSSTable("ma", 4);
        String query = String.format("SELECT * FROM \"%s\"", path);
        CFMetaData cfdata = CassandraUtils.tableFromCQL(new ByteArrayInputStream(Utils.CQL4.getBytes()));
        Query q = new Query(query, Collections.singleton(path), cfdata);
        ResultSet result = q.getResults().getResultSet();
        Assert.assertEquals(1, result.rows.size());
        TableTransformer.dumpResults(cfdata, result, System.out);
    }
}
