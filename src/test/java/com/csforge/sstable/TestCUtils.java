package com.csforge.sstable;


import junit.framework.Assert;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;

public class TestCUtils {
    static {
        Config.setClientMode(true);

        // Partitioner is not set in client mode.
        if (DatabaseDescriptor.getPartitioner() == null)
            DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    @Test
    public void testFromCQLWithoutKeyspace() throws Exception {
        CFMetaData cfm = CassandraUtils.tableFromCQL(new ByteArrayInputStream(
                ("   CREATE TABLE users (\n" +
                "        user_name varchar PRIMARY KEY,\n" +
                "        password varchar,\n" +
                "        gender varchar,\n" +
                "        state varchar,\n" +
                "        birth_year bigint\n" +
                "    );").getBytes()));
        Assert.assertEquals("users", cfm.cfName);
        Assert.assertEquals(5, cfm.allColumns().size());
    }

    @Test
    public void testFromCQLWithKeyspace() throws Exception {
        CFMetaData cfm = CassandraUtils.tableFromCQL(new ByteArrayInputStream(
                ("   CREATE TABLE blog.users (\n" +
                        "        user_name varchar PRIMARY KEY,\n" +
                        "        password varchar,\n" +
                        "        gender varchar,\n" +
                        "        state varchar,\n" +
                        "        birth_year bigint\n" +
                        "    );").getBytes()));
        Assert.assertEquals("users", cfm.cfName);
        Assert.assertEquals("blog", cfm.ksName);
        Assert.assertEquals(5, cfm.allColumns().size());
    }

    @Test
    public void testFromSSTable() throws Exception {
        File path = Utils.getSSTable("ma", 2);
        CFMetaData cfm = CassandraUtils.tableFromSSTable(path);
        Assert.assertEquals(5, cfm.allColumns().size());
    }
}
