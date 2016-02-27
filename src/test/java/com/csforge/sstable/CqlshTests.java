package com.csforge.sstable;


import junit.framework.Assert;
import org.apache.cassandra.config.CFMetaData;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;

public class CqlshTests {

    @Test
    public void testUse() throws Exception {
        Cqlsh sh = new Cqlsh();
        File path = Utils.getDB(3);
        CFMetaData cfdata = CassandraUtils.tableFromCQL(new ByteArrayInputStream(Utils.CQL3.getBytes()));
        sh.metadata = cfdata;
        sh.doUse("USE " + path.getAbsolutePath() + " \"ban ana\"");
        Assert.assertEquals(path.getAbsoluteFile(), sh.sstables.get(0));
        Assert.assertEquals(1, sh.sstables.size());
    }

    @Test
    public void testUseDirectory() throws Exception {
        Cqlsh sh = new Cqlsh();
        File path = Utils.getDB(3);
        CFMetaData cfdata = CassandraUtils.tableFromCQL(new ByteArrayInputStream(Utils.CQL3.getBytes()));
        sh.metadata = cfdata;
        sh.doUse("USE " + path.getParentFile().getAbsolutePath());
        Assert.assertTrue(sh.sstables.contains(path.getAbsoluteFile()));
    }
}
