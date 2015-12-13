package com.csforge.sstable.reader;

import com.google.common.collect.Lists;
import junit.framework.Assert;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

@RunWith(PowerMockRunner.class)
@PrepareForTest({Descriptor.class, CFMetaData.class, SSTableReader.class})
public class CassandraReaderTest {

    public UnfilteredRowIterator makePartition(DecoratedKey key) {
        UnfilteredRowIterator partition = Mockito.mock(UnfilteredRowIterator.class);
        when(partition.partitionKey()).thenReturn(key);
        return partition;
    }

    @Test
    public void testreadSSTable() throws IOException {
        CassandraReader reader = new CassandraReader();
        reader.metadata = mock(CFMetaData.class);

        mockStatic(Descriptor.class);
        mockStatic(SSTableReader.class);
        Descriptor desc = Mockito.mock(Descriptor.class);
        when(Descriptor.fromFilename("test")).thenReturn(desc);
        SSTableReader mockReader = Mockito.mock(SSTableReader.class);
        ISSTableScanner rowIter = Mockito.mock(ISSTableScanner.class);
        when(SSTableReader.openNoValidation(desc,  reader.metadata)).thenReturn(mockReader);
        DecoratedKey k1 = Murmur3Partitioner.instance.decorateKey(ByteBuffer.wrap("1".getBytes()));
        DecoratedKey k2 = Murmur3Partitioner.instance.decorateKey(ByteBuffer.wrap("2".getBytes()));
        Iterator<UnfilteredRowIterator> uri = Lists.newArrayList(makePartition(k1), makePartition(k2)).iterator();
        ISSTableScanner u =  new ISSTableScanner() {
            public long getLengthInBytes() { return 0; }
            public long getCurrentPosition() { return 0; }
            public String getBackingFiles() { return null; }
            public boolean isForThrift() { return false; }
            public CFMetaData metadata() { return null; }
            public void close() { }
            public boolean hasNext() {
                return uri.hasNext();
            }
            public UnfilteredRowIterator next() {
                return uri.next();
            }
        };
        when(mockReader.getScanner()).thenReturn(u);

        List<Partition> results = reader.readSSTable("test", null).collect(Collectors.toList());
        Assert.assertEquals(results.get(0).getKey(), k1);
        Assert.assertEquals(results.get(1).getKey(), k2);
    }
}
