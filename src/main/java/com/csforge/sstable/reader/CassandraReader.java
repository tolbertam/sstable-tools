package com.csforge.sstable.reader;

import com.google.common.base.Preconditions;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.KeyIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class CassandraReader {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(CassandraReader.class);
    CFMetaData metadata = null;

    public CassandraReader(CFMetaData metadata) {
        this.metadata = metadata;
    }

    /**
     * return all the decorated keys within an sstable
     * @param sstablePath - path to sstable
     */
    public Stream<DecoratedKey> keys(File sstablePath) {
        Descriptor desc = Descriptor.fromFilename(sstablePath.getAbsolutePath());
        KeyIterator keyIter = new KeyIterator(desc, metadata);
        Spliterator<DecoratedKey> spliterator = Spliterators.spliteratorUnknownSize(keyIter, Spliterator.IMMUTABLE);
        return StreamSupport.stream(spliterator, false);
    }

    public Stream<Partition> readSSTable(File sstablePath, Stream<String> keys, Set<String> excludes)
            throws IOException {
        Descriptor desc = Descriptor.fromFilename(sstablePath.getAbsolutePath());
        SSTableReader sstable = SSTableReader.openNoValidation(desc, metadata);
        IPartitioner partitioner = sstable.getPartitioner();
        return keys.filter(key -> !excludes.contains(key))
                   .map(metadata.getKeyValidator()::fromString)
                   .map(partitioner::decorateKey)
                   .sorted()
                   .map(key -> sstable.iterator(key, ColumnFilter.all(metadata), false, false))
                   .map(Partition::new);
    }

    public Stream<Partition> readSSTable(File sstablePath, Set<String> excludes) throws IOException {
        Preconditions.checkNotNull(sstablePath);
        Descriptor desc = Descriptor.fromFilename(sstablePath.getAbsolutePath());
        SSTableReader sstable = SSTableReader.openNoValidation(desc, metadata);

        Spliterator<UnfilteredRowIterator> spliterator = Spliterators.spliteratorUnknownSize(
                sstable.getScanner(), Spliterator.IMMUTABLE);
        Stream<UnfilteredRowIterator> stream = StreamSupport.stream(spliterator, false);
        return stream.filter(i -> excludes == null ||
                excludes.isEmpty() ||
                !excludes.contains(metadata.getKeyValidator().getString(i.partitionKey().getKey())))
                     .map(Partition::new);
    }

    public CFMetaData getMetadata() {
        return metadata;
    }

}
