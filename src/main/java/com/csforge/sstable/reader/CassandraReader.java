package com.csforge.sstable.reader;

import com.google.common.annotations.VisibleForTesting;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.CFStatement;
import org.apache.cassandra.cql3.statements.CreateTableStatement;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.KeyIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.*;

import java.io.IOException;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class CassandraReader {

    CFMetaData metadata = null;

    @VisibleForTesting
    CassandraReader() {}

    public CassandraReader(String cql) {
        Schema.instance.setKeyspaceMetadata(KeyspaceMetadata.create("turtle", KeyspaceParams.local(), Tables.none(),
                Views.none(), Types.none(), Functions.none()));
        CFStatement statement = (CFStatement) QueryProcessor.parseStatement(cql);
        statement.prepareKeyspace("turtle");
        metadata = ((CreateTableStatement) statement.prepare().statement).getCFMetaData();
    }

    /**
     * return all the decorated keys within an sstable
     * @param sstablePath - path to sstable
     */
    public Stream<DecoratedKey> keys(String sstablePath) {
        Descriptor desc = Descriptor.fromFilename(sstablePath);
        KeyIterator keyIter = new KeyIterator(desc, metadata);
        Spliterator<DecoratedKey> spliterator = Spliterators.spliteratorUnknownSize(keyIter, Spliterator.IMMUTABLE);
        return StreamSupport.stream(spliterator, false);
    }

    public Stream<Partition> readSSTable(String sstablePath, Stream<String> keys, Set<String> excludes)
            throws IOException {
        Descriptor desc = Descriptor.fromFilename(sstablePath);
        SSTableReader sstable = SSTableReader.openNoValidation(desc, metadata);
        IPartitioner partitioner = sstable.getPartitioner();
        return keys.filter(key -> !excludes.contains(key))
                   .map(metadata.getKeyValidator()::fromString)
                   .map(partitioner::decorateKey)
                   .sorted()
                   .map(key -> sstable.iterator(key, ColumnFilter.all(metadata), false, false))
                   .map(Partition::new);
    }

    public Stream<Partition> readSSTable(String sstablePath, Set<String> excludes) throws IOException {
        Descriptor desc = Descriptor.fromFilename(sstablePath);
        SSTableReader sstable = SSTableReader.openNoValidation(desc, metadata);

        Spliterator<UnfilteredRowIterator> spliterator = Spliterators.spliteratorUnknownSize(
                sstable.getScanner(), Spliterator.IMMUTABLE);
        Stream<UnfilteredRowIterator> stream = StreamSupport.stream(spliterator, false);
        return stream.filter(i -> {
                        return excludes == null ||
                               excludes.isEmpty() ||
                               !excludes.contains(metadata.getKeyValidator().getString(i.partitionKey().getKey()));
                      })
                     .map(Partition::new);
    }

    public CFMetaData getMetadata() {
        return metadata;
    }

}
