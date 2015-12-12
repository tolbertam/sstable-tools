package com.csforge.reader;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.CFStatement;
import org.apache.cassandra.cql3.statements.CreateTableStatement;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.*;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class CassandraReader {

    CFMetaData metadata = null;

    public CassandraReader(String cql) {
        Schema.instance.setKeyspaceMetadata(KeyspaceMetadata.create("turtle", KeyspaceParams.local(), Tables.none(), Views.none(), Types.none(), Functions.none()));
        CFStatement statement = (CFStatement) QueryProcessor.parseStatement(cql);
        statement.prepareKeyspace("turtle");
        metadata = ((CreateTableStatement) statement.prepare().statement).getCFMetaData();
    }

    public Stream<DecoratedKey> keys(Stream<String> keys) {
        return null;
    }

    public Stream<Partition> readSSTable(String sstablePath, Set<DecoratedKey> excludes) throws IOException {
        Descriptor desc = Descriptor.fromFilename(sstablePath);
        SSTableReader sstable = SSTableReader.openNoValidation(desc, metadata);

        Spliterator<UnfilteredRowIterator> spliterator = Spliterators.spliteratorUnknownSize(sstable.getScanner(), Spliterator.IMMUTABLE);
        Stream<UnfilteredRowIterator> stream = StreamSupport.stream(spliterator, false);
        return stream.filter(i -> excludes != null && excludes.contains(i.partitionKey()))
                     .map(Partition::new);
    }

    public CFMetaData getMetadata() {
        return metadata;
    }

}
