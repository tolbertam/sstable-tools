package com.csforge.reader;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.CreateTableStatement;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.tools.Util;

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
        metadata = ((CreateTableStatement) QueryProcessor.parseStatement(cql).prepare().statement).getCFMetaData();
    }

    public Stream<DecoratedKey> keys(Stream<String> keys) {
        return null;
    }

    public Stream<Partition> readSSTable(String sstablePath, Set excludes) throws IOException {
        Descriptor desc = Descriptor.fromFilename(sstablePath);
        SSTableReader sstable = SSTableReader.openNoValidation(desc, metadata);

        Spliterator<UnfilteredRowIterator> spliterator = Spliterators.spliteratorUnknownSize(sstable.getScanner(), Spliterator.IMMUTABLE);
        Stream<UnfilteredRowIterator> stream = StreamSupport.stream(spliterator, false);
        return stream.map(Partition::new);
    }

}
