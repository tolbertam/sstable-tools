package com.csforge.sstable.reader;

import com.google.common.annotations.VisibleForTesting;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.CFStatement;
import org.apache.cassandra.cql3.statements.CreateTableStatement;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.KeyIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.sstable.metadata.ValidationMetadata;
import org.apache.cassandra.schema.*;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class CassandraReader {
    private static final Logger logger = LoggerFactory.getLogger(CassandraReader.class);

    CFMetaData metadata = null;

    @VisibleForTesting
    CassandraReader() {}

    public static CassandraReader fromCql(String cql) {
        Schema.instance.setKeyspaceMetadata(KeyspaceMetadata.create("turtle", KeyspaceParams.local(), Tables.none(),
                Views.none(), Types.none(), Functions.none()));
        CFStatement statement = (CFStatement) QueryProcessor.parseStatement(cql);
        statement.prepareKeyspace("turtle");

        CassandraReader reader = new CassandraReader();
        reader.metadata = ((CreateTableStatement) statement.prepare().statement).getCFMetaData();
        return reader;
    }

    private static Object readPrivate(Object obj, String name) throws NoSuchFieldException, IllegalAccessException {
        Field type = obj.getClass().getDeclaredField(name);
        type.setAccessible(true);
        return type.get(obj);
    }

    public static CassandraReader fromSSTable(String path) {
        try {
            Schema.instance.setKeyspaceMetadata(KeyspaceMetadata.create("turtle", KeyspaceParams.local(), Tables.none(),
                    Views.none(), Types.none(), Functions.none()));
            Descriptor desc = Descriptor.fromFilename(path);

            EnumSet<MetadataType> types = EnumSet.of(MetadataType.VALIDATION, MetadataType.STATS, MetadataType.HEADER);
            Map<MetadataType, MetadataComponent> sstableMetadata = desc.getMetadataSerializer().deserialize(desc, types);
            ValidationMetadata validationMetadata = (ValidationMetadata) sstableMetadata.get(MetadataType.VALIDATION);
            StatsMetadata statsMetadata = (StatsMetadata) sstableMetadata.get(MetadataType.STATS);
            SerializationHeader.Component header = (SerializationHeader.Component) sstableMetadata.get(MetadataType.HEADER);

            IPartitioner partitioner = FBUtilities.newPartitioner(validationMetadata.partitioner);
            DatabaseDescriptor.setPartitionerUnsafe(partitioner);

            AbstractType<?> keyType = (AbstractType<?>) readPrivate(header, "keyType");
            List<AbstractType<?>> clusteringTypes = (List<AbstractType<?>>) readPrivate(header, "clusteringTypes");
            Map<ByteBuffer, AbstractType<?>> staticColumns = (Map<ByteBuffer, AbstractType<?>>) readPrivate(header, "staticColumns");
            Map<ByteBuffer, AbstractType<?>> regularColumns = (Map<ByteBuffer, AbstractType<?>>) readPrivate(header, "regularColumns");

            CassandraReader reader = new CassandraReader();
            CFMetaData.Builder builder = CFMetaData.Builder.create("turtle", "turtles");
            staticColumns.entrySet().stream()
                    .forEach(entry ->
                            builder.addStaticColumn(UTF8Type.instance.getString(entry.getKey()), entry.getValue()));
            regularColumns.entrySet().stream()
                    .forEach(entry ->
                            builder.addRegularColumn(UTF8Type.instance.getString(entry.getKey()), entry.getValue()));
            builder.addPartitionKey("PartitionKey", keyType);
            for(int i = 0;i < clusteringTypes.size(); i++) {
                builder.addClusteringColumn("key" + (i>0? i: ""), clusteringTypes.get(i));
            }
            reader.metadata = builder.build();
            return reader;
        } catch (IOException|NoSuchFieldException|IllegalAccessException e) {
            logger.error("Unable to build CFMetaData", e);
            System.exit(-1);
            return null;
        }
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
