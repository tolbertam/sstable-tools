package com.csforge.sstable;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.io.CharStreams;
import com.google.common.io.Files;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.CFStatement;
import org.apache.cassandra.cql3.statements.CreateTableStatement;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.ValidationMetadata;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.*;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class CassandraUtils {
    private static final Logger logger = LoggerFactory.getLogger(CassandraUtils.class);
    private static final AtomicInteger cfCounter = new AtomicInteger();

    static {
        Config.setClientMode(true);

        // Partitioner is not set in client mode.
        if (DatabaseDescriptor.getPartitioner() == null)
            DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);

        // need system keyspace metadata registered for functions used in CQL like count/avg/json
        Schema.instance.setKeyspaceMetadata(SystemKeyspace.metadata());

    }

    public static CFMetaData tableFromBestSource(File sstablePath) throws IOException, NoSuchFieldException, IllegalAccessException {
        // TODO add CQL/Thrift mechanisms as well
        String cqlPath = System.getProperty("sstabletools.schema");
        InputStream in;
        if (!Strings.isNullOrEmpty(cqlPath)) {
            in = new FileInputStream(cqlPath);
        } else {
            in = Query.class.getClassLoader().getResourceAsStream("schema.cql");
            if(in == null) {
                in = new FileInputStream("schema.cql");
            }
        }
        CFMetaData metadata;
        if(in == null) {
            logger.debug("Using metadata from sstable");
            metadata = CassandraUtils.tableFromSSTable(sstablePath);
        } else {
            logger.debug("Using metadata from CQL schema in " + cqlPath);
            metadata = CassandraUtils.tableFromCQL(in);
        }
        return metadata;
    }

    public static CFMetaData tableFromCQL(InputStream source) throws IOException {
        String schema = CharStreams.toString(new InputStreamReader(source, "UTF-8"));
        CFStatement statement = (CFStatement) QueryProcessor.parseStatement(schema);
        String keyspace = "";
        try {
            keyspace = statement.keyspace() == null? "turtles" : statement.keyspace();
        } catch (AssertionError e) { // if -ea added we should provide lots of warnings that things probably wont work
            logger.warn("Remove '-ea' JVM option when using sstable-tools library");
            keyspace = "turtles";
        }
        statement.prepareKeyspace(keyspace);

        Schema.instance.setKeyspaceMetadata(KeyspaceMetadata.create(keyspace, KeyspaceParams.local(), Tables.none(),
                Views.none(), Types.none(), Functions.none()));
        return ((CreateTableStatement) statement.prepare().statement).getCFMetaData();
    }

    public static Object readPrivate(Object obj, String name) throws NoSuchFieldException, IllegalAccessException {
        Field type = obj.getClass().getDeclaredField(name);
        type.setAccessible(true);
        return type.get(obj);
    }

    @SuppressWarnings("unchecked")
    public static CFMetaData tableFromSSTable(File path) throws IOException, NoSuchFieldException, IllegalAccessException {
        Preconditions.checkNotNull(path);
        Descriptor desc = Descriptor.fromFilename(path.getAbsolutePath());

        EnumSet<MetadataType> types = EnumSet.of(MetadataType.VALIDATION, MetadataType.STATS, MetadataType.HEADER);
        Map<MetadataType, MetadataComponent> sstableMetadata = desc.getMetadataSerializer().deserialize(desc, types);
        ValidationMetadata validationMetadata = (ValidationMetadata) sstableMetadata.get(MetadataType.VALIDATION);
        Preconditions.checkNotNull(validationMetadata, "Validation Metadata could not be resolved, accompanying Statistics.db file must be missing.");
        SerializationHeader.Component header = (SerializationHeader.Component) sstableMetadata.get(MetadataType.HEADER);
        Preconditions.checkNotNull(header, "Metadata could not be resolved, accompanying Statistics.db file must be missing.");

        IPartitioner partitioner = FBUtilities.newPartitioner(validationMetadata.partitioner);
        DatabaseDescriptor.setPartitionerUnsafe(partitioner);

        AbstractType<?> keyType = (AbstractType<?>) readPrivate(header, "keyType");
        List<AbstractType<?>> clusteringTypes = (List<AbstractType<?>>) readPrivate(header, "clusteringTypes");
        Map<ByteBuffer, AbstractType<?>> staticColumns = (Map<ByteBuffer, AbstractType<?>>) readPrivate(header, "staticColumns");
        Map<ByteBuffer, AbstractType<?>> regularColumns = (Map<ByteBuffer, AbstractType<?>>) readPrivate(header, "regularColumns");
        int id = cfCounter.incrementAndGet();
        CFMetaData.Builder builder = CFMetaData.Builder.create("turtle" + id, "turtles" + id);
        staticColumns.entrySet().stream()
                .forEach(entry ->
                        builder.addStaticColumn(UTF8Type.instance.getString(entry.getKey()), entry.getValue()));
        regularColumns.entrySet().stream()
                .forEach(entry ->
                        builder.addRegularColumn(UTF8Type.instance.getString(entry.getKey()), entry.getValue()));
        builder.addPartitionKey("PartitionKey", keyType);
        for (int i = 0; i < clusteringTypes.size(); i++) {
            builder.addClusteringColumn("key" + (i > 0 ? i : ""), clusteringTypes.get(i));
        }
        CFMetaData metaData = builder.build();
        Schema.instance.setKeyspaceMetadata(KeyspaceMetadata.create(metaData.ksName, KeyspaceParams.local(),
                Tables.of(metaData), Views.none(), Types.none(), Functions.none()));
        return metaData;
    }
}
