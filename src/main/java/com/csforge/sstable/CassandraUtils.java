package com.csforge.sstable;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.io.CharStreams;
import jline.console.ConsoleReader;
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
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.*;
import org.apache.cassandra.schema.*;
import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.cassandra.utils.FBUtilities;
import org.joda.time.Duration;
import org.joda.time.format.PeriodFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class CassandraUtils {
    private static final Logger logger = LoggerFactory.getLogger(CassandraUtils.class);
    private static final AtomicInteger cfCounter = new AtomicInteger();
    public static Map<String, UserType> knownTypes = Maps.newHashMap();
    public static String cqlOverride = null;
    private static String FULL_BAR = Strings.repeat("█", 30);
    private static String EMPTY_BAR = Strings.repeat("░", 30);

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

        CFMetaData metadata;
        if (!Strings.isNullOrEmpty(cqlOverride)) {
            logger.debug("Using override metadata");
            metadata = CassandraUtils.tableFromCQL(new ByteArrayInputStream(cqlOverride.getBytes()));
        } else {
            InputStream in = findSchema();
            if (in == null) {
                logger.debug("Using metadata from sstable");
                metadata = CassandraUtils.tableFromSSTable(sstablePath);
            } else {
                metadata = CassandraUtils.tableFromCQL(in);
            }
        }
        return metadata;
    }

    private static Types getTypes() {
        if (knownTypes.isEmpty()) {
            return Types.none();
        } else {
            return Types.of(knownTypes.values().toArray(new UserType[0]));
        }
    }

    public static InputStream findSchema() throws IOException {
        String cqlPath = System.getProperty("sstabletools.schema");
        InputStream in;
        if (!Strings.isNullOrEmpty(cqlPath)) {
            in = new FileInputStream(cqlPath);
        } else {
            in = Query.class.getClassLoader().getResourceAsStream("schema.cql");
            if (in == null && new File("schema.cql").exists()) {
                in = new FileInputStream("schema.cql");
            }
        }
        return in;
    }

    public static void loadTablesFromRemote(String host, int port) throws IOException {
        Cluster.Builder builder = Cluster.builder().addContactPoints(host).withPort(port);

        try (Cluster cluster = builder.build(); Session session = cluster.connect()) {
            Metadata metadata = cluster.getMetadata();
            IPartitioner partitioner = FBUtilities.newPartitioner(metadata.getPartitioner());
            if (DatabaseDescriptor.getPartitioner() == null)
                DatabaseDescriptor.setPartitionerUnsafe(partitioner);
            for (com.datastax.driver.core.KeyspaceMetadata ksm : metadata.getKeyspaces()) {
                if(!ksm.getName().equals("system")) {
                    for (TableMetadata tm : ksm.getTables()) {
                        CassandraUtils.tableFromCQL(new ByteArrayInputStream(tm.asCQLQuery().getBytes()), tm.getId());
                    }
                }
            }
        }
    }

    public static CFMetaData tableFromCQL(InputStream source) throws IOException {
        return tableFromCQL(source, null);
    }

    public static CFMetaData tableFromCQL(InputStream source, UUID cfid) throws IOException {
        String schema = CharStreams.toString(new InputStreamReader(source, "UTF-8"));
        CFStatement statement = (CFStatement) QueryProcessor.parseStatement(schema);
        String keyspace = "";
        try {
            keyspace = statement.keyspace() == null ? "turtles" : statement.keyspace();
        } catch (AssertionError e) { // if -ea added we should provide lots of warnings that things probably wont work
            logger.warn("Remove '-ea' JVM option when using sstable-tools library");
            keyspace = "turtles";
        }
        statement.prepareKeyspace(keyspace);
        if(Schema.instance.getKSMetaData(keyspace) == null) {
            Schema.instance.setKeyspaceMetadata(KeyspaceMetadata.create(keyspace, KeyspaceParams.local(), Tables.none(),
                    Views.none(), getTypes(), Functions.none()));
        }
        CFMetaData cfm;
        if(cfid != null) {
            cfm = ((CreateTableStatement) statement.prepare().statement).metadataBuilder().withId(cfid).build();
            KeyspaceMetadata prev = Schema.instance.getKSMetaData(keyspace);
            List<CFMetaData> tables = Lists.newArrayList(prev.tablesAndViews());
            tables.add(cfm);
            Schema.instance.setKeyspaceMetadata(prev.withSwapped(Tables.of(tables)));
            Schema.instance.load(cfm);
        } else {
            cfm = ((CreateTableStatement) statement.prepare().statement).getCFMetaData();
        }
        return cfm;
    }

    public static Object readPrivate(Object obj, String name) throws NoSuchFieldException, IllegalAccessException {
        Field type = obj.getClass().getDeclaredField(name);
        type.setAccessible(true);
        return type.get(obj);
    }

    public static Object callPrivate(Object obj, String name) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Method m = obj.getClass().getDeclaredMethod(name);
        m.setAccessible(true);
        return m.invoke(obj);
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

        IPartitioner partitioner = validationMetadata.partitioner.endsWith("LocalPartitioner") ?
                new LocalPartitioner(header.getKeyType()) :
                FBUtilities.newPartitioner(validationMetadata.partitioner);

        DatabaseDescriptor.setPartitionerUnsafe(partitioner);
        AbstractType<?> keyType = header.getKeyType();
        List<AbstractType<?>> clusteringTypes = header.getClusteringTypes();
        Map<ByteBuffer, AbstractType<?>> staticColumns = header.getStaticColumns();
        Map<ByteBuffer, AbstractType<?>> regularColumns = header.getRegularColumns();
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
                Tables.of(metaData), Views.none(), getTypes(), Functions.none()));
        return metaData;
    }

    public static <T> Stream<T> asStream(Iterator<T> iter) {
        Spliterator<T> splititer = Spliterators.spliteratorUnknownSize(iter, Spliterator.IMMUTABLE);
        return StreamSupport.stream(splititer, false);
    }

    public static String wrapQuiet(String toWrap, boolean color) {
        StringBuilder sb = new StringBuilder();
        if (color) {
            sb.append(TableTransformer.ANSI_WHITE);
        }
        sb.append("(");
        sb.append(toWrap);
        sb.append(")");
        if (color) {
            sb.append(TableTransformer.ANSI_RESET);
        }
        return sb.toString();
    }

    public static String toDateString(long time, TimeUnit unit, boolean color) {
        return wrapQuiet(new java.text.SimpleDateFormat("MM/dd/yyyy HH:mm:ss").format(new java.util.Date(unit.toMillis(time))), color);
    }

    public static String toDurationString(long duration, TimeUnit unit, boolean color) {
        return wrapQuiet(PeriodFormat.getDefault().print(new Duration(unit.toMillis(duration)).toPeriod()), color);
    }

    public static String toByteString(long bytes, boolean si, boolean color) {
        int unit = si ? 1000 : 1024;
        if (bytes < unit) return bytes + " B";
        int exp = (int) (Math.log(bytes) / Math.log(unit));
        String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp - 1) + (si ? "" : "i");
        return wrapQuiet(String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre), color);
    }

    private static class ValuedByteBuffer {
        public long value;
        public ByteBuffer buffer;

        public ValuedByteBuffer(ByteBuffer buffer, long value) {
            this.value = value;
            this.buffer = buffer;
        }

        public long getValue() {
            return value;
        }
    }

    private static Comparator<ValuedByteBuffer> VCOMP = Comparator.comparingLong(ValuedByteBuffer::getValue).reversed();

    public static void printStats(String fname, PrintStream out) throws IOException, NoSuchFieldException, IllegalAccessException {
        printStats(fname, out, null);
    }

    public static void printStats(String fname, PrintStream out, ConsoleReader console) throws IOException, NoSuchFieldException, IllegalAccessException {
        boolean color = console == null || console.getTerminal().isAnsiSupported();
        String c = color ? TableTransformer.ANSI_BLUE : "";
        String s = color ? TableTransformer.ANSI_CYAN : "";
        String r = color ? TableTransformer.ANSI_RESET : "";
        if (new File(fname).exists()) {
            Descriptor descriptor = Descriptor.fromFilename(fname);

            Map<MetadataType, MetadataComponent> metadata = descriptor.getMetadataSerializer().deserialize(descriptor, EnumSet.allOf(MetadataType.class));
            ValidationMetadata validation = (ValidationMetadata) metadata.get(MetadataType.VALIDATION);
            StatsMetadata stats = (StatsMetadata) metadata.get(MetadataType.STATS);
            CompactionMetadata compaction = (CompactionMetadata) metadata.get(MetadataType.COMPACTION);
            CompressionMetadata compression = null;
            File compressionFile = new File(descriptor.filenameFor(Component.COMPRESSION_INFO));
            if (compressionFile.exists())
                compression = CompressionMetadata.create(fname);
            SerializationHeader.Component header = (SerializationHeader.Component) metadata.get(MetadataType.HEADER);

            CFMetaData cfm = tableFromBestSource(new File(fname));
            SSTableReader reader = SSTableReader.openNoValidation(descriptor, cfm);
            ISSTableScanner scanner = reader.getScanner();
            long bytes = scanner.getLengthInBytes();
            MinMaxPriorityQueue<ValuedByteBuffer> widestPartitions = MinMaxPriorityQueue
                    .orderedBy(VCOMP)
                    .maximumSize(5)
                    .create();
            MinMaxPriorityQueue<ValuedByteBuffer> largestPartitions = MinMaxPriorityQueue
                    .orderedBy(VCOMP)
                    .maximumSize(5)
                    .create();
            MinMaxPriorityQueue<ValuedByteBuffer> mostTombstones = MinMaxPriorityQueue
                    .orderedBy(VCOMP)
                    .maximumSize(5)
                    .create();
            long partitionCount = 0;
            long rowCount = 0;
            long tombstoneCount = 0;
            long cellCount = 0;
            double totalCells = stats.totalColumnsSet;
            int lastPercent = 0;
            while (scanner.hasNext()) {
                UnfilteredRowIterator partition = scanner.next();

                long psize = 0;
                long pcount = 0;
                int ptombcount = 0;
                partitionCount++;
                if (!partition.staticRow().isEmpty()) {
                    rowCount++;
                    pcount++;
                    psize += partition.staticRow().dataSize();
                }
                if (!partition.partitionLevelDeletion().isLive()) {
                    tombstoneCount++;
                    ptombcount++;
                }
                while (partition.hasNext()) {
                    Unfiltered unfiltered = partition.next();
                    switch (unfiltered.kind()) {
                        case ROW:
                            rowCount++;
                            Row row = (Row) unfiltered;
                            psize += row.dataSize();
                            pcount++;
                            for (Cell cell : row.cells()) {
                                cellCount++;
                                double percentComplete = Math.min(1.0, cellCount / totalCells);
                                if (lastPercent != (int) (percentComplete * 100)) {
                                    lastPercent = (int) (percentComplete * 100);
                                    int cols = (int) (percentComplete * 30);
                                    System.out.printf("\r%sAnalyzing SSTable...  %s%s%s %s(%%%s)", c, s, FULL_BAR.substring(30 - cols), EMPTY_BAR.substring(cols), r, (int) (percentComplete * 100));
                                    System.out.flush();
                                }
                                if (cell.isTombstone()) {
                                    tombstoneCount++;
                                    ptombcount++;
                                }
                            }
                            break;
                        case RANGE_TOMBSTONE_MARKER:
                            tombstoneCount++;
                            ptombcount++;
                            break;
                    }
                }
                widestPartitions.add(new ValuedByteBuffer(partition.partitionKey().getKey(), pcount));
                largestPartitions.add(new ValuedByteBuffer(partition.partitionKey().getKey(), psize));
                mostTombstones.add(new ValuedByteBuffer(partition.partitionKey().getKey(), ptombcount));
            }
            out.printf("\r%80s\r", " ");
            out.printf("%sPartitions%s:%s %s%n", c, s, r, partitionCount);
            out.printf("%sRows%s:%s %s%n", c, s, r, rowCount);
            out.printf("%sTombstones%s:%s %s%n", c, s, r, tombstoneCount);
            out.printf("%sCells%s:%s %s%n", c, s, r, cellCount);
            out.printf("%sWidest Partitions%s:%s%n", c, s, r);
            asStream(widestPartitions.iterator()).sorted(VCOMP).forEach(p -> {
                out.printf("%s   [%s%s%s]%s %s%n", s, r, cfm.getKeyValidator().getString(p.buffer), s, r, p.value);
            });
            out.printf("%sLargest Partitions%s:%s%n", c, s, r);
            asStream(largestPartitions.iterator()).sorted(VCOMP).forEach(p -> {
                out.printf("%s   [%s%s%s]%s %s %s%n", s, r, cfm.getKeyValidator().getString(p.buffer), s, r, p.value, toByteString(p.value, true, color));
            });
            out.printf("%sTombstone Leaders%s:%s%n", c, s, r);
            asStream(mostTombstones.iterator()).sorted(VCOMP).forEach(p -> {
                if (p.value > 0) {
                    out.printf("%s   [%s%s%s]%s %s%n", s, r, cfm.getKeyValidator().getString(p.buffer), s, r, p.value);
                }
            });

            List<AbstractType<?>> clusteringTypes = (List<AbstractType<?>>) readPrivate(header, "clusteringTypes");
            if (validation != null) {
                out.printf("%sPartitioner%s:%s %s%n", c, s, r, validation.partitioner);
                out.printf("%sBloom Filter FP chance%s:%s %f%n", c, s, r, validation.bloomFilterFPChance);
            }
            if (stats != null) {
                out.printf("%sSize%s:%s %s %s %n", c, s, r, bytes, toByteString(bytes, true, color));
                out.printf("%sCompressor%s:%s %s%n", c, s, r, compression != null ? compression.compressor().getClass().getName() : "-");
                if (compression != null)
                    out.printf("%s  Compression ratio%s:%s %s%n", c, s, r, stats.compressionRatio);

                out.printf("%sMinimum timestamp%s:%s %s %s%n", c, s, r, stats.minTimestamp, toDateString(stats.minTimestamp, TimeUnit.MICROSECONDS, color));
                out.printf("%sMaximum timestamp%s:%s %s %s%n", c, s, r, stats.maxTimestamp, toDateString(stats.maxTimestamp, TimeUnit.MICROSECONDS, color));

                out.printf("%sSSTable min local deletion time%s:%s %s %s%n", c, s, r, stats.minLocalDeletionTime, toDateString(stats.minLocalDeletionTime, TimeUnit.SECONDS, color));
                out.printf("%sSSTable max local deletion time%s:%s %s %s%n", c, s, r, stats.maxLocalDeletionTime, toDateString(stats.maxLocalDeletionTime, TimeUnit.SECONDS, color));

                out.printf("%sTTL min%s:%s %s %s%n", c, s, r, stats.minTTL, toDurationString(stats.minTTL, TimeUnit.SECONDS, color));
                out.printf("%sTTL max%s:%s %s %s%n", c, s, r, stats.maxTTL, toDurationString(stats.maxTTL, TimeUnit.SECONDS, color));
                if (header != null && clusteringTypes.size() == stats.minClusteringValues.size()) {
                    List<ByteBuffer> minClusteringValues = stats.minClusteringValues;
                    List<ByteBuffer> maxClusteringValues = stats.maxClusteringValues;
                    String[] minValues = new String[clusteringTypes.size()];
                    String[] maxValues = new String[clusteringTypes.size()];
                    for (int i = 0; i < clusteringTypes.size(); i++) {
                        minValues[i] = clusteringTypes.get(i).getString(minClusteringValues.get(i));
                        maxValues[i] = clusteringTypes.get(i).getString(maxClusteringValues.get(i));
                    }
                    out.printf("%sminClustringValues%s:%s %s%n", c, s, r, Arrays.toString(minValues));
                    out.printf("%smaxClustringValues%s:%s %s%n", c, s, r, Arrays.toString(maxValues));
                }
                out.printf("%sEstimated droppable tombstones%s:%s %s%n", c, s, r, stats.getEstimatedDroppableTombstoneRatio((int) (System.currentTimeMillis() / 1000)));
                out.printf("%sSSTable Level%s:%s %d%n", c, s, r, stats.sstableLevel);
                out.printf("%sRepaired at%s:%s %d %s%n", c, s, r, stats.repairedAt, toDateString(stats.repairedAt, TimeUnit.MILLISECONDS, color));
                out.printf("  %sLower bound%s:%s %s%n", c, s, r, stats.commitLogLowerBound);
                out.printf("  %sUpper bound%s:%s %s%n", c, s, r, stats.commitLogUpperBound);
                out.printf("%stotalColumnsSet%s:%s %s%n", c, s, r, stats.totalColumnsSet);
                out.printf("%stotalRows%s:%s %s%n", c, s, r, stats.totalRows);
                out.printf("%sEstimated tombstone drop times%s:%s%n", c, s, r);

                TermHistogram h = new TermHistogram(stats.estimatedTombstoneDropTime.getAsMap().entrySet());
                String bcolor = color ? "\u001B[36m" : "";
                String reset = color ? "\u001B[0m" : "";
                String histoColor = color ? "\u001B[37m" : "";
                out.printf("%s  %-" + h.maxValueLength + "s                       | %-" + h.maxCountLength + "s   %%   Histogram %n", bcolor, "Value", "Count");
                stats.estimatedTombstoneDropTime.getAsMap().entrySet().stream().forEach(e -> {
                    String histo = h.asciiHistogram(e.getValue(), 30);
                    out.printf(reset +
                                    "  %-" + h.maxValueLength + "d %s %s|%s %" + h.maxCountLength + "s %s %s%s %n",
                            e.getKey().longValue(), toDateString(e.getKey().intValue(), TimeUnit.SECONDS, color),
                            bcolor, reset,
                            e.getValue(),
                            wrapQuiet(String.format("%3s", (int) (100 * (e.getValue() / h.sum))), color),
                            histoColor,
                            histo);
                });

                out.printf("%sEstimated partition size%s:%s%n", c, s, r);
                TermHistogram.printHistogram(stats.estimatedPartitionSize, out, color);

                out.printf("%sEstimated column count%s:%s%n", c, s, r);
                TermHistogram.printHistogram(stats.estimatedColumnCount, out, color);
            }
            if (compaction != null) {
                out.printf("%sEstimated cardinality%s:%s %s%n", c, s, r, compaction.cardinalityEstimator.cardinality());
            }
            if (header != null) {
                EncodingStats encodingStats = header.getEncodingStats();
                AbstractType<?> keyType = header.getKeyType();
                Map<ByteBuffer, AbstractType<?>> staticColumns = header.getStaticColumns();
                Map<ByteBuffer, AbstractType<?>> regularColumns = header.getRegularColumns();
                Map<String, String> statics = staticColumns.entrySet().stream()
                        .collect(Collectors.toMap(
                                e -> UTF8Type.instance.getString(e.getKey()),
                                e -> e.getValue().toString()));
                Map<String, String> regulars = regularColumns.entrySet().stream()
                        .collect(Collectors.toMap(
                                e -> UTF8Type.instance.getString(e.getKey()),
                                e -> e.getValue().toString()));

                out.printf("%sEncodingStats minTTL%s:%s %s %s%n", c, s, r, encodingStats.minTTL, toDurationString(encodingStats.minTTL, TimeUnit.SECONDS, color));
                out.printf("%sEncodingStats minLocalDeletionTime%s:%s %s %s%n", c, s, r, encodingStats.minLocalDeletionTime, toDateString(encodingStats.minLocalDeletionTime, TimeUnit.MILLISECONDS, color));
                out.printf("%sEncodingStats minTimestamp%s:%s %s %s%n", c, s, r, encodingStats.minTimestamp, toDateString(encodingStats.minTimestamp, TimeUnit.MICROSECONDS, color));
                out.printf("%sKeyType%s:%s %s%n", c, s, r, keyType.toString());
                out.printf("%sClusteringTypes%s:%s %s%n", c, s, r, clusteringTypes.toString());
                out.printf("%sStaticColumns%s:%s {%s}%n", c, s, r, FBUtilities.toString(statics));
                out.printf("%sRegularColumns%s:%s {%s}%n", c, s, r, FBUtilities.toString(regulars));
            }
        }
    }

    public static final TreeMap<Double, String> bars = new TreeMap<Double, String>() {{
        this.put(7.0 / 8.0, "▉"); // 7/8ths left block
        this.put(3.0 / 4.0, "▊"); // 3/4th block
        this.put(5.0 / 8.0, "▋"); // five eighths
        this.put(3.0 / 8.0, "▍"); // three eighths
        this.put(1.0 / 4.0, "▎");
        this.put(1.0 / 8.0, "▏");
    }};

    private static class TermHistogram {
        long max;
        long min;
        double sum;
        int maxCountLength = 5;
        int maxValueLength = 5;

        public static void printHistogram(EstimatedHistogram histogram, PrintStream out, boolean colors) {
            String bcolor = colors ? "\u001B[36m" : "";
            String reset = colors ? "\u001B[0m" : "";
            String histoColor = colors ? "\u001B[37m" : "";

            TermHistogram h = new TermHistogram(histogram);
            out.printf("%s  %-" + h.maxValueLength + "s | %-" + h.maxCountLength + "s   %%   Histogram %n", bcolor, "Value", "Count");
            long[] counts = histogram.getBuckets(false);
            long[] offsets = histogram.getBucketOffsets();
            for (int i = 0; i < counts.length; i++) {
                if (counts[i] > 0) {
                    String histo = h.asciiHistogram(counts[i], 30);
                    out.printf(reset +
                                    "  %-" + h.maxValueLength + "d %s|%s %" + h.maxCountLength + "s %s %s%s %n",
                            offsets[i],
                            bcolor, reset,
                            counts[i],
                            wrapQuiet(String.format("%3s", (int) (100 * ((double) counts[i] / h.sum))), true),
                            histoColor,
                            histo);
                }
            }
        }

        public TermHistogram(Collection<Map.Entry<Double, Long>> histogram) {
            histogram.stream().forEach(e -> {
                max = Math.max(max, e.getValue());
                min = Math.min(min, e.getValue());
                sum += e.getValue();
                maxCountLength = Math.max(maxCountLength, ("" + e.getValue()).length());
                maxValueLength = Math.max(maxValueLength, ("" + e.getKey().longValue()).length());
            });
        }

        public TermHistogram(EstimatedHistogram histogram) {
            long[] counts = histogram.getBuckets(false);
            for (int i = 0; i < counts.length; i++) {
                long e = counts[i];
                if (e > 0) {
                    max = Math.max(max, e);
                    min = Math.min(min, e);
                    sum += e;
                    maxCountLength = Math.max(maxCountLength, ("" + e).length());
                    maxValueLength = Math.max(maxValueLength, ("" + histogram.getBucketOffsets()[i]).length());
                }
            }
        }

        public String asciiHistogram(long count, int length) {
            StringBuilder sb = new StringBuilder();
            long barVal = count;
            int intWidth = (int) (barVal * 1.0 / max * length);
            double remainderWidth = (barVal * 1.0 / max * length) - intWidth;
            sb.append(Strings.repeat("▉", intWidth));
            if (bars.floorKey(remainderWidth) != null) {
                sb.append("" + bars.get(bars.floorKey(remainderWidth)));
            }
            return sb.toString();
        }
    }

}
