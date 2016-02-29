package com.csforge.sstable;

import com.csforge.sstable.reader.Partition;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.VariableSpecifications;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.cql3.selection.Selection;
import org.apache.cassandra.cql3.statements.Bound;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.cql3.statements.StatementType;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.ComplexColumnData;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class Query {
    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(Query.class);

    public static QueryOptions OPTIONS = QueryOptions.DEFAULT;

    public final StatementRestrictions restrictions;
    public final SelectStatement.RawStatement statement;
    public final CFMetaData cfm;
    public final Selection selection;
    public final ColumnFilter queriedColumns;
    public final Collection<File> path;

    public Query(String query, Collection<File> path, CFMetaData cfm) throws IllegalAccessException, NoSuchFieldException, IOException {
        SelectStatement.RawStatement statement = (SelectStatement.RawStatement) QueryProcessor.parseStatement(query);
        if (!statement.parameters.orderings.isEmpty()) {
            throw new UnsupportedOperationException("ORDER BY not supported");
        }
        if (statement.parameters.isDistinct) {
            throw new UnsupportedOperationException("DISTINCT not supported");
        }
        this.path = path;

        VariableSpecifications boundNames = statement.getBoundVariables();

        Selection selection = statement.selectClause.isEmpty()
                ? Selection.wildcard(cfm)
                : Selection.fromSelectors(cfm, statement.selectClause);

        // yes its unfortunate, im sorry
        StatementType type = mock(StatementType.class);
        when(type.allowClusteringColumnSlices()).thenReturn(true);
        when(type.allowNonPrimaryKeyInWhereClause()).thenReturn(true);
        when(type.allowPartitionKeyRanges()).thenReturn(true);
        when(type.allowUseOfSecondaryIndices()).thenReturn(false);

        this.restrictions = new StatementRestrictions(
                type,
                cfm,
                statement.whereClause,
                boundNames,
                selection.containsOnlyStaticColumns(),
                selection.containsACollection(),
                true, true);

        this.statement = statement;
        this.cfm = cfm;
        this.selection = selection;

        ColumnFilter filter;
        if (selection.isWildcard()) {
            filter = ColumnFilter.all(cfm);
        } else {
            ColumnFilter.Builder builder = ColumnFilter.selectionBuilder();
            selection.getColumns().stream().filter(def -> !def.isPrimaryKeyColumn()).forEach(builder::add);
            filter = builder.build();
        }
        this.queriedColumns = filter;
    }

    private ClusteringIndexFilter makeClusteringIndexFilter()
            throws InvalidRequestException {
        if (restrictions.isColumnRange()) {
            Slices slices = makeSlices();
            if (slices == Slices.NONE && !selection.containsStaticColumns())
                return null;

            return new ClusteringIndexSliceFilter(slices, false);
        } else {
            NavigableSet<Clustering> clusterings = restrictions.getClusteringColumns(OPTIONS);
            if (clusterings.isEmpty() && queriedColumns.fetchedColumns().statics.isEmpty())
                return null;

            return new ClusteringIndexNamesFilter(clusterings, false);
        }
    }

    private Slices makeSlices()
            throws InvalidRequestException {
        SortedSet<Slice.Bound> startBounds = restrictions.getClusteringColumnsBounds(Bound.START, OPTIONS);
        SortedSet<Slice.Bound> endBounds = restrictions.getClusteringColumnsBounds(Bound.END, OPTIONS);
        assert startBounds.size() == endBounds.size();

        // The case where startBounds == 1 is common enough that it's worth optimizing
        if (startBounds.size() == 1) {
            Slice.Bound start = startBounds.first();
            Slice.Bound end = endBounds.first();
            return cfm.comparator.compare(start, end) > 0
                    ? Slices.NONE
                    : Slices.with(cfm.comparator, Slice.make(start, end));
        }

        Slices.Builder builder = new Slices.Builder(cfm.comparator, startBounds.size());
        Iterator<Slice.Bound> startIter = startBounds.iterator();
        Iterator<Slice.Bound> endIter = endBounds.iterator();
        while (startIter.hasNext() && endIter.hasNext()) {
            Slice.Bound start = startIter.next();
            Slice.Bound end = endIter.next();

            // Ignore slices that are nonsensical
            if (cfm.comparator.compare(start, end) > 0)
                continue;

            builder.add(start, end);
        }

        return builder.build();
    }

    public UnfilteredPartitionIterator getScanner() throws IOException {
        return getScanner(Integer.MAX_VALUE, new PagingData());
    }

    public UnfilteredPartitionIterator getScanner(int pageSize, PagingData pagingData) throws IOException {
        Preconditions.checkNotNull(pagingData);
        AbstractBounds<PartitionPosition> bounds = restrictions.getPartitionKeyBounds(OPTIONS);
        DataRange range = new DataRange(bounds, makeClusteringIndexFilter());
        final DataRange pageRange;
        if (pagingData.hasMorePages()) {
            pageRange = range.forPaging(new Bounds<>(pagingData.getPartitionKey(), bounds.right),
                    cfm.comparator, pagingData.getClustering(), false);
        } else {
            pageRange = range;
        }
        List<SSTableReader> readers = Lists.newArrayList();
        for (File f : path) {
            Descriptor desc = Descriptor.fromFilename(f.getAbsolutePath());
            readers.add(SSTableReader.openNoValidation(desc, cfm));
        }
        int now = FBUtilities.nowInSeconds();
        List<UnfilteredPartitionIterator> all = readers.stream().map(r -> r.getScanner(queriedColumns, pageRange, false)).collect(Collectors.toList());
        UnfilteredPartitionIterator ret = UnfilteredPartitionIterators.mergeLazily(all, now);
        ret = restrictions.getRowFilter(null, OPTIONS).filter(ret, now);
        if (statement.limit != null && !selection.isAggregate()) {
            int limit = Integer.parseInt(statement.limit.getText());
            DataLimits limits = DataLimits.cqlLimits(limit);
            if (pageSize != Integer.MAX_VALUE) {
                if (pagingData.hasMorePages()) {
                    limits = limits.forPaging(pageSize, pagingData.getPartitionKey().getKey(), limit - pagingData.getRowCount());
                } else {
                    limits = limits.forPaging(pageSize);
                }
            }
            ret = limits.filter(ret, now);
        }
        return ret;
    }

    public ResultSetData getResults() throws IOException {
        return getResults(Integer.MAX_VALUE);
    }

    public ResultSetData getResults(int pageSize) throws IOException {
        return getResults(pageSize, new PagingData());
    }

    public ResultSetData getResults(int pageSize, PagingData pagingData) throws IOException {
        Preconditions.checkNotNull(pagingData);
        int now = FBUtilities.nowInSeconds();
        Selection.ResultSetBuilder result = selection.resultSetBuilder(statement.parameters.isJson);
        try (UnfilteredPartitionIterator scanner = getScanner(pageSize, pagingData)) {
            PartitionIterator partitions = UnfilteredPartitionIterators.filter(scanner, now);
            AtomicInteger rowsPaged = new AtomicInteger(0);
            Clustering newClustering = null;
            DecoratedKey newPartitionKey = null;

            int limit = statement.limit != null && !selection.isAggregate() ? Integer.parseInt(statement.limit.getText()) : Integer.MAX_VALUE;

            int rowsThusFar = pagingData.getRowCount() + rowsPaged.get();
            while (rowsThusFar < limit && partitions.hasNext()) {
                try (RowIterator partition = partitions.next()) {
                    newPartitionKey = partition.partitionKey();
                    // Remaining is the min of how many the requested page size - how many pages
                    // - or - the limit minus rows read overall.
                    newClustering = processPartition(partition, OPTIONS, result, now, Math.min(pageSize - rowsPaged.get(), limit - rowsThusFar), rowsPaged);
                    rowsThusFar = pagingData.getRowCount() + rowsPaged.get();
                    if (newClustering != null) {
                        break;
                    }
                }
            }

            // Stop paging when we hit limit.
            if (rowsThusFar >= limit) {
                newClustering = null;
            }

            PagingData newPagingData = new PagingData(newPartitionKey, newClustering, rowsThusFar);
            return new ResultSetData(result.build(OPTIONS.getProtocolVersion()), newPagingData);
        }
    }

    Clustering processPartition(RowIterator partition, QueryOptions options, Selection.ResultSetBuilder result, int nowInSec, int remaining, AtomicInteger rowsPaged)
            throws InvalidRequestException {
        int protocolVersion = options.getProtocolVersion();

        ByteBuffer[] keyComponents = SelectStatement.getComponents(cfm, partition.partitionKey());

        Row staticRow = partition.staticRow();
        if (!partition.hasNext()) {
            if (!staticRow.isEmpty() && (!restrictions.usesSecondaryIndexing() || cfm.isStaticCompactTable()) && !restrictions.hasClusteringColumnsRestriction()) {
                result.newRow(protocolVersion);
            }
            for (ColumnDefinition def : selection.getColumns()) {
                switch (def.kind) {
                    case PARTITION_KEY:
                        result.add(keyComponents[def.position()]);
                        break;
                    case STATIC:
                        addValue(result, def, staticRow, nowInSec, protocolVersion);
                        break;
                    default:
                        result.add((ByteBuffer) null);
                }
            }
            return null;
        }

        while (remaining-- > 0 && partition.hasNext()) {
            Row row = partition.next();
            result.newRow(protocolVersion);
            // Respect selection order
            for (ColumnDefinition def : selection.getColumns()) {
                switch (def.kind) {
                    case PARTITION_KEY:
                        result.add(keyComponents[def.position()]);
                        break;
                    case CLUSTERING:
                        result.add(row.clustering().get(def.position()));
                        break;
                    case REGULAR:
                        addValue(result, def, row, nowInSec, protocolVersion);
                        break;
                    case STATIC:
                        addValue(result, def, staticRow, nowInSec, protocolVersion);
                        break;
                }
            }
            rowsPaged.incrementAndGet();
            if (remaining <= 0) {
                return row.clustering();
            }
        }
        return null;
    }

    private static void addValue(Selection.ResultSetBuilder result, ColumnDefinition def, Row row, int nowInSec, int protocolVersion) {
        if (def.isComplex()) {
            // Collections are the only complex types we have so far
            assert def.type.isCollection() && def.type.isMultiCell();
            ComplexColumnData complexData = row.getComplexColumnData(def);
            if (complexData == null)
                result.add((ByteBuffer) null);
            else
                result.add(((CollectionType) def.type).serializeForNativeProtocol(def, complexData.iterator(), protocolVersion));
        } else {
            result.add(row.getCell(def), nowInSec);
        }
    }


    public static void main(String... args) {
        try {
            String query = String.join(" ", args);
            SelectStatement.RawStatement statement = (SelectStatement.RawStatement) QueryProcessor.parseStatement(query);
            // The column family name in this case is the sstable file path.
            File path = new File(statement.columnFamily());
            CFMetaData metadata = CassandraUtils.tableFromBestSource(path);
            Query q = new Query(query, Collections.singleton(path), metadata);

            if (System.getProperty("query.toJson") != null) {
                Spliterator<UnfilteredRowIterator> spliterator = Spliterators.spliteratorUnknownSize(
                        q.getScanner(), Spliterator.IMMUTABLE);
                Stream<UnfilteredRowIterator> stream = StreamSupport.stream(spliterator, false);
                JsonTransformer.toJson(stream.map(Partition::new), q.cfm, false, System.out);
            } else {
                TableTransformer.dumpResults(metadata, q.getResults().getResultSet(), System.out);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
