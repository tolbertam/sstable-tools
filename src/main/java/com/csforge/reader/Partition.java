package com.csforge.reader;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionColumns;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;

import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Layer that wraps UnfilteredRowIterator
 */
public class Partition {

    UnfilteredRowIterator parent;

    public Partition(UnfilteredRowIterator parent) {
        this.parent = parent;
    }

    /**
     * A timestamp (typically in microseconds since the unix epoch, although this is not enforced) after which
     * data should be considered deleted. If set to Long.MIN_VALUE, this implies that the data has not been marked
     * for deletion at all.
     */
    public long markedForDeleteAt() {
        return parent.partitionLevelDeletion().markedForDeleteAt();
    }

    public boolean isLive() {
        return markedForDeleteAt() > Long.MIN_VALUE;
    }

    /**
     * The local server timestamp, in seconds since the unix epoch, at which this tombstone was created. This is
     * only used for purposes of purging the tombstone after gc_grace_seconds have elapsed.
     */
    public int localDeletionTime() {
        return parent.partitionLevelDeletion().localDeletionTime();
    }

    public DecoratedKey getKey() {
        return parent.partitionKey();
    }

    public PartitionColumns columns() {
        return parent.columns();
    }

    public Row staticRow() {
        return parent.staticRow();
    }

    public Stream<Unfiltered> rows() {
        Spliterator<Unfiltered> spliterator = Spliterators.spliteratorUnknownSize(parent, Spliterator.IMMUTABLE);
        return StreamSupport.stream(spliterator, false);
    }
}
