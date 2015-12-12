package com.csforge.reader;

import org.apache.cassandra.db.rows.Unfiltered;

import java.util.stream.Stream;

/**
 */
public interface Partition {

    /**
     * A timestamp (typically in microseconds since the unix epoch, although this is not enforced) after which
     * data should be considered deleted. If set to Long.MIN_VALUE, this implies that the data has not been marked
     * for deletion at all.
     */
    public long markedForDeleteAt();

    /**
     * The local server timestamp, in seconds since the unix epoch, at which this tombstone was created. This is
     * only used for purposes of purging the tombstone after gc_grace_seconds have elapsed.
     */
    public int localDeletionTime();

    public Unfiltered staticRow();

    public Stream<Unfiltered> rows();
}
