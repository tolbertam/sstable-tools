package com.csforge.sstable;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DecoratedKey;

class PagingData {

    final DecoratedKey partitionKey;

    final Clustering clustering;

    final int rowCount;

    public PagingData() {
        this(null, null, 0);
    }

    public PagingData(DecoratedKey partitionKey, Clustering clustering, int rowCount) {
        this.partitionKey = partitionKey;
        this.clustering = clustering;
        this.rowCount = rowCount;
    }

    boolean hasMorePages() {
        return partitionKey != null && clustering != null;
    }

    public DecoratedKey getPartitionKey() {
        return partitionKey;
    }

    public Clustering getClustering() {
        return clustering;
    }

    public Integer getRowCount() {
        return rowCount;
    }
}
