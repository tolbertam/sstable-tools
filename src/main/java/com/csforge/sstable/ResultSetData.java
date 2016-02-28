package com.csforge.sstable;

import org.apache.cassandra.cql3.ResultSet;

public class ResultSetData {

    private final ResultSet resultSet;

    private final PagingData pagingData;

    public ResultSetData(ResultSet resultSet, PagingData pagingData) {
        this.resultSet = resultSet;
        this.pagingData = pagingData;
    }

    public ResultSet getResultSet() {
        return resultSet;
    }

    public PagingData getPagingData() {
        return pagingData;
    }
}
