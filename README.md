# SSTable Tools

[![Build Status](https://travis-ci.org/tolbertam/sstable-tools.svg?branch=master)](https://travis-ci.org/tolbertam/sstable-tools)

A toolkit for parsing, creating and doing other fun stuff with Cassandra 3.x SSTables.

**Note**: This project is under heavy development and will likely be broken
in many ways.

**Features:**

* [sstable2json](#sstable2json) - Utility for exporting C\* 3.X SSTables into JSON.

## Building

This project uses [Apache Maven](https://maven.apache.org/) to build a
self-contained executable jar.  To build the jar simply execute:

```shell
mvn package
```

The executable jar will be present in the target directory.

## sstable2json

sstable2json is a utility in the spirit of the [original sstable2json](https://docs.datastax.com/en/cassandra/1.2/cassandra/tools/toolsSstable2JsonUtilsTOC.html)
which has since been deprecated ([CASSANDRA-9618](https://issues.apache.org/jira/browse/CASSANDRA-9618))
and has since been entirely removed with plans to add a replacement ([CASSANDRA-7464](https://issues.apache.org/jira/browse/CASSANDRA-7464)).

A key differentiator between the storage format between older verisons of
Cassandra and Cassandra 3.0 is that an SSTable was previously a representation 
of partitions and their cells (identified by their clustering and column
name) whereas with Cassandra 3.0 an SSTable now represents partitions and their
rows.  You can read about these changes in more detail by visiting
[this blog post](http://www.datastax.com/2015/12/storage-engine-30).

Since data is now organized to map better to the CQL data model, understanding
the layout of data is now easier to grasp and thus the output of this tool
should be much more pleasant to follow.

### Usage

```
java -jar sstable-tools.jar toJson

usage: toJson <sstable> [-c <arg>] [-e] [-k <arg>] [-x <arg>]

Converts SSTable into a JSON formatted document.
  -c <arg> Optional file containing "CREATE TABLE..." for the sstable's schema.  Used to determine the partition and
           clustering key names. Must not include "keyspace." in create statement.  If not included will not print key names.
  -e       Only print out the keys for the sstable.  If enabled other options are ignored.
  -k <arg> Partition key to be included.  May be used multiple times.  If not set will default to all keys.
  -x <arg> Partition key to be excluded.  May be used multiple times.
```

### Examples

An example showing an SSTable with 2 partitions, each having their own
series of rows with columns:

```json
[
  {
    "partition" : {
      "key" : [
        { "name" : "symbol", "value" : "YYY0" },
        { "name" : "year", "value" : "1994" }
      ]
    },
    "rows" : [
      {
        "type" : "row",
        "clustering" : [
          { "name" : "month", "value" : "12" },
          { "name" : "day", "value" : "30" }
        ],
        "cells" : [
          { "name" : "close", "value" : "11.5", "tstamp" : 1449469421746061 },
          { "name" : "open", "value" : "12.5", "tstamp" : 1449469421746061 },
          { "name" : "volume", "value" : "63100", "tstamp" : 1449469421746061 }
        ]
      },
      {
        "type" : "row",
        "clustering" : [
          { "name" : "month", "value" : "12" },
          { "name" : "day", "value" : "29" }
        ],
        "cells" : [
          { "name" : "close", "value" : "12.75", "tstamp" : 1449469421746060 },
          { "name" : "high", "value" : "13.0", "tstamp" : 1449469421746060 },
          { "name" : "low", "value" : "12.5", "tstamp" : 1449469421746060 },
          { "name" : "open", "value" : "12.5", "tstamp" : 1449469421746060 },
          { "name" : "volume", "value" : "27500", "tstamp" : 1449469421746060 }
        ]
      }
    ]
  },
  {
    "partition" : {
      "key" : [
        { "name" : "symbol", "value" : "AAAA" },
        { "name" : "year", "value" : "2016" }
      ]
    },
    "rows" : [
      {
        "type" : "row",
        "clustering" : [
          { "name" : "month", "value" : "12" },
          { "name" : "day", "value" : "13" }
        ],
        "liveness_info" : { "tstamp" : 1450034993891342, "ttl" : 10000, "expires_at" : 1450044993, "expired" : false },
        "cells" : [
          { "name" : "close", "value" : "100.0" },
          { "name" : "high", "value" : "101.5" },
          { "name" : "low", "value" : "90.2" },
          { "name" : "open", "value" : "96.72", "tstamp" : 1450034999438049, "ttl" : 300, "expires_at" : 1450035299, "expired" : true },
          { "name" : "volume", "value" : "49554" }
        ]
      }
    ]
  }
]
```

An example demonstrating what output will look like if you do not provide a CQL
schema via `-c`.  Partition and Clustering key column names are simply omitted
and only their values are provided:


```json
[
  {
    "partition" : {
      "key" : [ "ZLC", "2003" ]
    },
    "rows" : [
      {
        "type" : "row",
        "clustering" : [ "12", "31" ],
        "cells" : [
          { "name" : "close", "value" : "53.2", "tstamp" : 1449469421557041 },
          { "name" : "high", "value" : "53.48", "tstamp" : 1449469421557041 },
          { "name" : "low", "value" : "52.56", "tstamp" : 1449469421557041 },
          { "name" : "open", "value" : "53.35", "tstamp" : 1449469421557041 },
          { "name" : "volume", "value" : "93600", "tstamp" : 1449469421557041 }
        ]
      }
    ]
  }
]
```


An example showing an SSTable with tombstones at all levels:

A partition, its rows and its columns can also have
[tombstones](http://docs.datastax.com/en/cassandra/2.0/cassandra/dml/dml_about_deletes_c.html)
which represent deletes.  In Cassandra 3.0, users can now delete ranges of 
rows ([CASSANDRA-6237](https://issues.apache.org/jira/browse/CASSANDRA-6237))
which creates range tombstones.


```json
[
  {
    "partition" : {
      "key" : [
        { "name" : "symbol", "value" : "CORP" },
        { "name" : "year", "value" : "2006" }
      ],
      "deletion_info" : { "deletion_time" : 1449967308857460, "tstamp" : 1449967308 }
    }
  },
  {
    "partition" : {
      "key" : [
        { "name" : "symbol", "value" : "CORP" },
        { "name" : "year", "value" : "2004" }
      ]
    },
    "rows" : [
      {
        "type" : "range_tombstone_bound",
        "start" : {
          "type" : "inclusive",
          "clustering" : [
            { "name" : "month", "value" : "4" },
            { "name" : "day", "value" : "*" }
          ],
          "deletion_info" : { "deletion_time" : 1449958944345632, "tstamp" : 1449958944 }
        }
      },
      {
        "type" : "range_tombstone_bound",
        "end" : {
          "type" : "inclusive",
          "clustering" : [
            { "name" : "month", "value" : "4" },
            { "name" : "day", "value" : "*" }
          ],
          "deletion_info" : { "deletion_time" : 1449958944345632, "tstamp" : 1449958944 }
        }
      },
      {
        "type" : "row",
        "clustering" : [
          { "name" : "month", "value" : "3" },
          { "name" : "day", "value" : "16" }
        ],
        "cells" : [
          { "name" : "high", "value" : "100.0", "ttl" : 144000, "deletion_time" : 1450103151, "expired" : false, "tstamp" : 1449959151499921 }
        ]
      },
      {
        "type" : "row",
        "clustering" : [
          { "name" : "month", "value" : "3" },
          { "name" : "day", "value" : "14" }
        ],
        "cells" : [
          { "name" : "open", "deletion_time" : 1449958979, "tstamp" : 1449958979852772 }
        ]
      },
      {
        "type" : "row",
        "clustering" : [
          { "name" : "month", "value" : "3" },
          { "name" : "day", "value" : "12" }
        ],
        "deletion_info" : { "deletion_time" : 1449958958765226, "tstamp" : 1449958958 }
      },
      {
        "type" : "range_tombstone_bound",
        "start" : {
          "type" : "exclusive",
          "clustering" : [
            { "name" : "month", "value" : "2" },
            { "name" : "day", "value" : "12" }
          ],
          "deletion_info" : { "deletion_time" : 1449961223094378, "tstamp" : 1449961223 }
        }
      },
      {
        "type" : "range_tombstone_boundary",
        "start" : {
          "type" : "exclusive",
          "clustering" : [
            { "name" : "month", "value" : "2" },
            { "name" : "day", "value" : "7" }
          ],
          "deletion_info" : { "deletion_time" : 1449961214148839, "tstamp" : 1449961214 }
        },
        "end" : {
          "type" : "inclusive",
          "clustering" : [
            { "name" : "month", "value" : "2" },
            { "name" : "day", "value" : "7" }
          ],
          "deletion_info" : { "deletion_time" : 1449961223094378, "tstamp" : 1449961223 }
        }
      },
      {
        "type" : "range_tombstone_bound",
        "end" : {
          "type" : "exclusive",
          "clustering" : [
            { "name" : "month", "value" : "2" },
            { "name" : "day", "value" : "3" }
          ],
          "deletion_info" : { "deletion_time" : 1449961214148839, "tstamp" : 1449961214 }
        }
      }
    ]
  }
]
```
