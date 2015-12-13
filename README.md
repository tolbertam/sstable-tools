# SSTable Tools

[![Build Status](https://travis-ci.org/tolbertam/sstable-tools.svg?branch=master)](https://travis-ci.org/tolbertam/sstable-tools)

A toolkit for parsing, creating and doing other fun stuff with Cassandra 3.x SSTables.

**Note**: This project is under heavy development and will likely be broken
in may ways.

**Features:**

* [sstable2json](#sstable2json) - Utility for exporting C\* 3.X SSTables into JSON.

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
TODO: Establish sjk-style usage.  For now you can access this utility via
`com.csforge.sstable.SSTable2Json`.

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
        { "name" : "symbol", "value" : "ZZZ0" },
        { "name" : "year", "value" : "2004" }
      ]
    },
    "rows" : [
      {
        "type" : "row",
        "clustering" : [
          { "name" : "month", "value" : "12" },
          { "name" : "day", "value" : "31" }
        ],
        "cells" : [
          { "name" : "close", "value" : "29.79", "tstamp" : 1449469422019040 },
          { "name" : "high", "value" : "30.02", "tstamp" : 1449469422019040 },
          { "name" : "low", "value" : "29.54", "tstamp" : 1449469422019040 },
          { "name" : "open", "value" : "29.84", "tstamp" : 1449469422019040 },
          { "name" : "volume", "value" : "375700", "tstamp" : 1449469422019040 }
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
