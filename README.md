# SSTable Tools

[![Build Status](https://travis-ci.org/tolbertam/sstable-tools.svg?branch=master)](https://travis-ci.org/tolbertam/sstable-tools)[ ![Download](https://api.bintray.com/packages/tolbertam/sstable-tools/sstable-tools.jar/images/download.svg) ](https://bintray.com/tolbertam/sstable-tools/sstable-tools.jar/_latestVersion)

A toolkit for parsing, creating and doing other fun stuff with Cassandra 3.x SSTables. This project is under heavy development and not yet stable.

Pre compiled binary available from bintray:

* [sstable-tools-3.0.0-alpha2.jar](https://bintray.com/artifact/download/tolbertam/sstable-tools/sstable-tools-3.0.0-alpha2.jar) -  Currently tested with 3.0, 3.1, 3.1.1, 3.2.1.

Example Usage:

    java -jar sstable-tools-3.0.0-alpha2.jar toJson ma-2-big-Data.db

**Note:** No environment configuration is necessary for this tool to work if all components of the sstable are available but the cql create statement allows for more details.

see more below

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

Additional improvements over the sstable2json tool includes no longer requiring the cassandra.yaml in classpath with the schema of the sstables loaded. Also by running in client mode this tool will not write to system tables or your commit log. It can safely be run as any user anywhere with no side effects.

Since data is now organized to map better to the CQL data model, understanding
the layout of data is now easier to grasp and thus the output of this tool
should be much more pleasant to follow.

### Usage

```
java -jar sstable-tools.jar toJson

usage: toJson <sstable> [-c] [-e] [-k <arg>] [-x <arg>]

Converts SSTable into a JSON formatted document.
  -c       Print one partition per line.
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
      "key" : [ "YYY0", "1994" ]
    },
    "rows" : [
      {
        "type" : "row",
        "clustering" : [ "12", "30" ],
        "cells" : [
          { "name" : "close", "value" : "11.5", "tstamp" : 1449469421746061 },
          { "name" : "open", "value" : "12.5", "tstamp" : 1449469421746061 },
          { "name" : "volume", "value" : "63100", "tstamp" : 1449469421746061 }
        ]
      },
      {
        "type" : "row",
        "clustering" : [ "12", "29" ],
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
      "key" : [ "AAAA", "2016"]
    },
    "rows" : [
      {
        "type" : "row",
        "clustering" : [ "12", "13" ],
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

An example showing an SSTable with tombstones at all levels:

A partition, its rows and its columns can also have
[tombstones](http://docs.datastax.com/en/cassandra/2.0/cassandra/dml/dml_about_deletes_c.html)
which represent deletes.  In Cassandra 3.0, users can now delete ranges of
rows ([CASSANDRA-6237](https://issues.apache.org/jira/browse/CASSANDRA-6237))
which creates range tombstones.


```json
TODO - update with loss of cql create option
```
