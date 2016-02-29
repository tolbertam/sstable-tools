# SSTable Tools

[![Build Status](https://travis-ci.org/tolbertam/sstable-tools.svg?branch=master)](https://travis-ci.org/tolbertam/sstable-tools)[ ![Download](https://api.bintray.com/packages/tolbertam/sstable-tools/sstable-tools.jar/images/download.svg) ](https://bintray.com/tolbertam/sstable-tools/sstable-tools.jar/_latestVersion)

A toolkit for parsing, creating and doing other fun stuff with Cassandra 3.x SSTables. This project is under development and not yet stable.

Pre compiled binary available from bintray:

* [sstable-tools-3.0.0-alpha3.jar](https://bintray.com/artifact/download/tolbertam/sstable-tools/sstable-tools-3.0.0-alpha3.jar) -  Currently tested with 3.0, 3.1, 3.1.1, 3.2.1, 3.3.

Example usage:

    java -jar sstable-tools.jar cqlsh
    java -jar sstable-tools.jar toJson ma-2-big-Data.db
    java -jar sstable-tools.jar describe ma-2-big-Data.db

Example shell usage:

    java -jar sstable-tools.jar cqlsh

    ## Select one or more sstables (space delimited, or choose directory to include all)
    cqlsh> use ma-2-big-Data.db;
    Using: /home/user/sstable-tools/ma-2-big-Data.db

    ## Use predefined schema file.
    ## Can view with 'schema'.
    ## This is optional but without it the partition
    ## key and clustering index names are unknown.
    cqlsh> schema schema.cql
    Successfully imported schema from '/home/user/sstable-tools/schema.cql'.

    ## Alternatively, use 'CREATE TABLE' statement to enter a schema.
    cqlsh> CREATE TABLE users (
    ...        user_name varchar PRIMARY KEY,
    ...        password varchar,
    ...        gender varchar,
    ...        state varchar,
    ...        birth_year bigint
    ...    );

    ## Discover the data in sstable(s) using CQL queries
    cqlsh> SELECT * FROM sstable WHERE age > 1 LIMIT 1

     ┌────────────┬─────────────┬─────────┬───────────┬────────┐
     │user_name   │birth_year   │gender   │password   │state   │
     ╞════════════╪═════════════╪═════════╪═══════════╪════════╡
     │frodo       │1985         │male     │pass@      │CA      │
     └────────────┴─────────────┴─────────┴───────────┴────────┘

    ## Display raw sstable data (useful to see tombstones and expired ttls)
    ## with optional where clause
    cqlsh> DUMP WHERE age > 1 LIMIT 1

    [frodo] Row[info=[ts=1455937221199050] ]:  | [birth_year=1985 ts=1455937221199050], [gender=male ts=1455937221199050], [password=pass@ ts=1455937221199050], [state=CA ts=1455937221199050]

    ## Describe the sstable data and metadata
    cqlsh> describe sstables;

    /Users/clohfink/git/sstable-tools/./src/test/resources/ma-2-big-Data.db
    =======================================================================
    Partitions: 1
    Rows: 1
    Tombstones: 0
    Cells: 4
    Widest Partitions:
       [frodo] 1
    Largest Partitions:
       [frodo] 104 (104 B)
    Tombstone Leaders:
    Partitioner: org.apache.cassandra.dht.Murmur3Partitioner
    Bloom Filter FP chance: 0.010000
    Size: 50 (50 B)
    Compressor: org.apache.cassandra.io.compress.LZ4Compressor
      Compression ratio: -1.0
    Minimum timestamp: 1455937221199050 (02/19/2016 21:00:21)
    Maximum timestamp: 1455937221199050 (02/19/2016 21:00:21)
    SSTable min local deletion time: 2147483647 (01/18/2038 21:14:07)
    SSTable max local deletion time: 2147483647 (01/18/2038 21:14:07)
    TTL min: 0 (0 milliseconds)
    ...[snip]...

    ## Paging is enabled by default and can be manipulated by using 'PAGING'
    cqlsh> PAGING 20;
    Now Query paging is enabled
    Page size: 20
    cqlsh> PAGING OFF;
    Disabled Query paging.
    cqlsh> PAGING ON;
    Now Query paging is enabled
    Page size: 20

    ## Used sstables, schema, and paging settings and persisted for future use.
    ## Use the 'PERSIST' command to view preferences and to enable/disable
    ## persistence.
    cqlsh> PERSIST;
    Preferences are currently enabled:
    pagingEnabled=true
    pagingSize=20
    preferencesEnabled=true
    schema=""
    sstables=[
        "/home/user/sstable-tools/ma--big-Data.db"
    ]
    cqlsh> PERSIST OFF;
    Disabled Preferences.

**Note:** No environment configuration is necessary for this tool to work if all components of the sstable are available but the cql create statement allows for more details.

**Features:**

* [cqlsh](#cqlsh) - Drop into an interactive shell to make queries against SSTables.
* [describe](#describe) - Describe SSTable data and metadata.
* [sstable2json](#sstable2json) - Utility for exporting C\* 3.X SSTables into JSON.

## Building

This project uses [Apache Maven](https://maven.apache.org/) to build a
self-contained executable jar.  To build the jar simply execute:

```shell
mvn package
```

The executable jar will be present in the target directory.

## cqlsh
cql shell similiar and modeled after the C* cqlsh tool. Enables issuing cql queries against raw sstables and
provides additional diagnostic tools over them. Provides history (reverse searchable with ctrl-r) for ease of use.

```text
Commands:

HELP               - prints this message
EXIT               - leaves the shell
CREATE TABLE ...   - A CREATE TABLE cql statement to use as metadata when reading sstables (HIGHLY RECOMMENDED!)
DESCRIBE SCHEMA    - Show currently used schema (or serialized cfmetadata if generated)
DESCRIBE SSTABLES  - Provide details and statistics on current sstable(s)
PAGING [(ON|OFF)]  - Enables, disables, or shows current status of query paging.
PAGING <SIZE>      - Enables paging and sets paging size.
PERSIST [(ON|OFF)] - Enables, disables, or shows current status of persistence of settings state.
SCHEMA [<FILE>]    - Imports a cql file as the active table schema or shows active user-defined schema.
USE                - update the sstable[s] used by default with select, dump, describe commands
    USE /var/lib/cassandra/data/system/peers/ma-1-big-Data.db
    or with multiple sstables separated with spaces. This can also be a directory which will add all sstables in it.
    USE ma-1-big-Data.db ma-2-big-Data.db "/home/path with space/db/data/sstables"

SELECT             - run a cql query against the current sstable (unless other specified)
    SELECT * FROM sstables WHERE key > 1 LIMIT 10
    the keyword sstables will use the current sstable set with the USE command or set when running cqlsh. You can also
    specify an sstable here
    SELECT avg(someColumn) FROM /var/lib/cassandra/data/system/peers/ma-1-big-Data.db WHERE key > 1

DUMP               - dump the raw unfiltered partitions/rows. Useful for debuging TTLed/tombstoned data.
    DUMP;
    Can also specify a where clause for filtering the results.
    DUMP WHERE partitionKey = 'OpsCenter';
```

## describe

Provides information about an sstable's data and its metadata. Can be used as argument or via cqlsh.
![describe](http://imgur.com/sSZuwDs.gif)
Example Output:

```
/Users/clohfink/git/sstable-tools/ma-119-big-Data.db
====================================================
Partitions: 22515                                                               
Rows: 13579337
Tombstones: 0
Cells: 13579337
Widest Partitions:
   [12345] 999999
   [99049] 62664
   [99007] 60437
   [99017] 59728
   [99010] 59555
Largest Partitions:
   [12345] 189888705 (189.9 MB)
   [99049] 2965017 (3.0 MB)
   [99007] 2860391 (2.9 MB)
   [99017] 2826094 (2.8 MB)
   [99010] 2818038 (2.8 MB)
Tombstone Leaders:
Partitioner: org.apache.cassandra.dht.Murmur3Partitioner
Bloom Filter FP chance: 0.010000
Size: 540932262 (540.9 MB) 
Compressor: org.apache.cassandra.io.compress.LZ4Compressor
  Compression ratio: 0.3068105022732033
Minimum timestamp: 1456554952195298 (02/27/2016 00:35:52)
Maximum timestamp: 1456594562846756 (02/27/2016 11:36:02)
SSTable min local deletion time: 2147483647 (01/18/2038 21:14:07)
SSTable max local deletion time: 2147483647 (01/18/2038 21:14:07)
TTL min: 0 (0 milliseconds)
TTL max: 0 (0 milliseconds)
minClustringValues: [1]
maxClustringValues: [999999]
Estimated droppable tombstones: 0.0
SSTable Level: 0
Repaired at: 0 (12/31/1969 18:00:00)
  ReplayPosition(segmentId=1456414025108, position=16709244)
totalColumnsSet: 13579337
totalRows: 13579337
Estimated tombstone drop times:
  Value                            | Count      %   Histogram 
  2147483647 (01/18/2038 21:14:07) | 27158674 (100) ██████████████████████████████ 
Estimated partition size:
  Value                           | Count   %   Histogram 
  258       (12/31/1969 18:04:18) |     6 (  0)  
  310       (12/31/1969 18:05:10) |    20 (  0) ▏ 
  372       (12/31/1969 18:06:12) |    50 (  0) ▍ 
  446       (12/31/1969 18:07:26) |    45 (  0) ▍ 
  535       (12/31/1969 18:08:55) |    66 (  0) ▌ 
  642       (12/31/1969 18:10:42) |    62 (  0) ▌ 
  770       (12/31/1969 18:12:50) |   120 (  0) █ 
  924       (12/31/1969 18:15:24) |   116 (  0) ▉ 
  1109      (12/31/1969 18:18:29) |   144 (  0) █▏ 
  1331      (12/31/1969 18:22:11) |   175 (  0) █▍ 
  1597      (12/31/1969 18:26:37) |   210 (  0) █▊ 
  1916      (12/31/1969 18:31:56) |   248 (  1) ██ 
  2299      (12/31/1969 18:38:19) |   324 (  1) ██▋ 
  2759      (12/31/1969 18:45:59) |   356 (  1) ███ 
  3311      (12/31/1969 18:55:11) |   460 (  2) ███▉ 
  3973      (12/31/1969 19:06:13) |   534 (  2) ████▌ 
  4768      (12/31/1969 19:19:28) |   569 (  2) ████▊ 
  5722      (12/31/1969 19:35:22) |   667 (  2) █████▋ 
  6866      (12/31/1969 19:54:26) |   786 (  3) ██████▋ 
  8239      (12/31/1969 20:17:19) |  1002 (  4) ████████▍ 
  9887      (12/31/1969 20:44:47) |  1266 (  5) ██████████▋ 
  11864     (12/31/1969 21:17:44) |  1446 (  6) ████████████▏ 
  14237     (12/31/1969 21:57:17) |  1779 (  7) ███████████████ 
  17084     (12/31/1969 22:44:44) |  2081 (  9) █████████████████▌ 
  20501     (12/31/1969 23:41:41) |  2598 ( 11) █████████████████████▉ 
  24601     (01/01/1970 00:50:01) |  3089 ( 13) ██████████████████████████ 
  29521     (01/01/1970 02:12:01) |  3555 ( 15) ██████████████████████████████ 
  35425     (01/01/1970 03:50:25) |   681 (  3) █████▋
  1629722   (01/19/1970 14:42:02) |     4 (  0)  
  186563160 (11/30/1975 01:06:00) |     1 (  0)  
Estimated column count:
  Value                         | Count   %   Histogram 
  10      (12/31/1969 18:00:10) |    41 (  0) ▎ 
  12      (12/31/1969 18:00:12) |    39 (  0) ▎ 
  14      (12/31/1969 18:00:14) |    32 (  0) ▎ 
  17      (12/31/1969 18:00:17) |    63 (  0) ▌ 
  20      (12/31/1969 18:00:20) |    64 (  0) ▌ 
  24      (12/31/1969 18:00:24) |   102 (  0) ▉ 
  29      (12/31/1969 18:00:29) |   120 (  0) █ 
  35      (12/31/1969 18:00:35) |   123 (  0) █ 
  42      (12/31/1969 18:00:42) |   165 (  0) █▍ 
  50      (12/31/1969 18:00:50) |   189 (  0) █▋ 
  60      (12/31/1969 18:01:00) |   220 (  0) █▉ 
  72      (12/31/1969 18:01:12) |   313 (  1) ██▋ 
  86      (12/31/1969 18:01:26) |   326 (  1) ██▊ 
  103     (12/31/1969 18:01:43) |   429 (  1) ███▋ 
  124     (12/31/1969 18:02:04) |   493 (  2) ████▎ 
  149     (12/31/1969 18:02:29) |   566 (  2) ████▉ 
  179     (12/31/1969 18:02:59) |   643 (  2) █████▌ 
  215     (12/31/1969 18:03:35) |   765 (  3) ██████▋ 
  258     (12/31/1969 18:04:18) |   949 (  4) ████████▏ 
  310     (12/31/1969 18:05:10) |  1197 (  5) ██████████▍ 
  372     (12/31/1969 18:06:12) |  1382 (  6) ████████████ 
  446     (12/31/1969 18:07:26) |  1722 (  7) ██████████████▉ 
  535     (12/31/1969 18:08:55) |  1971 (  8) █████████████████ 
  642     (12/31/1969 18:10:42) |  2469 ( 10) █████████████████████▍ 
  770     (12/31/1969 18:12:50) |  2908 ( 12) █████████████████████████▎ 
  924     (12/31/1969 18:15:24) |  3455 ( 15) ██████████████████████████████ 
  1109    (12/31/1969 18:18:29) |  1708 (  7) ██████████████▊ 
  1131752 (01/13/1970 20:22:32) |     1 (  0)  
Estimated cardinality: 22873
EncodingStats minTTL: 0 (0 milliseconds)
EncodingStats minLocalDeletionTime: 1442880000 (01/17/1970 10:48:00)
EncodingStats minTimestamp: 1456554952195298 (02/27/2016 00:35:52)
KeyType: org.apache.cassandra.db.marshal.UTF8Type
ClusteringTypes: [org.apache.cassandra.db.marshal.UTF8Type]
StaticColumns: {}
RegularColumns: {val:org.apache.cassandra.db.marshal.UTF8Type}
```

### Usage

```
java -jar sstable-tools.jar describe /path/to/file.db
```

## sstable2json

sstable2json is a utility in the spirit of the [original sstable2json](https://docs.datastax.com/en/cassandra/1.2/cassandra/tools/toolsSstable2JsonUtilsTOC.html)
which was previously deprecated ([CASSANDRA-9618](https://issues.apache.org/jira/browse/CASSANDRA-9618)).
This functionality was merged into cassandra by ([CASSANDRA-7464](https://issues.apache.org/jira/browse/CASSANDRA-7464)) and to be released in Cassandra 3.0.4 and 3.4.  This will likely be removed from sstable-tools in a future release.

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
[tombstones](http://docs.datastax.com/en/cassandra/3.x/cassandra/dml/dmlAboutDeletes.html)
which represent deletes.  In Cassandra 3.0, users can now delete ranges of
rows ([CASSANDRA-6237](https://issues.apache.org/jira/browse/CASSANDRA-6237))
which creates range tombstones.

```json
TODO: update with loss of cql create option
```
