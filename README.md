# SSTable Tools

[![Build Status](https://travis-ci.org/tolbertam/sstable-tools.svg?branch=master)](https://travis-ci.org/tolbertam/sstable-tools)[ ![Download](https://api.bintray.com/packages/tolbertam/sstable-tools/sstable-tools.jar/images/download.svg) ](https://bintray.com/tolbertam/sstable-tools/sstable-tools.jar/_latestVersion)

A toolkit for parsing, creating and doing other fun stuff with Cassandra 3.x SSTables. This project is under development and not yet stable. Mainly a proof of concept playground for wish items.

Pre compiled binary available from bintray:

* [sstable-tools-3.0.0-alpha6.jar](https://bintray.com/artifact/download/tolbertam/sstable-tools/sstable-tools-3.0.0-alpha6.jar) -  Supports 3.0 to 3.7 (ma and mb sstable formats)

**Note:** This tool formerly included a `tojson` command which dumped SSTable contents to JSON.  This functionality has
since been merged into Cassandra starting with versions 3.0.4 and 3.4 as the `sstabledump` command.

Example usage:

    java -jar sstable-tools.jar cqlsh
    java -jar sstable-tools.jar hints 1458779867606-1.hints
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
* [hints](#hints) - Dump hints from a hint file

## Building

This project uses [Apache Maven](https://maven.apache.org/) to build a
self-contained executable jar.  To build the jar simply execute:

```shell
mvn package
```

The executable jar will be present in the target directory.

## cqlsh
cql shell similiar and modeled after the C* cqlsh tool. Enables issuing cql queries against raw sstables and
provides additional diagnostic tools over them. Provides history (reverse searchable with ctrl-r) and autocomplete for ease of use.

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

![cql example](http://imgur.com/YXyjffj.gif)
(note: slow analyzing artifact of ttygif, describing sstable in this scenario is sub second.)

## describe

Provides information about an sstable's data and its metadata. Can be used as argument or via cqlsh.

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
  Lower bound: ReplayPosition(segmentId=-1, position=0)
  Upper bound: ReplayPosition(segmentId=1456414025108, position=16709244)
totalColumnsSet: 13579337
totalRows: 13579337
Estimated tombstone drop times:
  Value                            | Count      %   Histogram 
  2147483647 (01/18/2038 21:14:07) | 27158674 (100) ▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉ 
Estimated partition size:
  Value     | Count   %   Histogram 
  258       |     6 (  0)  
  310       |    20 (  0) ▏ 
  372       |    50 (  0) ▍ 
  446       |    45 (  0) ▍ 
  535       |    66 (  0) ▍ 
  642       |    62 (  0) ▍ 
  770       |   120 (  0) ▉ 
  924       |   116 (  0) ▉ 
  1109      |   144 (  0) ▉▏ 
  1331      |   175 (  0) ▉▍ 
  1597      |   210 (  0) ▉▊ 
  1916      |   248 (  1) ▉▉ 
  2299      |   324 (  1) ▉▉▋ 
  2759      |   356 (  1) ▉▉▉ 
  3311      |   460 (  2) ▉▉▉▉ 
  3973      |   534 (  2) ▉▉▉▉▍ 
  4768      |   569 (  2) ▉▉▉▉▊ 
  5722      |   667 (  2) ▉▉▉▉▉▋ 
  6866      |   786 (  3) ▉▉▉▉▉▉▋ 
  8239      |  1002 (  4) ▉▉▉▉▉▉▉▉▍ 
  9887      |  1266 (  5) ▉▉▉▉▉▉▉▉▉▉▋ 
  11864     |  1446 (  6) ▉▉▉▉▉▉▉▉▉▉▉▉▏ 
  14237     |  1779 (  7) ▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉ 
  17084     |  2081 (  9) ▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▍ 
  20501     |  2598 ( 11) ▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉ 
  24601     |  3089 ( 13) ▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉ 
  29521     |  3555 ( 15) ▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉ 
  35425     |   681 (  3) ▉▉▉▉▉▋ 
  186563160 |     1 (  0)  
Estimated column count:
  Value   | Count   %   Histogram 
  10      |    41 (  0) ▎ 
  12      |    39 (  0) ▎ 
  14      |    32 (  0) ▎ 
  17      |    63 (  0) ▍ 
  20      |    64 (  0) ▍ 
  24      |   102 (  0) ▉ 
  29      |   120 (  0) ▉ 
  35      |   123 (  0) ▉ 
  42      |   165 (  0) ▉▍ 
  50      |   189 (  0) ▉▋ 
  60      |   220 (  0) ▉▉ 
  72      |   313 (  1) ▉▉▋ 
  86      |   326 (  1) ▉▉▊ 
  103     |   429 (  1) ▉▉▉▋ 
  124     |   493 (  2) ▉▉▉▉▎ 
  149     |   566 (  2) ▉▉▉▉▉ 
  179     |   643 (  2) ▉▉▉▉▉▍ 
  215     |   765 (  3) ▉▉▉▉▉▉▋ 
  258     |   949 (  4) ▉▉▉▉▉▉▉▉▏ 
  310     |  1197 (  5) ▉▉▉▉▉▉▉▉▉▉▍ 
  372     |  1382 (  6) ▉▉▉▉▉▉▉▉▉▉▉▉ 
  446     |  1722 (  7) ▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉ 
  535     |  1971 (  8) ▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉ 
  642     |  2469 ( 10) ▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▍ 
  770     |  2908 ( 12) ▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▎ 
  924     |  3455 ( 15) ▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉▉ 
  1109    |  1708 (  7) ▉▉▉▉▉▉▉▉▉▉▉▉▉▉▊ 
  1131752 |     1 (  0)  
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

## hints

Deserialize the mutations in a hints file and print them to standard out. To have the information necessary do deserialize
the mutations this tool requires the schema of the file. This is currently handled by connecting to the cluster and querying
the metadata. You can specify the host and (cql) port via the `-h` and `-p` options.

Example Output:

```
java -jar sstable-tools.jar hints 1458786695234-1.hints
Loading schema from 127.0.0.1:9042
/Users/clohfink/1458786695234-1.hints
=====================================
[test.t1] key=1 columns=[[] | [val]]
    Row: EMPTY | val=1
[[val=1 ts=1458786688691698]]
[test.wide] key=myPartitionKey columns=[[] | [val]]
    Row: key2=myClusteringKey | val=myValue
[[val=myValue ts=1458790233298650]]
[test.wide] key=myPartitionKey columns=[[] | [val]]
    Row: key2=myClusteringKey2 | val=myValue2
[[val=myValue2 ts=1458790238249336]]

```

### Usage

```
java -jar sstable-tools.jar hints

usage: hints [-h <arg>] [-p <arg>] [-s] hintfile [hintfile ...]

Hint Dump for Apache Cassandra 3.x
Options:
  -h <arg> Host to extract schema frome.
  -p <arg> CQL native port.
  -s       Only output mutations.
  hintfile at least one file containing hints

```
