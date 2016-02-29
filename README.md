# SSTable Tools

[![Build Status](https://travis-ci.org/tolbertam/sstable-tools.svg?branch=master)](https://travis-ci.org/tolbertam/sstable-tools)[ ![Download](https://api.bintray.com/packages/tolbertam/sstable-tools/sstable-tools.jar/images/download.svg) ](https://bintray.com/tolbertam/sstable-tools/sstable-tools.jar/_latestVersion)

A toolkit for parsing, creating and doing other fun stuff with Cassandra 3.x SSTables. This project is under heavy development and not yet stable.

Pre compiled binary available from bintray:

* [sstable-tools-3.0.0-alpha2.jar](https://bintray.com/artifact/download/tolbertam/sstable-tools/sstable-tools-3.0.0-alpha2.jar) -  Currently tested with 3.0, 3.1, 3.1.1, 3.2.1, 3.3.

Example usage:

    java -jar sstable-tools-3.0.0-alpha2.jar toJson ma-2-big-Data.db
    java -jar sstable-tools-3.0.0-alpha2.jar describe ma-2-big-Data.db
    java -jar sstable-tools-3.0.0-alpha3.jar SELECT count(*) FROM ma-2-big-Data.db WHERE age > 30  (TODO: Escape this as it doesnt work as is.)
    
Example shell usage:

    java -jar sstable-tools-3.0.0-alpha2.jar cqlsh
    
    ## Select one or more (space delimited, or choose directory to include all)
    cqlsh> use ma-2-big-Data.db;
    
    ## Use create table statement to set the schema (can view with 'describe schema'). This is optional but without
    ## it the partition key and clustering index names are unknown.
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
    
    ## Display raw sstable data (useful to see tombstones and expired ttls) with optional where clause
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
    
>>>>>>> update readme a little
**Note:** No environment configuration is necessary for this tool to work if all components of the sstable are available but the cql create statement allows for more details.

**Note:** CQL statements require bash escaping when using the "select" command via command line ```java -jar sstable-tools.jar select \* from \"path\"```


see more below

**Features:**

* [sstable2json](#sstable2json) - Utility for exporting C\* 3.X SSTables into JSON.
* [select](#select) - Make CQL queries against SSTables

## Building

This project uses [Apache Maven](https://maven.apache.org/) to build a
self-contained executable jar.  To build the jar simply execute:

```shell
mvn package
```

The executable jar will be present in the target directory.

## cqlsh
cql shell similiar and modeled after the C* cqlsh tool. This will allow issuing cql queries against raw sstables and
providing additional diagnostic tools over them. Provides history (reverse searchable with ctrl-r) for ease of use.

```text
EXIT              - leaves the shell (also ctrl-d on prompt, ctrl-c to break back to blank prompt)
CREATE TABLE ...  - A CREATE TABLE cql statement to use as metadata when reading sstables (HIGHLY RECOMMENDED!)
DESCRIBE SCHEMA   - Show currently used schema (or serialized cfmetadata if generated)
DESCRIBE SSTABLES - Provide details and statistics on current sstable(s)
USE               - update the sstable[s] used by default with select, dump, describe commands
    USE /var/lib/cassandra/data/system/peers/ma-1-big-Data.db
    or with multiple sstables separated with spaces. This can also be a directory which will add all sstables in it.
    USE ma-1-big-Data.db ma-2-big-Data.db "/home/path with space/db/data/sstables"

SELECT            - run a cql query against the current sstable (unless other specified)
    SELECT * FROM sstables WHERE key > 1 LIMIT 10
    the keyword sstables will use the current sstable set with the USE command or set when running cqlsh. You can also
    specify an sstable here
    SELECT avg(someColumn) FROM /var/lib/cassandra/data/system/peers/ma-1-big-Data.db WHERE key > 1

DUMP              - dump the raw unfiltered partitions/rows. Useful for debuging TTLed/tombstoned data.
    DUMP;
    Can also specify a where clause for filtering the results.
    DUMP WHERE partitionKey = 'OpsCenter';
```

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
[tombstones](http://docs.datastax.com/en/cassandra/3.x/cassandra/dml/dmlAboutDeletes.html)
which represent deletes.  In Cassandra 3.0, users can now delete ranges of
rows ([CASSANDRA-6237](https://issues.apache.org/jira/browse/CASSANDRA-6237))
which creates range tombstones.


```json
TODO: update with loss of cql create option
```

## select

Use the CQL parser to query the sstables directly without Cassandra or any configuration. Does not currently support ORDER_BY and DISTINCT but all other features should work. It will search the classpath for a ```schema.cql``` (override with ```-Dsstabletools.schema=...```) that contains the CQL ```CREATE TABLE``` statement for the schema. If it cannot find one it will fall back to a best guess from the sstable metadata. The data is dumped as a result set in a data table, but to see the raw data set the ```-Dquery.toJson``` for the alternative output.

**WARNING:** without the schema the queries become difficult for any partition/clustering columns as their names are not included in meta data yet (CASSANDRA-9587)

### Usage

```
java -jar sstable-tools.jar SELECT ...

usage: SELECT <column selection> FROM <sstable> WHERE <where clause>
```


### Examples

With a schema.cql file:
```CQL
CREATE TABLE mykeyspace.users (
    user_name varchar PRIMARY KEY,
    password varchar,
    gender varchar,
    state varchar,
    birth_year bigint
);
```

An example that selects users by birth year and state

```json
java -jar sstable-tools.jar SELECT * FROM ma-2-big-Data.db WHERE birth_year >= 1985 AND state = 'CA'

 ┌────────────┬─────────────┬─────────┬───────────┬────────┐
 │user_name   │birth_year   │gender   │password   │state   │
 ╞════════════╪═════════════╪═════════╪═══════════╪════════╡
 │frodo       │1985         │male     │pass@      │CA      │
 └────────────┴─────────────┴─────────┴───────────┴────────┘
```

Another example is given the table
```cql
CREATE TABLE IF NOT EXISTS test.wide ( key text, key2 text, val text, PRIMARY KEY (key, key2));
```
With four partitions, each with 9 rows, key2 1-9 all with the val = "X"
```
java -jar sstable-tools.jar SELECT * FROM ma-3-big-Data.db 
 ┌──────┬───────┬──────┐
 │key   │key2   │val   │
 ╞══════╪═══════╪══════╡
 │4     │1      │X     │
 ├──────┼───────┼──────┤
 │4     │2      │X     │
 ├──────┼───────┼──────┤
 │4     │3      │X     │
 ├──────┼───────┼──────┤
 │4     │4      │X     │
 ├──────┼───────┼──────┤
...

 │1     │7      │X     │
 ├──────┼───────┼──────┤
 │1     │8      │X     │
 ├──────┼───────┼──────┤
 │1     │9      │X     │
 └──────┴───────┴──────┘
```
You can perform aggregates

```
java -jar sstable-tools.jar SELECT count(*) FROM ma-3-big-Data.db 
 ┌────────┐
 │count   │
 ╞════════╡
 │36      │
 └────────┘
 
 java -jar sstable-tools.jar SELECT min(key2), max(key2) FROM ma-3-big-Data.db 
 ┌───────────────────┬───────────────────┐
 │system.min(key2)   │system.max(key2)   │
 ╞═══════════════════╪═══════════════════╡
 │1                  │9                  │
 └───────────────────┴───────────────────┘
```

To see unfiltered data (useful for tombstone debugging) use the raw json format
```
java -jar -Dquery.toJson=true sstable-tools.jar SELECT * FROM ma-3-big-Data.db WHERE key2 = '1'
[
  {
    "partition" : {
      "key" : [ "4" ]
    },
    "rows" : [
      {
        "type" : "row",
        "clustering" : [ "1" ],
        "liveness_info" : { "tstamp" : 1456111364877667 },
        "cells" : [
          { "name" : "val", "value" : "X" }
        ]
      }
    ]
  },
  {
    "partition" : {
      "key" : [ "3" ]
    },
    "rows" : [
      {
        "type" : "row",
        "clustering" : [ "1" ],
        "liveness_info" : { "tstamp" : 1456111364856446 },
        "cells" : [
          { "name" : "val", "value" : "X" }
        ]
      }
    ]
  },
  {
    "partition" : {
      "key" : [ "2" ]
    },
    "rows" : [
      {
        "type" : "row",
        "clustering" : [ "1" ],
        "liveness_info" : { "tstamp" : 1456111364834000 },
        "cells" : [
          { "name" : "val", "value" : "X" }
        ]
      }
    ]
  },
  {
    "partition" : {
      "key" : [ "1" ]
    },
    "rows" : [
      {
        "type" : "row",
        "clustering" : [ "1" ],
        "liveness_info" : { "tstamp" : 1456111364803946 },
        "cells" : [
          { "name" : "val", "value" : "X" }
        ]
      }
    ]
  }
]
```
