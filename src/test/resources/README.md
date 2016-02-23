# Description of SSTables in test/resources

## 1

    CREATE TABLE composites (
        key1 varchar,
        key2 varchar,
        ckey1 varchar,
        ckey2 varchar,
        value bigint,
        PRIMARY KEY((key1, key2), ckey1, ckey2)
    );

2 partitions

## 2

    CREATE TABLE users (
        user_name varchar PRIMARY KEY,
        password varchar,
        gender varchar,
        state varchar,
        birth_year bigint
    );

1 partition

[
  {
    "partition" : {
      "key" : [ "frodo" ]
    },
    "rows" : [
      {
        "type" : "row",
        "liveness_info" : { "tstamp" : 1455937221199050 },
        "cells" : [
          { "name" : "birth_year", "value" : "1985" },
          { "name" : "gender", "value" : "male" },
          { "name" : "password", "value" : "pass@" },
          { "name" : "state", "value" : "CA" }
        ]
      }
    ]
  }
]

## 3

    CREATE TABLE IF NOT EXISTS test.wide ( key text, key2 text, val text, PRIMARY KEY (key, key2));

4 partitions, each with 9 rows, clustered 1-9 with val = "X"

## 4

    CREATE TABLE collections (key1 varchar, listval list<text>, mapval map<text, text>, setval set<text>, PRIMARY KEY (key1))

    insert into collections (key1, listval, mapval, setval) VALUES ( '1', ['1'], {'mapkey': 'mapvalue'}, {'set'}) ;