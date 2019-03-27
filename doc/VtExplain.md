# VTExplain Tool

The vtexplain tool provides information about how Vitess will execute a statement (the Vitess version of MySQL `EXPLAIN`).

## Prerequisites

You'll need to build the `vtexplain` binary in your environment.
To find instructions on how to build this binary please refer to this [guide](https://vitess.io/docs/tutorials/local/). 

## Explaining a Query

In order to explain a query you will need to first collect a sql schema for the various tables and a vschema json file containing a map of keyspace to the set of vindexes / tables in the vschema.

For example, let's use the following:

Schema:

```SQL
CREATE TABLE users(
  user_id bigint,
  name varchar(128),
  primary key(user_id)
);

CREATE TABLE users_name_idx(
  user_id bigint,
  name varchar(128),
  primary key(name, user_id)
);
```

VSchema:

```json
{
  "mainkeyspace": {
    "sharded": true,
    "vindexes": {
      "hash": {
        "type": "hash"
      },
      "md5": {
        "type": "unicode_loose_md5",
        "params": {},
        "owner": ""
      },
      "users_name_idx": {
        "type": "lookup_hash",
        "params": {
          "from": "name",
          "table": "users_name_idx",
          "to": "user_id"
        },
        "owner": "users"
      }
    },
    "tables": {
      "users": {
        "column_vindexes": [
          {
            "column": "user_id",
            "name": "hash"
          },
          {
            "column": "name",
            "name": "users_name_idx"
          }
        ],
        "auto_increment": null
      },
      "users_name_idx": {
        "type": "",
        "column_vindexes": [
          {
            "column": "name",
            "name": "md5"
          }
        ],
        "auto_increment": null
      }
    }
  }
}
```

These can be passed to vtexplain either on the command line or (more readily) through files.

Then you can use the tool like this:

**Select:**

```bash
vtexplain -shards 8 -vschema-file /tmp/vschema.json -schema-file /tmp/schema.sql -replication-mode "ROW" -output-mode text -sql "SELECT * from users"
----------------------------------------------------------------------
SELECT * from users

1 mainkeyspace/-20: select * from users limit 10001
1 mainkeyspace/20-40: select * from users limit 10001
1 mainkeyspace/40-60: select * from users limit 10001
1 mainkeyspace/60-80: select * from users limit 10001
1 mainkeyspace/80-a0: select * from users limit 10001
1 mainkeyspace/a0-c0: select * from users limit 10001
1 mainkeyspace/c0-e0: select * from users limit 10001
1 mainkeyspace/e0-: select * from users limit 10001

----------------------------------------------------------------------
```

The output shows the sequence of queries run.

In this case, the query planner is a scatter query to all shards, and each line shows:

(a) the logical sequence of the query
(b) the keyspace/shard
(c) the query that was executed

The fact that each query runs at time `1` shows that vitess executes these in parallel, and the `limit 10001` is automatically added as a protection against large results.

**Insert:**

```bash
vtexplain -shards 128 -vschema-file /tmp/vschema.json -schema-file /tmp/schema.sql -replication-mode "ROW" -output-mode text -sql "INSERT INTO users (user_id, name) VALUES(1, 'john')"

----------------------------------------------------------------------
INSERT INTO users (user_id, name) VALUES(1, 'john')

1 mainkeyspace/22-24: begin
1 mainkeyspace/22-24: insert into users_name_idx(name, user_id) values ('john', 1) /* vtgate:: keyspace_id:22c0c31d7a0b489a16332a5b32b028bc */
2 mainkeyspace/16-18: begin
2 mainkeyspace/16-18: insert into users(user_id, name) values (1, 'john') /* vtgate:: keyspace_id:166b40b44aba4bd6 */
3 mainkeyspace/22-24: commit
4 mainkeyspace/16-18: commit

----------------------------------------------------------------------
```

This example shows how Vitess handles an insert into a table with a secondary lookup vindex.

First, at time `1`, a transaction is opened on one shard to insert the row into the `users_name_idx` table. Then at time `2` a second transaction is opened on another shard with the actual insert into the `users` table, and finally each transaction is committed at time `3` and `4`.

**Configuration Options**

The `--shards` option specifies the number of shards to simulate. vtexplain will always allocate an evenly divided key range to each.

The `--replication-mode` option controls whether to simulate row based or statement based replication.

You can find more usage of `vtexplain` by executing the following command: 

```
vtexplain --help 
```
