# VTExplain Tool

The vtexplain tool provides information about how Vitess will execute a statement (the Vitess version of MySQL "EXPLAIN").

## Prerequisites

You'll need to build the `vtexplain` binary in your environment.
To find instructions on how to build this binary please refer to this [guide](http://vitess.io/getting-started/local-instance.html#manual-build). 

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

[mainkeyspace/c0-]:
select * from users limit 10001

[mainkeyspace/-40]:
select * from users limit 10001

[mainkeyspace/40-80]:
select * from users limit 10001

[mainkeyspace/80-c0]:
select * from users limit 10001

----------------------------------------------------------------------
```

**Insert:**

```bash
vtexplain -shards 1024 -vschema-file /tmp/vschema.json -schema-file /tmp/schema.sql -replication-mode "ROW" -output-mode text -sql "INSERT INTO users (user_id, name) VALUES(1, 'john')"

----------------------------------------------------------------------
INSERT INTO users (user_id, name) VALUES(1, 'john')

[mainkeyspace/22c0-2300]:
begin
insert into users_name_idx(name, user_id) values ('john', 1)
commit

[mainkeyspace/1640-1680]:
begin
insert into users(user_id, name) values (1, 'john')
commit

----------------------------------------------------------------------
```

**Configuration Options**

The `--shards` option specifies the number of shards to simulate. vtexplain will always allocate an evenly divided key range to each.

The `--replication-mode` option controls whether to simulate row based or statement based replication.

You can find more usage of `vtexplain` by executing the following command: 

```
vtexplain --help 
```
