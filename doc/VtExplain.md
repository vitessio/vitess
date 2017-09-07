# VTExplain Tool

The following document explains how to use VTExplain tool. This tool provides information about how Vitess will execute a statement (the Vitess analogous of MySQL "EXPLAIN").

## Prerequisites

You will have to compile the `vtexplain` binary in your environment. To find instructions on how to build this binary please refer to this [guide](http://vitess.io/getting-started/local-instance.html#manual-build). 

## Explaining a Query

In order to explain a query you will need to have at hand a schema and a vschema. Let's use the following:

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

Next, you could create a file with these schemas and start explaining queries:

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

You can find full capabilities of `vtexplain` by executing the following command: 

```
vtexplain --help 
```
