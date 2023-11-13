## Summary

### Table of Contents

- **[Major Changes](#major-changes)**
  - **[Deprecations and Deletions](#deprecations-and-deletions)**
  - **[Docker](#docker)**
    - [New MySQL Image](#mysql-image)
  - **[Query Compatibility](#query-compatibility)**
    - [`SHOW VSCHEMA KEYSPACES` Query](#show-vschema-keyspaces)

## <a id="major-changes"/>Major Changes

### <a id="deprecations-and-deletions"/>Deprecations and Deletions

- The `MYSQL_FLAVOR` environment variable is now removed from all Docker Images.

### <a id="docker"/>Docker

#### <a id="mysql-image"/>New MySQL Image

In `v19.0` the Vitess team is shipping a new image: `vitess/mysql`.
This lightweight image is a replacement of `vitess/lite` to only run `mysqld`.

Several tags are available to let you choose what version of MySQL you want to use: `vitess/mysql:8.0.30`, `vitess/mysql:8.0.34`.

### <a id="query-compatibility"/>Query Compatibility

#### <a id="show-vschema-keyspaces"/>`SHOW VSCHEMA KEYSPACES` Query

A SQL query, `SHOW VSCHEMA KEYSPACES` is now supported in Vitess. This query prints the vschema information
for all the keyspaces. It is useful for seeing the foreign key mode, whether the keyspace is sharded, and if there is an
error in the VSchema for the keyspace.

An example output of the query looks like - 
```sql
mysql> show vschema keyspaces;
+---------------+---------+------------------+-------+
| Keyspace Name | Sharded | Foreign Key Mode | Error |
+---------------+---------+------------------+-------+
| uks           | false   | managed          |       |
| ks            | true    | managed          |       |
+---------------+---------+------------------+-------+
2 rows in set (0.00 sec)
```
