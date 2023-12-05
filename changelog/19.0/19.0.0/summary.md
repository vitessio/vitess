## Summary

### Table of Contents

- **[Major Changes](#major-changes)**
  - **[Dropping Support for MySQL 5.7](#drop-support-mysql57)**
  - **[Deprecations and Deletions](#deprecations-and-deletions)**
    - [VTTablet Flags](#vttablet-flags)
  - **[Docker](#docker)**
    - [New MySQL Image](#mysql-image)
  - **[VTGate](#vtgate)**
    - [`FOREIGN_KEY_CHECKS` is now a Vitess Aware Variable](#fk-checks-vitess-aware)
  - **[Query Compatibility](#query-compatibility)**
    - [`SHOW VSCHEMA KEYSPACES` Query](#show-vschema-keyspaces)
  - **[Planned Reparent Shard](#planned-reparent-shard)**
    - [`--tolerable-replication-lag` Sub-flag](#tolerable-repl-lag)

## <a id="major-changes"/>Major Changes

### <a id="drop-support-mysql57"/>Dropping Support for MySQL 5.7

Oracle has marked MySQL 5.7 end of life as of October 2023. Vitess is also dropping support for MySQL 5.7 from v19 onwards. Users are advised to upgrade to MySQL 8.0 while on v18 version of Vitess before
upgrading to v19.

Vitess will however, continue to support importing from MySQL 5.7 into Vitess even in v19.

### <a id="deprecations-and-deletions"/>Deprecations and Deletions

- The `MYSQL_FLAVOR` environment variable is now removed from all Docker Images.

#### <a id="vttablet-flags"/>VTTablet Flags

- The following flags — which were deprecated in Vitess 7.0 — have been removed:
`--vreplication_healthcheck_topology_refresh`, `--vreplication_healthcheck_retry_delay`, and `--vreplication_healthcheck_timeout`.
- The `--vreplication_tablet_type` flag is now deprecated and ignored.

### <a id="docker"/>Docker

#### <a id="mysql-image"/>New MySQL Image

In `v19.0` the Vitess team is shipping a new image: `vitess/mysql`.
This lightweight image is a replacement of `vitess/lite` to only run `mysqld`.

Several tags are available to let you choose what version of MySQL you want to use: `vitess/mysql:8.0.30`, `vitess/mysql:8.0.34`.

### <a id="vtgate"/>VTGate

#### <a id="fk-checks-vitess-aware"/>`FOREIGN_KEY_CHECKS` is now a Vitess Aware Variable

When VTGate receives a query to change the `FOREIGN_KEY_CHECKS` value for a session, instead of sending the value down to MySQL, VTGate now keeps track of the value and changes the queries by adding `SET_VAR(FOREIGN_KEY_CHECKS=On/Off)` style query optimizer hints wherever required. 

### <a id="query-compatibility"/>Query Compatibility

#### <a id="show-vschema-keyspaces"/>`SHOW VSCHEMA KEYSPACES` Query

A SQL query, `SHOW VSCHEMA KEYSPACES` is now supported in Vitess. This query prints the vschema information
for all the keyspaces. It is useful for seeing the foreign key mode, whether the keyspace is sharded, and if there is an
error in the VSchema for the keyspace.

An example output of the query looks like - 
```sql
mysql> show vschema keyspaces;
+----------+---------+-------------+---------+
| Keyspace | Sharded | Foreign Key | Comment |
+----------+---------+-------------+---------+
| ks       | true    | managed     |         |
| uks      | false   | managed     |         |
+----------+---------+-------------+---------+
2 rows in set (0.01 sec)
```

### <a id="planned-reparent-shard"/>Planned Reparent Shard

#### <a id="tolerable-repl-lag"/>`--tolerable-replication-lag` Sub-flag

A new sub-flag `--tolerable-replication-lag` has been added to the command `PlannedReparentShard` that allows users to specify the amount of replication lag that is considered acceptable for a tablet to be eligible for promotion when Vitess makes the choice of a new primary.
This feature is opt-in and not specifying this sub-flag makes Vitess ignore the replication lag entirely.

A new flag in VTOrc with the same name has been added to control the behaviour of the PlannedReparentShard calls that VTOrc issues.

