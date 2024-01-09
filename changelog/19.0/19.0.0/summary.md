## Summary

### Table of Contents

- **[Major Changes](#major-changes)**
  - **[Dropping Support for MySQL 5.7](#drop-support-mysql57)**
  - **[Deprecations and Deletions](#deprecations-and-deletions)**
    - [VTTablet Flags](#vttablet-flags)
  - **[Docker](#docker)**
    - [New MySQL Image](#mysql-image)
  - **[New Stats](#new-stats)**
    - [Stream Consolidations](#stream-consolidations)
    - [Build Version in `/debug/vars`](#build-version-in-debug-vars)
  - **[VTGate](#vtgate)**
    - [`FOREIGN_KEY_CHECKS` is now a Vitess Aware Variable](#fk-checks-vitess-aware)
    - [Partial Multi-shard Commit Warnings](#partial-multi-shard-commit-warnings)
  - **[Vttestserver](#vttestserver)**
    - [`--vtcombo-bind-host` flag](#vtcombo-bind-host)
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

### <a id="new-stats"/>new stats

#### <a id="stream-consolidations"/>Stream Consolidations

Prior to 19.0 VTTablet reported how much time non-streaming executions spend waiting for consolidations to occur. In 19.0, VTTablet reports a similar stat for streaming executions in `/debug/vars` stat `Waits.Histograms.StreamConsolidations`.

#### <a id="build-version-in-debug-vars"/>Build Version in `/debug/vars`

The build version (e.g., `19.0.0-SNAPSHOT`) has been added to `/debug/vars`, allowing users to programmatically inspect Vitess components' build version at runtime.

### <a id="vtgate"/>VTGate

#### <a id="fk-checks-vitess-aware"/>`FOREIGN_KEY_CHECKS` is now a Vitess Aware Variable

When VTGate receives a query to change the `FOREIGN_KEY_CHECKS` value for a session, instead of sending the value down to MySQL, VTGate now keeps track of the value and changes the queries by adding `SET_VAR(FOREIGN_KEY_CHECKS=On/Off)` style query optimizer hints wherever required. 

#### <a id="partial-multi-shard-commit-warnings"/>Partial Multi-shard Commit Warnings

When using `multi` transaction mode (the default), it is possible for Vitess to successfully commit to one shard, but fail to commit to a subsequent shard, thus breaking the atomicity of a multi-shard transaction.

In `v19.0`, VTGate reports partial-success commits in warnings, e.g.:

```mysql
mysql> commit;
ERROR 1317 (70100): target: customer.-80.primary: vttablet: rpc error: code = Aborted desc = transaction 1703182545849001001: ended at 2023-12-21 14:07:41.515 EST (exceeded timeout: 30s) (CallerID: userData1)
mysql> show warnings;
+---------+------+----------------------------------------------------------+
| Level   | Code | Message                                                  |
+---------+------+----------------------------------------------------------+
| Warning |  301 | multi-db commit failed after committing to 1 shards: -80 |
+---------+------+----------------------------------------------------------+
1 row in set, 1 warning (0.00 sec)
```

### <a id="vttestserver"/>Vttestserver

#### <a id="vtcombo-bind-host"/>`--vtcombo-bind-host` flag

A new flag `--vtcombo-bind-host` has been added to vttestserver that allows the users to configure the bind host that vtcombo uses. This is especially useful when running vttestserver as a docker image and you want to run vtctld commands and look at the vtcombo `/debug/status` dashboard.

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

