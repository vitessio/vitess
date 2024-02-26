# Release of Vitess v19.0.0
## Summary

### Table of Contents

- **[Major Changes](#major-changes)**
  - **[Dropping Support for MySQL 5.7](#drop-support-mysql57)**
  - **[Deprecations and Deletions](#deprecations-and-deletions)**
    - [VTTablet Flags](#vttablet-flags)
    - [Docker Image vitess/lite](#deprecation-vitess-lite-mysqld)
    - [Explain Statement Format](#explain-stmt-format)
  - **[Breaking Changes](#breaking-changes)**
     - [ExecuteFetchAsDBA rejects multi-statement SQL](#execute-fetch-as-dba-reject-multi)
  - **[New Stats](#new-stats)**
    - [Stream Consolidations](#stream-consolidations)
    - [Build Version in `/debug/vars`](#build-version-in-debug-vars)
  - **[Planned Reparent Shard](#planned-reparent-shard)**
    - [`--tolerable-replication-lag` Sub-flag](#tolerable-repl-lag)
  - **[Query Compatibility](#query-compatibility)**
    - [Multi Table Delete Support](#multi-table-delete)
    - [`SHOW VSCHEMA KEYSPACES` Query](#show-vschema-keyspaces)
    - [`FOREIGN_KEY_CHECKS` is now a Vitess Aware Variable](#fk-checks-vitess-aware)
    - [Explain Statement](#explain-statement)
    - [Partial Multi-shard Commit Warnings](#partial-multi-shard-commit-warnings)
    - [New Lock Syntax](#lock-syntax)
    - [Support for AVG()](#avg-support)
    - [Support for non-recursive CTEs](#cte-support)
  - **[Vttestserver](#vttestserver)**
    - [`--vtcombo-bind-host` flag](#vtcombo-bind-host)
- **[Minor Changes](#minor-changes)**
    - **[Apply VSchema](#apply-vschema)**
        - [`--strict` sub-flag and `strict` gRPC field](#strict-flag-and-field)

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

#### <a id="deprecation-vitess-lite-mysqld"/>Docker Image vitess/lite

The `mysqld` binary is now deprecated in the `vitess/lite` Docker image and will be removed in a future release.
This means that the MySQL/Percona version specific image tags for the `vitess/lite` image are deprecated.

Below is a full list of available tags for `v19.0.0` and their deprecation status:

| Image                           | Deprecated | 
|---------------------------------|------------|
| `vitess/lite:v19.0.0`           | NO         |
| `vitess/lite:v19.0.0-mysql57`   | YES        |
| `vitess/lite:v19.0.0-mysql80`   | YES        |
| `vitess/lite:v19.0.0-percona57` | YES        |
| `vitess/lite:v19.0.0-percona80` | YES        |

If you are currently using `vitess/lite` as your `mysqld` image in your vitess-operator deployment we invite you to use an official MySQL image, such as `mysql:8.0.30`.

Below is an example of a kubernetes yaml file before and after upgrading to an official MySQL image:

```yaml
# before:

# the image used here includes MySQL 8.0.30 and its binaries

    mysqld:
      mysql80Compatible: vitess/lite:v19.0.0-mysql80
```
```yaml
# after:

# if we still want to use MySQL 8.0.30, we now have to use the
# official MySQL image with the 8.0.30 tag as shown below 

    mysqld:
      mysql80Compatible: mysql:8.0.30 # or even mysql:8.0.34 for instance
```

#### <a id="explain-stmt-format"/>Explain Statement Format

Explain statement format `vitess` and `vexplain` were deprecated in v16 and removed in v19 version.
Use [VExplain Statement](https://vitess.io/docs/19.0/user-guides/sql/vexplain/) for understanding Vitess plans.

### <a id="breaking-changes"/>Breaking Changes

#### <a id="execute-fetch-as-dba-reject-multi"/>ExecuteFetchAsDBA rejects multi-statement SQL

`vtctldclient ExecuteFetchAsDBA` (and similarly the `vtctl` and `vtctlclient` commands) now reject multi-statement SQL with error.

For example, `vtctldclient ExecuteFetchAsDBA my-tablet "stop replica; change replication source to auto_position=1; start replica` will return an error, without attempting to execute any of these queries.

Previously, `ExecuteFetchAsDBA` silently accepted multi statement SQL. It would (attempt to) execute all of them, but:

- It would only indicate error for the first statement. Errors on 2nd, 3rd, ... statements were silently ignored.
- It would not consume the result sets of the 2nd, 3rd, ... statements. It would then return the used connection to the pool in a dirty state. Any further query that happens to take that connection out of the pool could get unexpected results.
- As another side effect, multi-statement schema changes would cause schema to be reloaded with only the first change, leaving the cached schema inconsistent with the underlying database.

`ExecuteFetchAsDBA` does allow a specific use case of multi-statement SQL, which is where all statements are in the form of `CREATE TABLE` or `CREATE VIEW`. This is to support a common pattern of schema initialization, formalized in `ApplySchema --batch-size` which uses `ExecuteFetchAsDBA` under the hood.

### <a id="new-stats"/>New Stats

#### <a id="stream-consolidations"/>Stream Consolidations

Prior to 19.0 VTTablet reported how much time non-streaming executions spend waiting for consolidations to occur. In 19.0, VTTablet reports a similar stat for streaming executions in `/debug/vars` stat `Waits.Histograms.StreamConsolidations`.

#### <a id="build-version-in-debug-vars"/>Build Version in `/debug/vars`

The build version (e.g., `19.0.0-SNAPSHOT`) has been added to `/debug/vars`, allowing users to programmatically inspect Vitess components' build version at runtime.

### <a id="planned-reparent-shard"/>Planned Reparent Shard

#### <a id="tolerable-repl-lag"/>`--tolerable-replication-lag` Sub-flag

A new sub-flag `--tolerable-replication-lag` has been added to the command `PlannedReparentShard` that allows users to specify the amount of replication lag that is considered acceptable for a tablet to be eligible for promotion when Vitess makes the choice of a new primary.
This feature is opt-in and not specifying this sub-flag makes Vitess ignore the replication lag entirely.

A new flag in VTOrc with the same name has been added to control the behaviour of the PlannedReparentShard calls that VTOrc issues.

### <a id="query-compatibility"/>Query Compatibility

#### <a id="multi-table-delete"/> Multi Table Delete Support

Support is added for sharded multi-table delete with target on single table using multiple table join.

Example: `Delete t1 from t1 join t2 on t1.id = t2.id join t3 on t1.col = t3.col where t3.foo = 5 and t2.bar = 7`

More details about how it works is available in [MySQL Docs](https://dev.mysql.com/doc/refman/8.0/en/delete.html)

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

#### <a id="fk-checks-vitess-aware"/>`FOREIGN_KEY_CHECKS` is now a Vitess Aware Variable

When VTGate receives a query to change the `FOREIGN_KEY_CHECKS` value for a session, instead of sending the value down to MySQL, VTGate now keeps track of the value and changes the queries by adding `SET_VAR(FOREIGN_KEY_CHECKS=On/Off)` style query optimizer hints wherever required. 

#### <a id="explain-statement"/>Explain Statement

`Explain` statement can handle routed table queries now. `Explain` is unsupported when the tables involved in the query refers more than one keyspace. Users should use [VExplain Statement](https://vitess.io/docs/19.0/user-guides/sql/vexplain/) in those cases.

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
| Warning |  301 | multi-db commit failed after committing to 1 shards: 80- |
+---------+------+----------------------------------------------------------+
1 row in set, 1 warning (0.00 sec)
```

### <a id="vttestserver"/>Vttestserver

#### <a id="vtcombo-bind-host"/>`--vtcombo-bind-host` flag

A new flag `--vtcombo-bind-host` has been added to vttestserver that allows the users to configure the bind host that vtcombo uses. This is especially useful when running vttestserver as a docker image and you want to run vtctld commands and look at the vtcombo `/debug/status` dashboard.

### <a id="lock-syntax"/>New lock syntax

Vitess now supports the following LOCK syntax

```sql
SELECT .. FOR SHARE (NOWAIT|SKIP LOCKED)
SELECT .. FOR UPDATE (NOWAIT|SKIP LOCKED)
```

### <a id="avg-support"/>Support for AVG() aggregation function

Vtgate can now evaluate `AVG` on sharded keyspaces, by using a combination of `SUM/COUNT`

### <a id="cte-support"/>Support for non-recursive CTEs

Common table expressions that are not recursive can now be used. 

```sql
with userCount as (
    select id, count(*) as nr from user group by id)
select ref.col, userCount.nr
from ref join userCount on ref.user_id = userCount.id
```

## <a id="minor-changes"/>Minor Changes

### <a id="apply-vschema"/>Apply VSchema

#### <a id="strict-flag-and-field"/>`--strict` sub-flag and `strict` gRPC field

A new sub-flag `--strict` has been added to the command `ApplyVSchema` `vtctl` command that produces an error if unknown params are found in any Vindexes. An equivalent `strict` field has been added to the `ApplyVSchema` gRPC `vtctld` command.

------------
The entire changelog for this release can be found [here](https://github.com/vitessio/vitess/blob/main/changelog/19.0/19.0.0/changelog.md).

The release includes 453 merged Pull Requests.

Thanks to all our contributors: @ChaitanyaD48, @EshaanAgg, @FirePing32, @GuptaManan100, @Its-Maniaco, @Maniktherana, @Manni-99, @MrFabio, @VaibhavMalik4187, @ajm188, @aparajon, @app/dependabot, @app/github-actions, @app/vitess-bot, @aquarapid, @arthurschreiber, @austenLacy, @beingnoble03, @brendar, @davidpiegza, @dbussink, @deepthi, @derekperkins, @ejortegau, @frouioui, @gerayking, @glokta1, @harshit-gangal, @iheanyi, @jwangace, @lixin963, @mattlord, @mattrobenolt, @maxenglander, @mcrauwel, @mdlayher, @olyazavr, @pbibra, @pnacht, @rajivharlalka, @ravicodelabs, @rbranson, @rohit-nayak-ps, @samanthadrago, @shlomi-noach, @skullface, @systay, @testwill, @tycol7, @vmg, @wangweicugw, @williammartin, @wlx5575

