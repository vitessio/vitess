# Release of Vitess v22.0.0
## Summary

### Table of Contents

- **[Major Changes](#major-changes)**
  - **[Deprecations](#deprecations)**
    - [Metrics](#deprecated-metrics)
    - [CLI Flags](#deprecated-cli-flags)
  - **[Deletions](#deletions)**
    - [Metrics](#deleted-metrics)
    - [CLI Flags](#deleted-cli-flags)
    - [gh-ost and pt-osc Online DDL strategies](#deleted-ghost-ptosc)
  - **[New Metrics](#new-metrics)**
    - [VTGate](#new-vtgate-metrics)
    - [VTTablet](#new-vtgate-metrics)
  - **[Config File Changes](#config-file-changes)**
    - [VTOrc](#vtorc-config-file-changes)
    - [VTGate](#vtgate-config-file-changes)
  - **[VTOrc](#vtorc)**
    - [Stalled Disk Recovery](#stall-disk-recovery-vtorc)
    - [KeyRanges in `--clusters_to_watch`](#key-range-vtorc)
  - **[New Default Versions](#new-default-versions)**
    - [MySQL 8.0.40](#mysql-8-0-40)
    - [Docker `vitess/lite` images with Debian Bookworm](#debian-bookworm)
  - **[New Support](#new-support)**
    - [More Efficient JSON Replication](#efficient-json-replication)
    - [LAST_INSERT_ID(x)](#last-insert-id)
    - [Maximum Idle Connections in the Pool](#max-idle-connections)
    - [Filtering Query logs on Error](#query-logs)
    - [MultiQuery RPC in vtgate](#multiquery)
    - [Unsharded `CREATE PROCEDURE` support](#create-procedure)
  - **[Optimization](#optimization)**
    - [Prepared Statement](#prepared-statement)
  - **[RPC Changes](#rpc-changes)**
  - **[Prefer not promoting a replica that is currently taking a backup](#reparents-prefer-not-backing-up)**
  - **[Semi-sync monitor in vttablet](#semi-sync-monitor)**
  - **[Wrapped fatal transaction errors](#new-errors-fatal-tx)**
- **[Minor Changes](#minor-changes)**
  - **[Topology read concurrency behaviour changes](#topo-read-concurrency-changes)**
  - **[VTTablet](#minor-changes-vttablet)**
    - [CLI Flags](#flags-vttablet)
    - [ACL enforcement and reloading](#reloading-vttablet-acl)
  - **[VTAdmin](#vtadmin)**
    - [Updated to node v22.13.1](#updated-node)

## <a id="major-changes"/>Major Changes</a>

### <a id="deprecations"/>Deprecations</a>

#### <a id="deprecated-metrics"/>Metrics</a>

| Component |        Metric Name        |                     Deprecation PR                      |
|:---------:|:-------------------------:|:-------------------------------------------------------:|
| `vtgate`  |    `QueriesProcessed`     | [#17727](https://github.com/vitessio/vitess/pull/17727) |
| `vtgate`  |      `QueriesRouted`      | [#17727](https://github.com/vitessio/vitess/pull/17727) |
| `vtgate`  | `QueriesProcessedByTable` | [#17727](https://github.com/vitessio/vitess/pull/17727) |
| `vtgate`  |  `QueriesRoutedByTable`   | [#17727](https://github.com/vitessio/vitess/pull/17727) |


#### <a id="deprecated-cli-flags"/>CLI Flags</a>

| Component  |            Flag Name             | Notes                                                                                                          |                     Deprecation PR                      |
|:----------:|:--------------------------------:|----------------------------------------------------------------------------------------------------------------|:-------------------------------------------------------:|
| `vttablet` |          `twopc_enable`          | Usage of TwoPC commit will be determined by the `transaction_mode` set on VTGate via flag or session variable. | [#17279](https://github.com/vitessio/vitess/pull/17279) |
|  `vtgate`  | `grpc-send-session-in-streaming` | Session will be sent as part of response on StreamExecute API call.                                            | [#17907](https://github.com/vitessio/vitess/pull/17907) |

---

### <a id="deletions"/>Deletions</a>

#### <a id="deleted-metrics"/>Metrics</a>

| Component  |      Metric Name      | Was Deprecated In |                     Deprecation PR                      |
|:----------:|:---------------------:|:-----------------:|:-------------------------------------------------------:|
| `vttablet` |  `QueryCacheLength`   |     `v21.0.0`     | [#16289](https://github.com/vitessio/vitess/pull/16289) |
| `vttablet` |   `QueryCacheSize`    |     `v21.0.0`     | [#16289](https://github.com/vitessio/vitess/pull/16289) |
| `vttablet` | `QueryCacheCapacity`  |     `v21.0.0`     | [#16289](https://github.com/vitessio/vitess/pull/16289) |
| `vttablet` | `QueryCacheEvictions` |     `v21.0.0`     | [#16289](https://github.com/vitessio/vitess/pull/16289) |
| `vttablet` |   `QueryCacheHits`    |     `v21.0.0`     | [#16289](https://github.com/vitessio/vitess/pull/16289) |
| `vttablet` |  `QueryCacheMisses`   |     `v21.0.0`     | [#16289](https://github.com/vitessio/vitess/pull/16289) |

#### <a id="deleted-cli-flags"/>CLI Flags</a>

|           Component           |             Flag Name              | Was Deprecated In |                     Deprecation PR                      |
|:-----------------------------:|:----------------------------------:|:-----------------:|:-------------------------------------------------------:|
|          `vttablet`           | `queryserver-enable-settings-pool` |     `v21.0.0`     | [#16280](https://github.com/vitessio/vitess/pull/16280) |
|          `vttablet`           |  `remove-sharded-auto-increment`   |     `v21.0.0`     | [#16860](https://github.com/vitessio/vitess/pull/16860) |
|          `vttablet`           |     `disable_active_reparents`     |     `v20.0.0`     | [#14871](https://github.com/vitessio/vitess/pull/14871) |
| `vtgate`, `vtcombo`, `vtctld` |   `healthcheck-dial-concurrency`   |     `v21.0.0`     | [#16378](https://github.com/vitessio/vitess/pull/16378) |

#### <a id="deleted-ghost-ptosc"/>gh-ost and pt-osc Online DDL strategies</a>

Vitess no longer recognizes the `gh-ost` and `pt-osc` (`pt-online-schema-change`) Online DDL strategies. The `vitess` strategy is the recommended way to make schema changes at scale. `mysql` and `direct` strategies continue to be supported.

These `vttablet` flags have been removed:

- `--gh-ost-path`
- `--pt-osc-path`

The use of `gh-ost` and `pt-osc` as strategies as follows, yields an error:
```sh
$ vtctldclient ApplySchema --ddl-strategy="gh-ost" ...
$ vtctldclient ApplySchema --ddl-strategy="pt-osc" ...
```

---

### <a id="new-metrics"/>New Metrics

#### <a id="new-vtgate-metrics"/>VTGate

|           Name            |              Dimensions               |                         Description                         |                           PR                            |
|:-------------------------:|:-------------------------------------:|:-----------------------------------------------------------:|:-------------------------------------------------------:|
|     `QueryExecutions`     |       `Query`, `Plan`, `Tablet`       |                 Number of queries executed.                 | [#17727](https://github.com/vitessio/vitess/pull/17727) |
|       `QueryRoutes`       |       `Query`, `Plan`, `Tablet`       |       Number of vttablets the query was executed on.        | [#17727](https://github.com/vitessio/vitess/pull/17727) |
| `QueryExecutionsByTable`  |           `Query`, `Table`            | Queries executed at vtgate, with counts recorded per table. | [#17727](https://github.com/vitessio/vitess/pull/17727) |
|      `VStreamsCount`      | `Keyspace`, `ShardName`, `TabletType` |                  Number of active vstream.                  | [#17858](https://github.com/vitessio/vitess/pull/17858) |
| `VStreamsEventsStreamed`  | `Keyspace`, `ShardName`, `TabletType` |         Number of events sent across all vstreams.          | [#17858](https://github.com/vitessio/vitess/pull/17858) |
| `VStreamsEndedWithErrors` | `Keyspace`, `ShardName`, `TabletType` |         Number of vstreams that ended with errors.          | [#17858](https://github.com/vitessio/vitess/pull/17858) |
|    `CommitModeTimings`    |                `Mode`                 |      Timing metrics for commit (Single, Multi, TwoPC).      | [#16939](https://github.com/vitessio/vitess/pull/16939) |
|    `CommitUnresolved`     |                  N/A                  |             Counter for failure after Prepare.              | [#16939](https://github.com/vitessio/vitess/pull/16939) |


The work done in [#17727](https://github.com/vitessio/vitess/pull/17727) introduces new metrics for queries. Via this work we have deprecated several vtgate metrics, please see the [Deprecated Metrics](#deprecated-metrics) section. Here is an example on how to use them:
```
Query: select t1.a, t2.b from t1 join t2 on t1.id = t2.id
Shards: 2
Sharding Key: id for both tables

Metrics Published:
1. QueryExecutions – {select, scatter, primary}, 1
2. QueryRoutes – {select, scatter, primary}, 2
3. QueryExecutionsByTable – {select, t1}, 1 and {select, t2}, 1
```

#### <a id="new-vttablet-metrics"/>VTTablet

|           Name            |    Dimensions    |                    Description                    |                           PR                            |
|:-------------------------:|:----------------:|:-------------------------------------------------:|:-------------------------------------------------------:|
|        `TableRows`        |     `Table`      |      Estimated number of rows in the table.       | [#17570](https://github.com/vitessio/vitess/pull/17570) |
| `TableClusteredIndexSize` |     `Table`      | Byte size of the clustered index (i.e. row data). | [#17570](https://github.com/vitessio/vitess/pull/17570) |
|    `IndexCardinality`     | `Table`, `Index` |  Estimated number of unique values in the index   | [#17570](https://github.com/vitessio/vitess/pull/17570) |
|       `IndexBytes`        | `Table`, `Index` |              Byte size of the index.              | [#17570](https://github.com/vitessio/vitess/pull/17570) |
|  `UnresolvedTransaction`  |  `ManagerType`   |    Number of events sent across all vstreams.     | [#16939](https://github.com/vitessio/vitess/pull/16939) |
|   `CommitPreparedFail`    |  `FailureType`   |    Number of vstreams that ended with errors.     | [#16939](https://github.com/vitessio/vitess/pull/16939) |
|    `RedoPreparedFail`     |  `FailureType`   | Timing metrics for commit (Single, Multi, TwoPC)  | [#16939](https://github.com/vitessio/vitess/pull/16939) |

---

### <a id="config-file-changes"/>Config File Changes</a>

#### <a id="vtorc-config-file-changes"/>VTOrc</a>

The configuration file for VTOrc has been updated to now support dynamic fields. The old `--config` parameter has been removed. The alternative is to use the `--config-file` parameter. The configuration can now be provided in json, yaml or any other format that [viper](https://github.com/spf13/viper) supports.

The following fields can be dynamically changed -
1. `instance-poll-time`
2. `prevent-cross-cell-failover`
3. `snapshot-topology-interval`
4. `reasonable-replication-lag`
5. `audit-to-backend`
6. `audit-to-syslog`
7. `audit-purge-duration`
8. `wait-replicas-timeout`
9. `tolerable-replication-lag`
10. `topo-information-refresh-duration`
11. `recovery-poll-duration`
12. `allow-emergency-reparent`
13. `change-tablets-with-errant-gtid-to-drained`

To upgrade to the newer version of the configuration file, first switch to using the flags in your current deployment before upgrading. Then you can switch to using the configuration file in the newer release.

#### <a id="vtgate-config-file-changes"/>VTGate</a>

The Viper configuration keys for the following flags has been changed to match their flag names. Previously they had a discovery prefix instead of it being part of the name.

| Flag Name                                        | Old Configuration Key                            | New Configuration Key                            |
|--------------------------------------------------|--------------------------------------------------|--------------------------------------------------|
| `discovery_low_replication_lag`                  | `discovery.low_replication_lag`                  | `discovery_low_replication_lag`                  |
| `discovery_high_replication_lag_minimum_serving` | `discovery.high_replication_lag_minimum_serving` | `discovery_high_replication_lag_minimum_serving` |
| `discovery_min_number_serving_vttablets`         | `discovery.min_number_serving_vttablets`         | `discovery_min_number_serving_vttablets`         |
| `discovery_legacy_replication_lag_algorithm`     | `discovery.legacy_replication_lag_algorithm`     | `discovery_legacy_replication_lag_algorithm`     |

To upgrade to the newer version of the configuration keys, first switch to using the flags in your current deployment before upgrading. Then you can switch to using the new configuration keys in the newer release.

---

### <a id="vtorc"/>VTOrc</a>

#### <a id="stall-disk-recovery-vtorc"/>Stalled Disk Recovery</a>

VTOrc can now identify and recover from stalled disk errors.
VTTablets test whether the disk is writable and they send this information in the full status output to VTOrc.
If the disk is not writable on the primary tablet, VTOrc will attempt to recover the cluster by promoting a new primary.
This is useful in scenarios where the disk is stalled and the primary vttablet is unable to accept writes because of it.

To opt into this feature, `--enable-primary-disk-stalled-recovery` flag has to be specified on VTOrc, and `--disk-write-dir` flag has to be specified on the vttablets.
`--disk-write-interval` and `--disk-write-timeout` flags can be used to configure the polling interval and timeout respectively.

#### <a id="key-range-vtorc"/>KeyRanges in `--clusters_to_watch`</a>
VTOrc now supports specifying keyranges in the `--clusters_to_watch` flag. This means that there is no need to restart a VTOrc instance with a different flag value when you reshard a keyspace.

For example, if a VTOrc is configured to watch `ks/-80`, then it would watch all the shards that fall under the keyrange `-80`.
If a reshard is performed and `-80` is split into new shards `-40` and `40-80`, the VTOrc instance will automatically start watching the new shards without needing a restart.
In the previous logic, specifying `ks/-80` for the flag would mean that VTOrc would watch only 1 (or no) shard.
In the new system, since we interpret `-80` as a key range, it can watch multiple shards as described in the example.

Users can continue to specify exact keyranges. The new feature is backward compatible.

---
### <a id="new-default-versions"/>New Default Versions</a>

#### <a id="mysql-8-0-40"/>MySQL 8.0.40</a>

The default major MySQL version used by our `vitess/lite:latest` image is going from `8.0.30` to `8.0.40`.
This change was brought by [#17552](https://github.com/vitessio/vitess/pull/17552).

VTGate also advertises MySQL version `8.0.40` by default instead of `8.0.30` if no explicit version is set. The users can set the `mysql_server_version` flag to advertise the correct version.

>  ⚠️ Upgrading to this release with vitess-operator:
>
> If you are using the `vitess-operator`, considering that we are bumping the patch version of MySQL 80 from `8.0.30` to `8.0.40`, you will have to manually upgrade:
>
> 1. Add `innodb_fast_shutdown=0` to your extra cnf in your YAML file.
> 2. Apply this file.
> 3. Wait for all the pods to be healthy.
> 4. Then change your YAML file to use the new Docker Images (`vitess/lite:v22.0.0`).
> 5. Remove `innodb_fast_shutdown=0` from your extra cnf in your YAML file.
> 6. Apply this file.
>
> This is the last time this will be needed in the `8.0.x` series, as starting with MySQL `8.0.35` it is possible to upgrade and downgrade between `8.0.x` versions without needing to run `innodb_fast_shutdown=0`.

#### <a id="debian-bookworm"/>Docker `vitess/lite` images with Debian Bookworm</a>

The base system now uses Debian Bookworm instead of Debian Bullseye for the `vitess/lite` images. This change was brought by [#17552](https://github.com/vitessio/vitess/pull/17552).

---

### <a id="new-support"/>New Support</a>

#### <a id="efficient-json-replication"/>More Efficient JSON Replication</a>

In [#7345](https://github.com/vitessio/vitess/pull/17345) we added support for [`--binlog-row-value-options=PARTIAL_JSON`](https://dev.mysql.com/doc/refman/en/replication-options-binary-log.html#sysvar_binlog_row_value_options). You can read more about [this feature added to MySQL 8.0 here](https://dev.mysql.com/blog-archive/efficient-json-replication-in-mysql-8-0/).

If you are using MySQL 8.0 or later and using JSON columns, you can now enable this MySQL feature across your Vitess cluster(s) to lower the disk space needed for binary logs and improve the CPU and memory usage in both `mysqld` (standard intrashard MySQL replication) and `vttablet` ([VReplication](https://vitess.io/docs/reference/vreplication/vreplication/)) without losing any capabilities or features.

#### <a id="last-insert-id"/>`LAST_INSERT_ID(x)`</a>

In [#17408](https://github.com/vitessio/vitess/pull/17408) and [#17409](https://github.com/vitessio/vitess/pull/17409), we added the ability to use `LAST_INSERT_ID(x)` in Vitess directly at vtgate. This improvement allows certain queries—like `SELECT last_insert_id(123);` or `SELECT last_insert_id(count(*)) ...`—to be handled without relying on MySQL for the final value.

**Limitations**:
- When using `LAST_INSERT_ID(x)` in ordered queries (e.g., `SELECT last_insert_id(col) FROM table ORDER BY foo`), MySQL sets the session’s last-insert-id value according to the *last row returned*. Vitess does not guarantee the same behavior.

#### <a id="max-idle-connections"/>Maximum Idle Connections in the Pool</a>

In [#17443](https://github.com/vitessio/vitess/pull/17443) we introduced a new configurable max-idle-count parameter for connection pools. This allows you to specify the maximum number of idle connections retained in each connection pool to optimize performance and resource efficiency.

You can control idle connection retention for the query server’s query pool, stream pool, and transaction pool with the following flags:
•	--queryserver-config-query-pool-max-idle-count: Defines the maximum number of idle connections retained in the query pool.
•	--queryserver-config-stream-pool-max-idle-count: Defines the maximum number of idle connections retained in the stream pool.
•	--queryserver-config-txpool-max-idle-count: Defines the maximum number of idle connections retained in the transaction pool.

This feature ensures that, during traffic spikes, idle connections are available for faster responses, while minimizing overhead in low-traffic periods by limiting the number of idle connections retained. It helps strike a balance between performance, efficiency, and cost.

#### <a id="query-logs"/>Filtering Query logs on Error</a>

The `querylog-mode` setting can be configured to `error` to log only queries that result in errors. This option is supported in both VTGate and VTTablet.

---

#### <a id="multiquery"/>MultiQuery RPC in vtgate</a>

New RPCs in vtgate have been added that allow users to pass multiple queries in a single sql string. It behaves the same way MySQL does where-in multiple result sets for the queries are returned in the same order as the queries were passed until an error is encountered. The new RPCs are `ExecuteMulti` and `StreamExecuteMulti`. 

A new flag `--mysql-server-multi-query-protocol` has also been added that makes the server use this new implementation. This flag is set to `false` by default, so the old implementation is used by default. The new implementation is more efficient and allows for better performance when executing multiple queries in a single RPC call.

---

#### <a id="create-procedure"/>Unsharded `CREATE PROCEDURE` support</a>

Until now Vitess didn't allow users to create procedures through the vtgate, and they had to be created by running a DDL directly against the vttablets. In this release, we have started adding support for running `CREATE PROCEDURE` statements through the vtgate for unsharded keyspaces. Not all constructs of procedures are currently supported in the parser, so there are still some limitations which will be addressed in future releases.

---

### <a id="optimization"/>Optimization</a>

#### <a id="prepared-statement"/>Prepared Statement</a>
Prepared statements now benefit from Deferred Optimization, enabling parameter-aware query plans. 
Initially, a baseline plan is created at prepare-time, and on first execution, a more efficient parameter-optimized plan is generated. 
Subsequent executions dynamically switch between these plans based on input values, improving query performance while ensuring correctness.

---

### <a id="rpc-changes"/>RPC Changes</a>

These are the RPC changes made in this release -

1. `GetTransactionInfo` RPC has been added to both `VtctldServer`, and `TabletManagerClient` interface. These RPCs are used to facilitate the users in reading the state of an unresolved distributed transaction. This can be useful in debugging what went wrong and how to fix the problem.

---

### <a id="reparents-prefer-not-backing-up"/>Prefer not promoting a replica that is currently taking a backup

Emergency reparents now prefer not promoting replicas that are currently taking backups with a backup engine other than
`builtin`. Note that if there's only one suitable replica to promote, and it is taking a backup, it will still be
promoted.

For planned reparents, hosts taking backups with a backup engine other than `builtin` are filtered out of the list of
valid candidates. This means they will never get promoted - not even if there's no other candidates.

Note that behavior for `builtin` backups remains unchanged: a replica that is currently taking a `builtin` backup will
never be promoted, neither by planned nor by emergency reparents.

---

### <a id="semi-sync-monitor"/>Semi-sync monitor in vttablet</a>

A new component has been added to the vttablet binary to monitor the semi-sync status of primary vttablets.
We've observed cases where a brief network disruption can cause the primary to get stuck indefinitely waiting for semi-sync ACKs.
In rare scenarios, this can block reparent operations and render the primary unresponsive.
More information can be found in the issues [#17709](https://github.com/vitessio/vitess/issues/17709) and [#17749](https://github.com/vitessio/vitess/issues/17749).

To address this, the new component continuously monitors the semi-sync status. If the primary becomes stuck on semi-sync ACKs, it generates writes to unblock it. If this fails, VTOrc is notified of the issue and initiates an emergency reparent operation.

The monitoring interval can be adjusted using the `--semi-sync-monitor-interval` flag, which defaults to 10 seconds.

---

### <a id="new-errors-fatal-tx"/>Wrapped fatal transaction errors</a>


When a query fails while being in a transaction, due to the transaction no longer being valid (e.g. PRS, rollout, primary down, etc), the original error is now wrapped around a `VT15001` error.

For non-transactional queries that produce a `VT15001`, VTGate will try to rollback and clear the transaction.
Any new queries on the same connection will fail with a `VT09032` error, until a `ROLLBACK` is received
to acknowledge that the transaction was automatically rolled back and cleared by VTGate.

`VT09032` is returned to clients to avoid applications blindly sending queries to VTGate thinking they are still in a transaction.

This change was introduced by [#17669](https://github.com/vitessio/vitess/pull/17669).


---

## <a id="minor-changes"/>Minor Changes</a>

### <a id="topo-read-concurrency-changes"/>`--topo_read_concurrency` behaviour changes

The `--topo_read_concurrency` flag was added to all components that access the topology and the provided limit is now applied separately for each global or local cell _(default `32`)_.

All topology read calls _(`Get`, `GetVersion`, `List` and `ListDir`)_ now respect this per-cell limit. Previous to this version a single limit was applied to all cell calls and it was not respected by many topology calls.

---

### <a id="minor-changes-vttablet"/>VTTablet</a>

#### <a id="flags-vttablet"/>CLI Flags</a>

- `twopc_abandon_age` flag now supports values in the time.Duration format (e.g., 1s, 2m, 1h). 
While the flag will continue to accept float values (interpreted as seconds) for backward compatibility, 
**float inputs are deprecated** and will be removed in a future release.

- `--consolidator-query-waiter-cap` flag to set the maximum number of clients allowed to wait on the consolidator. The default value is set to 0 for unlimited wait. Users can adjust  this value based on the performance of VTTablet to avoid excessive memory usage and the risk of being OOMKilled, particularly in Kubernetes deployments.

#### <a id="reloading-vttablet-acl"/>ACL enforcement and reloading</a>

When a tablet is started with `--enforce-tableacl-config` it will exit with an error if the contents of the file are not valid. After the changes made in [#17485](https://github.com/vitessio/vitess/pull/17485) the tablet will no longer exit when reloading the contents of the file after receiving a SIGHUP. When the file contents are invalid on reload the tablet will now log an error and the active in-memory ACLs remain in effect.

---

### <a id="vtadmin"/>VTAdmin

#### <a id="updated-node"/>vtadmin-web updated to node v22.13.1 (LTS)

Building `vtadmin-web` now requires node >= v22.13.0 (LTS). Breaking changes from v20 to v22 can be found at https://nodejs.org/en/blog/release/v22.13.0 -- with no known issues that apply to VTAdmin.
Full details on the node v20.12.2 release can be found at https://nodejs.org/en/blog/release/v22.13.1.

------------
The entire changelog for this release can be found [here](https://github.com/vitessio/vitess/blob/main/changelog/22.0/22.0.0/changelog.md).

The release includes 466 merged Pull Requests.

Thanks to all our contributors: @GrahamCampbell, @GuptaManan100, @L3o-pold, @akagami-harsh, @anirbanmu, @app/dependabot, @app/vitess-bot, @arthmis, @arthurschreiber, @beingnoble03, @c-r-dev, @corbantek, @dbussink, @deepthi, @derekperkins, @ejortegau, @frouioui, @garfthoffman, @gmpify, @gopoto, @harshit-gangal, @huochexizhan, @jeefy, @jwangace, @kbutz, @lmorduch, @mattlord, @mattrobenolt, @maxenglander, @mcrauwel, @mounicasruthi, @niladrix719, @notfelineit, @rafer, @rohit-nayak-ps, @rvrangel, @shailpujan88, @shanth96, @shlomi-noach, @siadat, @systay, @timvaillancourt, @twthorn, @vitess-bot, @vmg, @wiebeytec, @wukuai

