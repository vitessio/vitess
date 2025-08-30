## Summary

### Table of Contents

- **[Major Changes](#major-changes)**
    - **[New default versions](#new-default-versions)**
        - [Upgrade to MySQL 8.4](#upgrade-to-mysql-8.4)
- **[Minor Changes](#minor-changes)**
    - **[Deletions](#deletions)**
        - [Metrics](#deleted-metrics)
    - **[New Metrics](#new-metrics)**
        - [VTGate](#new-vtgate-metrics)
    - **[Topology](#minor-changes-topo)**
        - [`--consul_auth_static_file` requires 1 or more credentials](#consul_auth_static_file-check-creds)
    - **[VTOrc](#minor-changes-vtorc)**
        - [Recovery stats to include keyspace/shard](#recoveries-stats-keyspace-shard)
    - **[VTTablet](#minor-changes-vttablet)**
        - [CLI Flags](#flags-vttablet)
        - [Managed MySQL configuration defaults to caching-sha2-password](#mysql-caching-sha2-password) 
        - [MySQL timezone environment propagation](#mysql-timezone-env)

## <a id="major-changes"/>Major Changes</a>

### <a id="new-default-versions"/>New default versions</a>

#### <a id="upgrade-to-mysql-8.4"/>Upgrade to MySQL 8.4</a>

The default major MySQL version used by our `vitess/lite:latest` image is going from `8.0.40` to `8.4.6`.
This change was merged in [#18569](https://github.com/vitessio/vitess/pull/18569).

VTGate also advertises MySQL version `8.4.6` by default instead of `8.0.40`. If that is not what you are running, you can set the `mysql_server_version` flag to advertise the desired version.

>  ⚠️ Upgrading to this release with vitess-operator:
>
> If you are using the `vitess-operator`, considering that we are bumping the MySQL version from `8.0.40` to `8.4.6`, you will have to manually upgrade:
>
> 1. Add `innodb_fast_shutdown=0` to your extra cnf in your YAML file.
> 2. Apply this file.
> 3. Wait for all the pods to be healthy.
> 4. Then change your YAML file to use the new Docker Images (`vitess/lite:v23.0.0`).
> 5. Remove `innodb_fast_shutdown=0` from your extra cnf in your YAML file.
> 6. Apply this file.
>
> This is only needed once when going from the latest `8.0.x` to `8.4.x`. Once you're on `8.4.x`, it is possible to upgrade and downgrade between `8.4.x` versions without needing to run `innodb_fast_shutdown=0`.

## <a id="minor-changes"/>Minor Changes</a>

### <a id="deletions"/>Deletions</a>

#### <a id="deleted-metrics"/>Metrics</a>

| Component |        Metric Name        | Was Deprecated In |                     Deprecation PR                      |
|:---------:|:-------------------------:|:-----------------:|:-------------------------------------------------------:|
| `vtgate`  |    `QueriesProcessed`     |     `v22.0.0`     | [#17727](https://github.com/vitessio/vitess/pull/17727) |
| `vtgate`  |      `QueriesRouted`      |     `v22.0.0`     | [#17727](https://github.com/vitessio/vitess/pull/17727) |
| `vtgate`  | `QueriesProcessedByTable` |     `v22.0.0`     | [#17727](https://github.com/vitessio/vitess/pull/17727) |
| `vtgate`  |  `QueriesRoutedByTable`   |     `v22.0.0`     | [#17727](https://github.com/vitessio/vitess/pull/17727) |

### <a id="new-metrics"/>New Metrics

#### <a id="new-vtgate-metrics"/>VTGate

|          Name           |   Dimensions    |                                     Description                                     |                           PR                            |
|:-----------------------:|:---------------:|:-----------------------------------------------------------------------------------:|:-------------------------------------------------------:|
| `TransactionsProcessed` | `Shard`, `Type` | Counts transactions processed at VTGate by shard distribution and transaction type. | [#18171](https://github.com/vitessio/vitess/pull/18171) |

### <a id="minor-changes-topo"/>Topology</a>

#### <a id="consul_auth_static_file-check-creds"/>`--consul_auth_static_file` requires 1 or more credentials</a>

The `--consul_auth_static_file` flag used in several components now requires that 1 or more credentials can be loaded from the provided json file.

### <a id="minor-changes-vtorc"/>VTOrc</a>

#### <a id="recoveries-stats-keyspace-shard">Recovery stats to include keyspace/shard</a>

The following recovery-related stats now include labels for keyspaces and shards:
1. `FailedRecoveries`
2. `PendingRecoveries`
3. `RecoveriesCount`
4. `SuccessfulRecoveries`

Previous to this release, only the recovery "type" was included in labels.

### <a id="minor-changes-vttablet"/>VTTablet</a>

#### <a id="flags-vttablet"/>CLI Flags</a>

- `skip-user-metrics` flag if enabled, replaces the username label with "UserLabelDisabled" to prevent metric explosion in environments with many unique users.

#### <a id="mysql-caching-sha2-password"/>Managed MySQL configuration defaults to caching-sha2-password</a>

The default authentication plugin for MySQL 8.0.26 and later is now `caching_sha2_password` instead of `mysql_native_password`. This change is made because `mysql_native_password` is deprecated and removed in future MySQL versions. `mysql_native_password` is still enabled for backwards compatibility.

This change specifically affects the replication user. If you have a user configured with an explicit password, it is recommended to make sure to upgrade this user after upgrading to v23 with a statement like the following:

```sql
ALTER USER 'vt_repl'@'%' IDENTIFIED WITH caching_sha2_password BY 'your-existing-password';
```

In future Vitess versions, the `mysql_native_password` authentication plugin will be disabled for managed MySQL instances.

#### <a id="mysql-timezone-env"/>MySQL timezone environment propagation</a>

Fixed a bug where environment variables like `TZ` were not propagated from mysqlctl to the mysqld process.
As a result, timezone settings from the environment were previously ignored. Now mysqld correctly inherits environment variables.
⚠️ Deployments that relied on the old behavior and explicitly set a non-UTC timezone may see changes in how DATETIME values are interpreted. To preserve compatibility, set `TZ=UTC` explicitly in MySQL pods.
