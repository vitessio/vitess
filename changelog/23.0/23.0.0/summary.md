## Summary

### Table of Contents
- **[Major Changes](#major-changes)**
  - **[VTOrc](#vtorc)**
    - [Support dynamic control of ERS by keyspace/shard in VTOrc](#vtorc-dynamic-ers-disabled)
- **[Minor Changes](#minor-changes)**
  - **[VTTablet](#minor-changes-vttablet)**
    - [CLI Flags](#flags-vttablet)
    - [Managed MySQL configuration defaults to caching-sha2-password](#mysql-caching-sha2-password)

## <a id="major-changes"/>Major Changes</a>

### <a id="vtorc"/>VTOrc</a>

#### <a id="vtorc-dynamic-ers-disabled"/>Support dynamic control of ERS by keyspace/shard in VTOrc</a>

**Note: disabling `EmergencyReparentShard`-based recoveries introduces availability risks; please use with extreme caution! If you rely on this functionality often, for example in automation, this may be signs of an anti-pattern. If so, please open an issue to discuss supporting your use case natively in VTOrc.**

The new `vtctldclient` RPC `SetVtorcEmergencyReparent` was introduced to allow VTOrc recoveries involving `EmergencyReparentShard` actions to be disabled on a per-keyspace or per-shard basis. Previous to this version, disabling ERS-based recoveries was only possible globally/per-VTOrc-instance. VTOrc will now consider this keyspace/shard-level setting that is refreshed from the topo on each recovery.

To provide observability of keyspace/shards with ERS-based VTOrc recoveries disabled, the label `ErsDisabled` was added to the `TabletsWatchedByShard` metric. This metric label can be used to create alerting to ensure ERS-based recoveries are not disabled for an undesired period of time.

---

## <a id="minor-changes"/>Minor Changes</a>

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
