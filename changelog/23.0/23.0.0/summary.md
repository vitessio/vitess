## Summary

### Table of Contents
- **[Major Changes](#major-changes)**
    - **[New Join type in VTGate](#new-join-type-vtgate)**
- **[Minor Changes](#minor-changes)**
    - **[VTTablet](#minor-changes-vttablet)**
        - [CLI Flags](#flags-vttablet)
        - [Managed MySQL configuration defaults to caching-sha2-password](#mysql-caching-sha2-password)

## <a id="major-changes"/>Major Changes</a>

### <a id="new-join-type-vtgate"/>New Join type in VTGate</a>

This release introduces a new join type in vtgate: Block Joins.
Block Joins use the MySQL `VALUES` statement to send all rows coming from the left-hand-side of the join to the right-hand-side, in a single network call.
Unlike Apply Joins, which execute the right-hand-side of the join as many times as we got rows from the left-hand-side.
This new approach allows vtgate to significantly reduce the amount of network calls and improve the performance of `JOIN`s.

This new feature is experimental, unsupported queries include: DMLs, joins with more than two tables, joins with aggregation, information schema and non-normal tables.

Block Joins can be enabled by setting the `--allow-block-joins` vtgate flag, or by using the `/*vt+ ALLOW_BLOCK_JOIN */` query hint.

More information about this feature can be found in its [RFC #16508](https://github.com/vitessio/vitess/issues/16508) and
on the Pull Request implementing it [#17641](https://github.com/vitessio/vitess/pull/17641).

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