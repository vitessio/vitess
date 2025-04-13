## Summary

### Table of Contents
- **[Minor Changes](#minor-changes)**
    - [`--consul_auth_static_file` requires 1 or more credentials](#consul_auth_static_file-check-creds)
    - **[VTTablet](#minor-changes-vttablet)**
        - [CLI Flags](#flags-vttablet)
        - [Managed MySQL configuration defaults to caching-sha2-password](#mysql-caching-sha2-password)

## <a id="minor-changes"/>Minor Changes</a>

### <a id="consul_auth_static_file-check-creds"/>`--consul_auth_static_file` requires 1 or more credentials</a>

The `--consul_auth_static_file` flag used in several components now requires that 1 or more credentials can be loaded from the provided json file.

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
