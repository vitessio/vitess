# Release of Vitess v23.0.8

## Summary

### Unsupported `sql_mode` values are rejected

#### VTGate

VTGate already rejected `SET sql_mode = ...` statements that enable a mode the Vitess parser does not support (`ANSI_QUOTES`, `NO_BACKSLASH_ESCAPES`, `PIPES_AS_CONCAT`, `REAL_AS_FLOAT`). The check compared mode names textually and could be bypassed. VTGate now implements MySQL 8.x's `sql_mode` assignment semantics and validates every assignment against them:

- Setting an unsupported mode is rejected even when the underlying MySQL already runs with that mode, that is, when the `SET` would not change the value. Such statements previously succeeded, even though VTGate does not parse queries under these modes. The combination mode `ANSI` is rejected as well, because it enables `ANSI_QUOTES`, `PIPES_AS_CONCAT`, and `REAL_AS_FLOAT`.
- `IGNORE_SPACE` and `HIGH_NOT_PRECEDENCE` are now also rejected. They are the two remaining modes that change how SQL text is interpreted. The Vitess parser does not honor them, so VTGate would parse queries differently than the session's `sql_mode` promises.
- Unknown mode names and invalid numeric values fail with MySQL's own errors: `ER_WRONG_VALUE_FOR_VAR` (1231), and `ER_UNSUPPORTED_SQL_MODE` (3899) for the bits of modes removed in MySQL 8.0.
- Numeric values decode against MySQL's `sql_mode` bitmask. For example, `SET sql_mode = 1048576` reports that `NO_BACKSLASH_ESCAPES` is unsupported. Valid numeric values are accepted.
- Constant values are validated at planning time, with no shard round trip. This includes constant expressions such as `CONCAT` over literals, and it also applies when `--enable-system-settings` is disabled. Non-constant expressions are validated at execution time, once their value is known.

#### VTTablet

VTTablet applies the same validation at its own entry points and returns the same errors: connection settings, on the settings pool and on reserved connections, and session-scope `SET sql_mode` statements. This covers clients that bypass VTGate's validation, such as VTGates that are not upgraded yet and clients that talk to the query service directly. `SET_VAR` optimizer hints are forwarded to MySQL unchanged.

Every MySQL connection Vitess creates now strips the modes that change how SQL text is interpreted from the session `sql_mode` it inherits from the server's global value. All other modes, such as the strict modes and the zero-date modes, are kept. This applies to every component: query serving, schema tracking, heartbeats, replication management, Online DDL, VReplication and VDiff, and connections to external MySQL servers. The global `sql_mode` is not changed.

#### Impact

- Clients that run `SET sql_mode` with an unsupported mode, including `ANSI`, `IGNORE_SPACE` and `HIGH_NOT_PRECEDENCE`, now receive an error. This also happens when the `SET` matches the backend's current `sql_mode`. Clients that set mode names MySQL would itself reject receive an error as well.
- During a rolling upgrade, VTTablets are typically upgraded before VTGates. A session that sets a now-rejected `sql_mode` through a VTGate that is not upgraded yet starts receiving errors from upgraded VTTablets.
- A backend whose global `sql_mode` includes one of these modes keeps it, but Vitess sessions no longer inherit it.

See [#20883](https://github.com/vitessio/vitess/pull/20883) for details.

### Table ACL: callers with every role on every table may run statements whose tables cannot be determined

v23.0.7 began denying, under strict table ACL (`--queryserver-config-strict-table-acl`), statements whose tables vttablet cannot determine (`DO`, `CALL`, `REPAIR`, `OPTIMIZE`, `LOAD DATA`, and a `CREATE TABLE` that vttablet's parser cannot fully parse) for every caller outside the exempt ACL (`--queryserver-config-acl-exempt-acl`); see [GHSA-w6mx-2f8x-pqf4](https://github.com/vitessio/vitess/security/advisories/GHSA-w6mx-2f8x-pqf4). A caller that is a reader, a writer and an admin in a table group covering every table (`"table_names_or_prefixes": ["%"]`) holds every role on any table such a statement could touch, so `DO`, `REPAIR`, `OPTIMIZE` and a partially parsed `CREATE TABLE` now run for it. `CALL` and `LOAD DATA` remain denied even then, since a `SQL SECURITY DEFINER` procedure runs with its definer's privileges and `LOAD DATA INFILE` reads files on the server, neither of which table ACL grants; issue them as a caller in the exempt ACL.

These checks are counted under the `undetermined-table-set` `TableName` label, in `TableACLDenied`, or in `TableACLAllowed` for a caller with every role on every table. With dry-run (`--queryserver-config-enable-table-acl-dry-run`), only the statements the check would deny increment `TableACLPseudoDenied`.

See [#21349](https://github.com/vitessio/vitess/pull/21349) for details.

### Online DDL escapes table names in the statements it runs

Online DDL builds statements such as `DROP TABLE`, `RENAME TABLE`, `LOCK TABLES`, `SHOW CREATE TABLE` and `SHOW TABLES LIKE` from templates. Each template supplied its own quotes around the migration's table name, but the name itself was not escaped. A table name containing a back quote or single quote could close that quoting and append SQL. Several of these statements run on the tablet's DBA connection, which accepts multiple statements. Any user allowed to submit an Online DDL migration could therefore run arbitrary SQL as the DBA user. That SQL bypassed vtgate routing and table ACLs and could reach every schema on that `mysqld`. See [GHSA-8w3p-4xg5-rm9w](https://github.com/vitessio/vitess/security/advisories/GHSA-8w3p-4xg5-rm9w).

vttablet now escapes table names as identifiers and encodes string arguments as literals before writing them into these statements. The table lifecycle (table GC) statements that rename, purge and drop tables escape their table names the same way.

See [#21381](https://github.com/vitessio/vitess/pull/21381) for details.
