# Release of Vitess v23.0.8

## Summary

### Table ACL: callers with every role on every table may run statements whose tables cannot be determined

v23.0.7 began denying, under strict table ACL (`--queryserver-config-strict-table-acl`), statements whose tables vttablet cannot determine (`DO`, `CALL`, `REPAIR`, `OPTIMIZE`, `LOAD DATA`, and a `CREATE TABLE` that vttablet's parser cannot fully parse) for every caller outside the exempt ACL (`--queryserver-config-acl-exempt-acl`); see [GHSA-w6mx-2f8x-pqf4](https://github.com/vitessio/vitess/security/advisories/GHSA-w6mx-2f8x-pqf4). A caller that is a reader, a writer and an admin in a table group covering every table (`"table_names_or_prefixes": ["%"]`) holds every role on any table such a statement could touch, so `DO`, `REPAIR`, `OPTIMIZE` and a partially parsed `CREATE TABLE` now run for it. `CALL` and `LOAD DATA` remain denied even then, since a `SQL SECURITY DEFINER` procedure runs with its definer's privileges and `LOAD DATA INFILE` reads files on the server, neither of which table ACL grants; issue them as a caller in the exempt ACL.

These checks are counted under the `undetermined-table-set` `TableName` label, in `TableACLDenied`, or in `TableACLAllowed` for a caller with every role on every table. With dry-run (`--queryserver-config-enable-table-acl-dry-run`), only the statements the check would deny increment `TableACLPseudoDenied`.

See [#21349](https://github.com/vitessio/vitess/pull/21349) for details.
