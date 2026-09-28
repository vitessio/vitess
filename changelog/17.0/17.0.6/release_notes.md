# Release of Vitess v17.0.6
## Summary

### Table of Contents

- **[Major Changes](#major-changes)**
  - **[Breaking Changes](#breaking-changes)**
    - [ExecuteFetchAsDBA rejects multi-statement SQL](#execute-fetch-as-dba-reject-multi)

## <a id="major-changes"/>Major Changes

### <a id="breaking-changes"/>Breaking Changes

#### <a id="execute-fetch-as-dba-reject-multi"/>ExecuteFetchAsDBA rejects multi-statement SQL

`vtctldclient ExecuteFetchAsDBA` (and similarly the `vtctl` and `vtctlclient` commands) now reject multi-statement SQL with error.

For example, `vtctldclient ExecuteFetchAsDBA my-tablet "stop replica; change replication source to auto_position=1; start replica` will return an error, without attempting to execute any of these queries.

Previously, `ExecuteFetchAsDBA` silently accepted multi statement SQL. It would (attempt to) execute all of them, but:

- It would only indicate error for the first statement. Errors on 2nd, 3rd, ... statements were silently ignored.
- It would not consume the result sets of the 2nd, 3rd, ... statements. It would then return the used connection to the pool in a dirty state. Any further query that happens to take that connection out of the pool could get unexpected results.
- As another side effect, multi-statement schema changes would cause schema to be reloaded with only the first change, leaving the cached schema inconsistent with the underlying database.

`ExecuteFetchAsDBA` does allow a specific use case of multi-statement SQL, which is where all statements are in the form of `CREATE TABLE` or `CREATE VIEW`. This is to support a common pattern of schema initialization.

------------
The entire changelog for this release can be found [here](https://github.com/vitessio/vitess/blob/main/changelog/17.0/17.0.6/changelog.md).

The release includes 24 merged Pull Requests.

Thanks to all our contributors: @app/vitess-bot, @frouioui, @harshit-gangal, @shlomi-noach, @systay

