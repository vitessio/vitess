## Major Changes

This release includes the following major changes or new features.

### Inclusive Naming
A number of commands and RPCs have been deprecated as we move from `master` to `primary`. 
All functionality is backwards compatible except as noted under Incompatible Changes.

### Gen4 Planner

The newest version of the query planner, `Gen4`, becomes an experimental feature as part of this release.
While `Gen4` has been under development for a few release cycles, we have now reached parity with its predecessor, `v3`.

To use `Gen4`, VTGate's `-planner_version` flag needs to be set to `gen4`.

## Incompatible Changes

### vtctl command output
Wherever vtctl commands produced `master` or `MASTER` for tablet type, they now produce `primary` or `PRIMARY`.
Scripts and tools that depend on parsing command output will need to change.

Example:
```sh
$ vtctlclient -server localhost:15999 ListAllTablets 
zone1-0000000100 sourcekeyspace 0 primary 192.168.0.134:15100 192.168.0.134:17100 [] 2021-09-24T01:12:00Z
zone1-0000000101 sourcekeyspace 0 rdonly 192.168.0.134:15101 192.168.0.134:17101 [] <null>
zone1-0000000102 sourcekeyspace 0 replica 192.168.0.134:15102 192.168.0.134:17102 [] <null>
zone1-0000000103 sourcekeyspace 0 rdonly 192.168.0.134:15103 192.168.0.134:17103 [] <null>
```
## Default Behaviour Changes

### Enabling reserved connections
Use of reserved connections are controlled by the vtgate flag `-enable_system_settings`. This flag has traditionally defaulted to false (or off) in release versions (i.e. x.0 and x.0.y) of Vitess, and to true (or on) in development versions.

From Vitess 12.0 onwards, it defaults to ***true*** in all release and development versions. You can read more [here](https://github.com/vitessio/vitess/issues/9125). Thus you should specify this flag explicitly, so you are sure whether it is enabled or not, independent of which Vitess release/build/version you are running.

If you have reserved connections disabled, you will get the `old` Vitess behavior: where most setting most system settings (e.g. sql_mode) are just silently ignored by Vitess. In situations where you know your backend MySQL defaults are acceptable, this may be the correct tradeoff to ensure the best possible performance of the vttablet <-> MySQL connection pools. As noted above, this comes down to a trade-off between compatibility and performance/scalability. You should also review [this section](https://vitess.io/docs/reference/query-serving/reserved-conn/#number-of-vttablet---mysql-connections) when deciding on whether or not to enable reserved connections.

## Deprecations

The command `vtctl VExec` is deprecated and removed. All Online DDL commands should run through `vtctl OnlineDDL`.

The command `vtctl OnlineDDL revert` is deprecated. Use `REVERT VITESS_MIGRATION '...'` SQL command either via `vtctl ApplySchema` or via `vtgate`.
