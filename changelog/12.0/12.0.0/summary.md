## Major Changes

This release includes the following major changes or new features.

### Inclusive Naming
A number of CLI commands and vttablet RPCs have been deprecated as we move from `master` to `primary`. 
All functionality is backwards compatible except as noted under Incompatible Changes.
Deprecated commands and flags will be removed in the next release (13.0).
Deprecated vttablet RPCs will be removed in the subsequent release (14.0).

### Gen4 Planner

The newest version of the query planner, `Gen4`, becomes an experimental feature as part of this release.
While `Gen4` has been under development for a few release cycles, we have now reached parity with its predecessor, `v3`.

To use `Gen4`, VTGate's `-planner_version` flag needs to be set to `gen4`.

### Query Buffering during failovers
In order to support buffering during resharding cutovers in addition to primary failovers, a new implementation
of query buffering has been added.
This is considered experimental. To enable it the flag `buffer_implementation` should be set to `keyspace_events`.
The existing implementation (flag value `healthcheck`) will be deprecated in a future release.

## Known Issues

- A critical vulnerability CVE-2021-44228 in the Apache Log4j logging library was disclosed on Dec 9 2021.
  The project provided release `2.15.0` with a patch that mitigates the impact of this CVE. It was quickly found that the initial patch was insufficient, and additional CVEs
  CVE-2021-45046 and CVE-2021-44832 followed.
  These have been fixed in release `2.17.1`. This release of Vitess, `v12.0.0`, uses a version of Log4j below `2.17.1`, for this reason, we encourage you to use version `v12.0.3` instead, to benefit from the vulnerability patches.

- An issue where the value of the -force flag is used instead of -keep_data flag's value in v2 vreplication workflows (#9174) is known to be present in this release. A workaround is available in the description of issue #9174. This issue is fixed in release >= `v12.0.1`.

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

### CLI commands
`VExec` is deprecated and removed. All Online DDL commands should be run through `OnlineDDL`.

`OnlineDDL revert` is deprecated. Use `REVERT VITESS_MIGRATION '...'` SQL command either via `ApplySchema` or via `vtgate`.

`InitShardMaster` is deprecated, use `InitShardPrimary` instead.

`SetShardIsMasterServing` is deprecated, use `SetShardIsPrimaryServing` instead.

Various command flags have been deprecated and new variants provided.
* `DeleteTablet` flag `allow_master` replaced with `allow_primary`
* `PlannedReparentShard` flag `avoid_master` replaced with `avoid_tablet`
* `PlannedReparentShard` flag `new_master` replaced with `new_primary`
* `BackupShard` flag `allow_master` replaced with `allow_primary`
* `Backup` flag `allow_master` replaced with `allow_primary`
* `EmergencyReparentShard` flag `new_master` replaced with `new_primary`
* `ReloadSchemeShard` flag `include_master` replaced with `include_primary`
* `ReloadSchemaKeyspace` flag `include_master` replaced with `include_primary`
* `ValidateSchemaKeyspace` flag `skip-no-master` replaced with `skip-no-primary`

