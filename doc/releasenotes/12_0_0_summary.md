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

## Incompatible Changes

### CLI command output
Wherever CLI commands produced `master` or `MASTER` for tablet type, they now produce `primary` or `PRIMARY`.
Scripts and tools that depend on parsing command output will need to change.

Example:
```sh
$ vtctlclient -server localhost:15999 ListAllTablets 
zone1-0000000100 sourcekeyspace 0 primary 192.168.0.134:15100 192.168.0.134:17100 [] 2021-09-24T01:12:00Z
zone1-0000000101 sourcekeyspace 0 rdonly 192.168.0.134:15101 192.168.0.134:17101 [] <null>
zone1-0000000102 sourcekeyspace 0 replica 192.168.0.134:15102 192.168.0.134:17102 [] <null>
zone1-0000000103 sourcekeyspace 0 rdonly 192.168.0.134:15103 192.168.0.134:17103 [] <null>
```

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

