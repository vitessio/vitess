## Summary

- [New command line flags and behavior](#new-command-line-flags-and-behavior)

## Known Issues

## Major Changes

### New command line flags and behavior

#### VTTablet: Initializing all replica DB with super_read_only
In order to prevent SUPER privileged users like `root` or `vt_dba` to produce errant GTIDs anywhere anytime, all the replica DBs are initialized with the Mysql 
global variable `super_read_only` value set to `ON`. During re-parenting, we set `super_read_only` to `OFF` for the promoted primary tablet. This will allow the 
primary to accept writes. All replica except the primary will still have their global variable `super_read_only` set to `ON`. This will make sure that apart from
the replication no other component or offline system can mutate replica DB resulting in errant GTIDs that are then lying in wait to cause later failures.

Reference PR for this change is [PR #12206](https://github.com/vitessio/vitess/pull/12206)
