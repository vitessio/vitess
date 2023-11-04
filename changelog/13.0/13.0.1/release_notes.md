# Release of Vitess v13.0.1
## Changelog

### Bug fixes
#### Cluster management
* Fix the race between PromoteReplica and replication manager tick #9859
#### Query Serving
* Fix `__sq_has_values1` error with `PulloutSubquery` #9864
* Fix planner panic on derived tables sorting in query builder #9959
* Fix make concatenate and limit concurrent safe #9981
* Fix reserved connection retry logic when vttablet or mysql is down #10005
* Fix Gen4 sub query planning when the outer query is a dual query #10007
* Fix parsing of bind variables in a few places #10015
* Fix route explain tab plan to the proper Keyspace #10029
* Fix Sequence query to ignore reserved and transaction #10054
* Fix dual query with exists clause having system table query in it #10055
* Fix Gen4 only_full_group_by regression #10079
#### VReplication
* VPlayer use stored/binlogged ENUM index value in WHERE clauses #9868
### CI/Build
#### Security
* Upgrade to `go1.17.9` #10088
  The `go1.17.9` version, released 2022-04-12, includes security fixes to the `crypto/elliptic` and `encoding/pem` packages, as well as bug fixes to the linker and runtime). More information [here](https://go.dev/doc/devel/release#go1.17.minor).
### Enhancement
#### Query Serving
* Fix allow multiple distinct columns on single shards #10047
* Fix add parsing for NOW in DEFAULT clause #10085


The release includes 30 commits (excluding merges)

Thanks to all our contributors: @GuptaManan100, @frouioui, @harshit-gangal, @mattlord, @rohit-nayak-ps, @systay, @vmg
