# Release of Vitess v12.0.4
## Major Changes

### Cluster management

A race condition #9819 that happens when running `PlannedReparentShard` was fixed through #9859.

### Query Serving

Two major bugs on `UNION` are fixed via this release. The first one, #10257, solves an issue around the `UNION` executor,
where the execution context was not handled properly in a concurrent environment. The second one, #9945, was detected by
one of our `UNION` tests becoming flaky in the CI, it got solved by #9979, which focuses on improving the concurrency of `UNION`
executions.

A panic when ordering results in descending order on a `hash` vindex is also fixed. The original issue can be found here #10019. 

## Changelog

### Bug fixes
#### Cluster management
* Fix the race between PromoteReplica and replication manager tick #9859
#### Query Serving
* Route explain table plan to the proper Keyspace #10028
* Multiple fixes to UNION planning and execution #10344


The release includes 5 commits (excluding merges)

Thanks to all our contributors: @GuptaManan100, @frouioui, @systay