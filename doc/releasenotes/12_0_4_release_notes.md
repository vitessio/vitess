# Release of Vitess v12.0.4
## Changelog

### Bug fixes
#### Cluster management
* Fix the race between PromoteReplica and replication manager tick #9859
#### Query Serving
* Route explain tab plan to the proper Keyspace #10028
* Diverse fixes to UNION planning and execution #10344


The release includes 5 commits (excluding merges)

Thanks to all our contributors: @GuptaManan100, @frouioui, @systay