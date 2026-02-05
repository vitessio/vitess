# Changelog of Vitess v21.0.3

### Bug fixes 
#### Backup and Restore
 * [release-21.0] Replace uses of os.Create with os2.Create within backup/restore workflows (#17648) [#17666](https://github.com/vitessio/vitess/pull/17666) 
#### Query Serving
 * [release-21.0] Fix how we generate the query serving error documentation (#17516) [#17536](https://github.com/vitessio/vitess/pull/17536)
 * [release-21.0] Fix panic inside schema tracker (#17659) [#17673](https://github.com/vitessio/vitess/pull/17673)
 * [release-21.0] smartconnpool: do not allow connections to starve (#17675) [#17685](https://github.com/vitessio/vitess/pull/17685) 
#### VReplication
 * [release-21.0] VReplication: Address SwitchTraffic bugs around replication lag and cancel on error (#17616) [#17644](https://github.com/vitessio/vitess/pull/17644)
 * [release-21.0] VDiff: fix race when a vdiff resumes on vttablet restart (#17638) [#17694](https://github.com/vitessio/vitess/pull/17694)
 * [release-21.0] Atomic Copy: Fix panics when the copy phase starts in some clusters (#17717) [#17748](https://github.com/vitessio/vitess/pull/17748) 
#### VTAdmin
 * [release-21.0] fix SchemaCacheConfig.DefaultExpiration (#17609) [#17612](https://github.com/vitessio/vitess/pull/17612) 
#### VTGate
 * [release-21.0] Increase health check buffer size [#17636](https://github.com/vitessio/vitess/pull/17636)
### Dependencies 
#### Java
 * [release-21.0] Bump io.netty:netty-handler from 4.1.110.Final to 4.1.118.Final in /java (#17730) [#17733](https://github.com/vitessio/vitess/pull/17733)
### Enhancement 
#### VTAdmin
 * [release-21.0] VTAdmin: update logo and favicon for the new Vitess logos (#17715) [#17725](https://github.com/vitessio/vitess/pull/17725)
### Internal Cleanup 
#### VReplication
 * [release-21.0] Always make sure to escape all strings (#17649) [#17657](https://github.com/vitessio/vitess/pull/17657)
### Release 
#### General
 * [release-21.0] Bump to `v21.0.3-SNAPSHOT` after the `v21.0.2` release [#17600](https://github.com/vitessio/vitess/pull/17600)

