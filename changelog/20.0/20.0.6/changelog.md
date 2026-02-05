# Changelog of Vitess v20.0.6

### Bug fixes 
#### Backup and Restore
 * [release-20.0] Replace uses of os.Create with os2.Create within backup/restore workflows (#17648) [#17665](https://github.com/vitessio/vitess/pull/17665) 
#### Query Serving
 * [release-20.0] Fix panic inside schema tracker (#17659) [#17672](https://github.com/vitessio/vitess/pull/17672)
 * [release-20.0] smartconnpool: do not allow connections to starve (#17675) [#17684](https://github.com/vitessio/vitess/pull/17684) 
#### VReplication
 * [release-20.0] VReplication: Address SwitchTraffic bugs around replication lag and cancel on error (#17616) [#17643](https://github.com/vitessio/vitess/pull/17643)
 * [release-20.0] VDiff: fix race when a vdiff resumes on vttablet restart (#17638) [#17693](https://github.com/vitessio/vitess/pull/17693)
 * [release-20.0] Atomic Copy: Fix panics when the copy phase starts in some clusters (#17717) [#17747](https://github.com/vitessio/vitess/pull/17747) 
#### VTAdmin
 * [release-20.0] fix SchemaCacheConfig.DefaultExpiration (#17609) [#17611](https://github.com/vitessio/vitess/pull/17611) 
#### VTGate
 * [release-20.0] Increase health check buffer size (#17636) [#17640](https://github.com/vitessio/vitess/pull/17640)
### Dependencies 
#### Java
 * [release-20.0] Bump io.netty:netty-handler from 4.1.110.Final to 4.1.118.Final in /java (#17730) [#17732](https://github.com/vitessio/vitess/pull/17732)
### Enhancement 
#### VTAdmin
 * [release-20.0] VTAdmin: update logo and favicon for the new Vitess logos (#17715) [#17724](https://github.com/vitessio/vitess/pull/17724)
### Internal Cleanup 
#### VReplication
 * [release-20.0] Always make sure to escape all strings (#17649) [#17656](https://github.com/vitessio/vitess/pull/17656)
### Release 
#### General
 * [release-20.0] Bump to `v20.0.6-SNAPSHOT` after the `v20.0.5` release [#17598](https://github.com/vitessio/vitess/pull/17598)

