# Changelog of Vitess v19.0.10

### Bug fixes 
#### Backup and Restore
 * [release-19.0] Replace uses of os.Create with os2.Create within backup/restore workflows (#17648) [#17664](https://github.com/vitessio/vitess/pull/17664) 
#### Query Serving
 * [release-19.0] Fix panic inside schema tracker (#17659) [#17671](https://github.com/vitessio/vitess/pull/17671)
 * [release-19.0] smartconnpool: do not allow connections to starve (#17675) [#17683](https://github.com/vitessio/vitess/pull/17683) 
#### VReplication
 * [release-19.0] VReplication: Address SwitchTraffic bugs around replication lag and cancel on error (#17616) [#17642](https://github.com/vitessio/vitess/pull/17642)
 * [release-19.0] Atomic Copy: Fix panics when the copy phase starts in some clusters (#17717) [#17746](https://github.com/vitessio/vitess/pull/17746) 
#### VTGate
 * [release-19.0] Increase health check buffer size (#17636) [#17639](https://github.com/vitessio/vitess/pull/17639)
### Dependencies 
#### Java
 * [release-19.0] Bump io.netty:netty-handler from 4.1.110.Final to 4.1.118.Final in /java (#17730) [#17731](https://github.com/vitessio/vitess/pull/17731)
### Enhancement 
#### VTAdmin
 * [release-19.0] VTAdmin: update logo and favicon for the new Vitess logos (#17715) [#17723](https://github.com/vitessio/vitess/pull/17723)
### Internal Cleanup 
#### VReplication
 * [release-19.0] Always make sure to escape all strings (#17649) [#17655](https://github.com/vitessio/vitess/pull/17655)
### Release 
#### General
 * [release-19.0] Bump to `v19.0.10-SNAPSHOT` after the `v19.0.9` release [#17596](https://github.com/vitessio/vitess/pull/17596)

