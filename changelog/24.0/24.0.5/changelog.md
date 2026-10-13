# Changelog of Vitess v24.0.5

### Bug fixes 
#### Authn/z
 * [release-24.0] Clear the LDAP updating latch when a refresh fails #21340 (#21341) [#21346](https://github.com/vitessio/vitess/pull/21346) 
#### Backup and Restore
 * [release-24.0] `BackupShard`: keep tablets and stats aligned when a status read fails (#21328) [#21371](https://github.com/vitessio/vitess/pull/21371) 
#### Documentation
 * [release-24.0] sqlparser: find margin comments with the parser's tokenizer (#21387) [#21542](https://github.com/vitessio/vitess/pull/21542)
 * [release-24.0] sqlparser: quote charset, collation and engine names when writing them back out (#21435) [#21544](https://github.com/vitessio/vitess/pull/21544) 
#### General
 * [release-24.0] atomic2: use acquire-release ordering for the 128-bit CAS on arm64 (#20283) [#21493](https://github.com/vitessio/vitess/pull/21493) 
#### Query Serving
 * [release-24.0] sqlerror: map ER_LOCK_NOWAIT to DEADLINE_EXCEEDED (#21325) [#21343](https://github.com/vitessio/vitess/pull/21343)
 * [release-24.0] sqlparser: escape date, time and timestamp literal values (#21389) [#21444](https://github.com/vitessio/vitess/pull/21444) 
#### VReplication
 * [release-24.0] VReplication: validate bulk-delete Before images against the field count (#20976) [#21455](https://github.com/vitessio/vitess/pull/21455) 
#### VTGate
 * [release-24.0] vtgate: reject global SET names that cannot be system variables (#21390) [#21399](https://github.com/vitessio/vitess/pull/21399)
 * [release-24.0] vtgate/buffer: do not lose the timeout thread's queue-not-empty wakeup (#21402) [#21440](https://github.com/vitessio/vitess/pull/21440)
 * [release-24.0] vtgate/buffer: create each shard buffer only once (#21403) [#21441](https://github.com/vitessio/vitess/pull/21441)
 * [release-24.0] sqlparser: rewrite `//` comments to `#/` before statement text reaches MySQL (#21410) [#21451](https://github.com/vitessio/vitess/pull/21451)
 * [release-24.0] sqltypes, vtgate, vttablet: reject bind variables that are not single SQL literals (#21386) [#21518](https://github.com/vitessio/vitess/pull/21518)
 * [release-24.0] vtgate: keep values containing */ out of the SET_VAR optimizer hint (#21388) [#21401](https://github.com/vitessio/vitess/pull/21401)
#### VTTablet
 * [release-24.0] VTTablet: let callers with every role on every table run statements whose tables cannot be determined (#21349) [#21414](https://github.com/vitessio/vitess/pull/21414)
 * [release-24.0] VTTablet: accept user variables and safe collation IDs in connection character set checks (#21465) [#21470](https://github.com/vitessio/vitess/pull/21470)
 * [release-24.0] tableacl: share the all-roles ACL of a table group across plans (#21420) [#21480](https://github.com/vitessio/vitess/pull/21480)
### Dependencies 
#### Docker
 * [release-24.0] Upgrade the Golang version to `go1.26.9` [#21496](https://github.com/vitessio/vitess/pull/21496)
### Feature 
#### VTGate
 * [release-24.0] sql_mode: reject unsupported modes at every layer, neutralize them on every connection (#20883) [#21407](https://github.com/vitessio/vitess/pull/21407)
### Internal Cleanup 
#### VTTablet
 * [release-24.0] OnlineDDL: correct why table names are escaped in generated statements (#21456) [#21472](https://github.com/vitessio/vitess/pull/21472)
### Release 
#### General
 * [release-24.0] Bump to `v24.0.5-SNAPSHOT` after the `v24.0.4` release [#21336](https://github.com/vitessio/vitess/pull/21336)
 * [release-24.0] Code Freeze for `v24.0.5` [#21555](https://github.com/vitessio/vitess/pull/21555)
### Security 
#### Query Serving
 * [release-24.0] Only accept ASCII-safe MySQL connection character sets (#21392) [#21458](https://github.com/vitessio/vitess/pull/21458)
 * [release-24.0] sqlparser: escape the user, host and engine names that SHOW statements print (#21464) [#21467](https://github.com/vitessio/vitess/pull/21467) 
#### VTAdmin
 * [release-24.0] VTAdmin: only accept POST for API routes that change cluster state (#21397) [#21482](https://github.com/vitessio/vitess/pull/21482)
 * [release-24.0] VTAdmin: check RBAC in ReloadSchemaShard and forward its keyspace and shard (#21394) [#21483](https://github.com/vitessio/vitess/pull/21483) 
#### VTTablet
 * [release-24.0] tabletserver: escape */ in the principal of query annotations (#21369) [#21416](https://github.com/vitessio/vitess/pull/21416)
 * [release-24.0] VStreamer: Validate provided LastPK value (#21377) [#21427](https://github.com/vitessio/vitess/pull/21427)
 * [release-24.0] OnlineDDL: Properly escape table identifiers (#21381) [#21429](https://github.com/vitessio/vitess/pull/21429)

