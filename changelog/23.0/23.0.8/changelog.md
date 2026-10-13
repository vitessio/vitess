# Changelog of Vitess v23.0.8

### Bug fixes 
#### Authn/z
 * [release-23.0] Clear the LDAP updating latch when a refresh fails #21340 (#21341) [#21345](https://github.com/vitessio/vitess/pull/21345) 
#### Backup and Restore
 * [release-23.0] `BackupShard`: keep tablets and stats aligned when a status read fails (#21328) [#21370](https://github.com/vitessio/vitess/pull/21370) 
#### Documentation
 * [release-23.0] sqlparser: find margin comments with the parser's tokenizer (#21387) [#21541](https://github.com/vitessio/vitess/pull/21541)
 * [release-23.0] sqlparser: quote charset, collation and engine names when writing them back out (#21435) [#21543](https://github.com/vitessio/vitess/pull/21543) 
#### General
 * [release-23.0] atomic2: use acquire-release ordering for the 128-bit CAS on arm64 (#20283) [#21492](https://github.com/vitessio/vitess/pull/21492) 
#### Query Serving
 * [release-23.0] sqlerror: map ER_LOCK_NOWAIT to DEADLINE_EXCEEDED (#21325) [#21342](https://github.com/vitessio/vitess/pull/21342)
 * [release-23.0] sqlparser: escape date, time and timestamp literal values (#21389) [#21443](https://github.com/vitessio/vitess/pull/21443) 
#### VReplication
 * [release-23.0] VReplication: validate bulk-delete Before images against the field count (#20976) [#21454](https://github.com/vitessio/vitess/pull/21454) 
#### VTGate
 * [release-23.0] vtgate: reject global SET names that cannot be system variables (#21390) [#21398](https://github.com/vitessio/vitess/pull/21398)
 * [release-23.0] vtgate/buffer: do not lose the timeout thread's queue-not-empty wakeup (#21402) [#21439](https://github.com/vitessio/vitess/pull/21439)
 * [release-23.0] sqlparser: rewrite `//` comments to `#/` before statement text reaches MySQL (#21410) [#21450](https://github.com/vitessio/vitess/pull/21450)
 * [release-23.0] sqltypes, vtgate, vttablet: reject bind variables that are not single SQL literals (#21386) [#21517](https://github.com/vitessio/vitess/pull/21517) 
#### VTTablet
 * [release-23.0] VTTablet: let callers with every role on every table run statements whose tables cannot be determined (#21349) [#21413](https://github.com/vitessio/vitess/pull/21413)
 * [release-23.0] VTTablet: accept user variables and safe collation IDs in connection character set checks (#21465) [#21469](https://github.com/vitessio/vitess/pull/21469)
 * [release-23.0] tableacl: share the all-roles ACL of a table group across plans (#21420) [#21478](https://github.com/vitessio/vitess/pull/21478)
### Compatibility Bug 
#### Query Serving
 * [release-23.0] sqlparser: replace nested tokenizer with inline versioned comment scanning (#19725) [#21408](https://github.com/vitessio/vitess/pull/21408)
### Feature 
#### VTGate
 * [release-23.0] sql_mode: reject unsupported modes at every layer, neutralize them on every connection (#20883) [#21385](https://github.com/vitessio/vitess/pull/21385)
### Internal Cleanup 
#### VTTablet
 * [release-23.0] OnlineDDL: correct why table names are escaped in generated statements (#21456) [#21471](https://github.com/vitessio/vitess/pull/21471)
### Release 
#### General
 * [release-23.0] Bump to `v23.0.8-SNAPSHOT` after the `v23.0.7` release [#21338](https://github.com/vitessio/vitess/pull/21338)
 * [release-23.0] Code Freeze for `v23.0.8` [#21553](https://github.com/vitessio/vitess/pull/21553)
### Security 
#### Query Serving
 * [release-23.0] Only accept ASCII-safe MySQL connection character sets (#21392) [#21457](https://github.com/vitessio/vitess/pull/21457)
 * [release-23.0] sqlparser: escape the user, host and engine names that SHOW statements print (#21464) [#21466](https://github.com/vitessio/vitess/pull/21466) 
#### VTAdmin
 * [release-23.0] VTAdmin: only accept POST for API routes that change cluster state (#21397) [#21479](https://github.com/vitessio/vitess/pull/21479)
 * [release-23.0] VTAdmin: check RBAC in ReloadSchemaShard and forward its keyspace and shard (#21394) [#21481](https://github.com/vitessio/vitess/pull/21481) 
#### VTTablet
 * [release-23.0] tabletserver: escape */ in the principal of query annotations (#21369) [#21415](https://github.com/vitessio/vitess/pull/21415)
 * [release-23.0] VStreamer: Validate provided LastPK value (#21377) [#21426](https://github.com/vitessio/vitess/pull/21426)
 * [release-23.0] OnlineDDL: Properly escape table identifiers (#21381) [#21428](https://github.com/vitessio/vitess/pull/21428)

