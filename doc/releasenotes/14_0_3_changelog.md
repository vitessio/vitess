# Changelog of Vitess v14.0.3

### Bug fixes 
#### Query Serving
 * [release-14.0] bugfix: Truncate columns even when sorting on vtgate (#11265) [#11324](https://github.com/vitessio/vitess/pull/11324)
 * [release-14.0] Fix complex predicates being pulled into `ON` conditions for `LEFT JOIN` statements [#11333](https://github.com/vitessio/vitess/pull/11333)
 * Fix: DML engine multiequal support [#11395](https://github.com/vitessio/vitess/pull/11395) 
#### VReplication
 * Backport-v14: VReplication: Handle DECIMAL 0 Value Edge Case #11212 [#11232](https://github.com/vitessio/vitess/pull/11232) 
#### VTorc
 * Fix VTOrc Discovery to also retry discovering tablets which aren't present in database_instance table [#10662](https://github.com/vitessio/vitess/pull/10662)
### Release 
#### Documentation
 * Update the release documentation [#11174](https://github.com/vitessio/vitess/pull/11174)
 * Add hyperlink in the release changelog [#11241](https://github.com/vitessio/vitess/pull/11241)
 * Addition of the release summary for `v14.0.3` [#11396](https://github.com/vitessio/vitess/pull/11396) 
#### General
 * Release `v14.0.2` [#11159](https://github.com/vitessio/vitess/pull/11159)
 * [release-14.0] Simple code freeze script and workflow (#11178) [#11198](https://github.com/vitessio/vitess/pull/11198)
 * [release-14.0] Improve the `do_release` script to have two different Pull Requests instead of one during a release [#11230](https://github.com/vitessio/vitess/pull/11230)

