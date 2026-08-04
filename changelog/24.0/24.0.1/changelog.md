# Changelog of Vitess v24.0.1

### Bug fixes 
#### VTGate
 * [release-24.0] planner: substitute merged DML IN/NOT IN subqueries via ListArg placeholder (#19973) [#20030](https://github.com/vitessio/vitess/pull/20030) 
#### VTOrc
 * Revert "Add flag for cells to watch to VTOrc (#19354)" [#20023](https://github.com/vitessio/vitess/pull/20023) 
#### VTTablet
 * [release-24.0] `go/mysql`, `vreplication`: fix flaky unit tests with shared root cause (#19990) [#19998](https://github.com/vitessio/vitess/pull/19998)
### Release 
#### General
 * [release-24.0] Code Freeze for `v24.0.1` [#20025](https://github.com/vitessio/vitess/pull/20025)
### Testing 
#### VTGate
 * [release-24.0] vtgate: regression tests for routing-rule + schema-tracker `t.*` expansion (#20026) [#20028](https://github.com/vitessio/vitess/pull/20028) 
#### vttestserver
 * [release-24.0] vttest: avoid 10-minute hang when vtcombo exits during startup (#20041) [#20044](https://github.com/vitessio/vitess/pull/20044)

