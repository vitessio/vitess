# Changelog of Vitess v19.0.1

### Bug fixes 
#### Backup and Restore
 * [release-19.0] Ensure that WithParams keeps the transport (#15421) [#15422](https://github.com/vitessio/vitess/pull/15422) 
#### Query Serving
 * [release-19.0] engine:  fix race in concatenate (#15454) [#15461](https://github.com/vitessio/vitess/pull/15461)
 * [release-19.0] Fix view tracking on sharded keyspace (#15436) [#15477](https://github.com/vitessio/vitess/pull/15477)
### CI/Build 
#### Build/CI
 * [release-19.0] Ensure to use latest golangci-lint (#15413) [#15414](https://github.com/vitessio/vitess/pull/15414)
 * [release-19.0] bump `github.com/golang/protobuf` to `v1.5.4` (#15426) [#15428](https://github.com/vitessio/vitess/pull/15428)
 * [release-19.0] Update all actions setup to latest versions (#15443) [#15446](https://github.com/vitessio/vitess/pull/15446) 
#### Online DDL
 * [release-19.0] `onlineddl_scheduler` test: fix flakiness in artifact cleanup test (#15396) [#15399](https://github.com/vitessio/vitess/pull/15399)
### Documentation 
#### Documentation
 * [release-19.0] Fix docs for unmanaged tablets (#15437) [#15474](https://github.com/vitessio/vitess/pull/15474)
### Release 
#### General
 * [release-19.0] Bump to `v19.0.1-SNAPSHOT` after the `v19.0.0` release [#15418](https://github.com/vitessio/vitess/pull/15418)
 * [release-19.0] Code Freeze for `v19.0.1` [#15481](https://github.com/vitessio/vitess/pull/15481)
### Testing 
#### Build/CI
 * [release-19.0] CI: Address data races on memorytopo Conn.closed (#15365) [#15371](https://github.com/vitessio/vitess/pull/15371)

