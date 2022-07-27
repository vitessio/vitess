# Release of Vitess v12.0.0
## Major Changes

This release includes the following major changes or new features.

### Inclusive Naming
A number of CLI commands and vttablet RPCs have been deprecated as we move from `master` to `primary`.
All functionality is backwards compatible except as noted under Incompatible Changes.
**Deprecated commands and flags will be removed in the next release (13.0).
Deprecated vttablet RPCs will be removed in the subsequent release (14.0).**

### Gen4 Planner
The newest version of the query planner, `Gen4`, becomes an experimental feature as part of this release.
While `Gen4` has been under development for a few release cycles, we have now reached parity with its predecessor, `v3`.

To use `Gen4`, VTGate's `-planner_version` flag needs to be set to `Gen4Fallback`.

### Query Buffering during failovers
In order to support buffering during resharding cutovers in addition to primary failovers, a new implementation
of query buffering has been added.
This is considered experimental. To enable it the flag `buffer_implementation` should be set to `keyspace_events`.
The existing implementation (flag value `healthcheck`) will be deprecated in a future release.

## Known Issues

- A critical vulnerability CVE-2021-44228 in the Apache Log4j logging library was disclosed on Dec 9 2021.
  The project provided release `2.15.0` with a patch that mitigates the impact of this CVE. It was quickly found that the initial patch was insufficient, and additional CVEs
  CVE-2021-45046 and CVE-2021-44832 followed.
  These have been fixed in release `2.17.1`. This release of Vitess, `v12.0.0`, uses a version of Log4j below `2.17.1`, for this reason, we encourage you to use version `v12.0.3` instead, to benefit from the vulnerability patches.

- An issue where the value of the -force flag is used instead of -keep_data flag's value in v2 vreplication workflows (#9174) is known to be present in this release. A workaround is available in the description of issue #9174. This issue is fixed in release >= `v12.0.1`.

## Incompatible Changes

### CLI command output
Wherever CLI commands produced `master` or `MASTER` for tablet type, they now produce `primary` or `PRIMARY`.
Scripts and tools that depend on parsing command output will need to change.

Example:
```sh
$ vtctlclient -server localhost:15999 ListAllTablets 
zone1-0000000100 sourcekeyspace 0 primary 192.168.0.134:15100 192.168.0.134:17100 [] 2021-09-24T01:12:00Z
zone1-0000000101 sourcekeyspace 0 rdonly 192.168.0.134:15101 192.168.0.134:17101 [] <null>
zone1-0000000102 sourcekeyspace 0 replica 192.168.0.134:15102 192.168.0.134:17102 [] <null>
zone1-0000000103 sourcekeyspace 0 rdonly 192.168.0.134:15103 192.168.0.134:17103 [] <null>
```

## Default Behaviour Changes

### Enabling reserved connections
Use of reserved connections is controlled by the vtgate flag `-enable_system_settings`. This flag has in the past defaulted to `false` (or `OFF`) in release versions (i.e. x.0 and x.0.y) of Vitess, and to `true` (or `ON`) in development versions.

From Vitess 12.0.0 onwards, it defaults to ***true*** in all release and development versions. You can read more about this [here](https://github.com/vitessio/vitess/issues/9125). Hence you should specify this flag explicitly, so that you are sure whether it is enabled or not, regardless of which Vitess release/build/version you are running.

If you have reserved connections disabled, you will get the ***old*** Vitess behavior: where most system settings (e.g. `sql_mode`) are just silently ignored by Vitess. In situations where you know your backend MySQL defaults are acceptable, this may be the correct setting to ensure the best possible performance of the vttablet <-> MySQL connection pools. This comes down to a trade-off between compatibility and performance/scalability. You should also review [this section](https://vitess.io/docs/reference/query-serving/reserved-conn/#number-of-vttablet---mysql-connections) of the documentation when deciding whether or not to enable reserved connections.

## Deprecations

### CLI commands
`VExec` is deprecated and removed. All Online DDL commands should be run through `OnlineDDL`.

`OnlineDDL revert` is deprecated. Use `REVERT VITESS_MIGRATION '...'` SQL command either via `ApplySchema` or via `vtgate`.

`InitShardMaster` is deprecated, use `InitShardPrimary` instead.

`SetShardIsMasterServing` is deprecated, use `SetShardIsPrimaryServing` instead.

Various command flags have been deprecated and new variants provided.
* `DeleteTablet` flag `allow_master` replaced with `allow_primary`
* `PlannedReparentShard` flag `avoid_master` replaced with `avoid_tablet`
* `PlannedReparentShard` flag `new_master` replaced with `new_primary`
* `BackupShard` flag `allow_master` replaced with `allow_primary`
* `Backup` flag `allow_master` replaced with `allow_primary`
* `EmergencyReparentShard` flag `new_master` replaced with `new_primary`
* `ReloadSchemeShard` flag `include_master` replaced with `include_primary`
* `ReloadSchemaKeyspace` flag `include_master` replaced with `include_primary`
* `ValidateSchemaKeyspace` flag `skip-no-master` replaced with `skip-no-primary`

## Minor Changes

### Query Serving
* Add `SHOW VITESS_REPLICATION_STATUS` Query Support #8900
  * The new `SHOW VITESS_REPLICATION_STATUS` command/query shows the MySQL replica/replication (not `vreplication`) health for the vitess deployment.

## Governance
* Added @GuptaManan100 to maintainers #8833

------------
## Changelog

### Bug fixes
#### Build/CI
* Update golang/x dependencies #8688
* tests: use AtomicInt32 instead of int to fix races #8696
#### Cluster management
* Use consul lock properly #8310
* Trivial: mysqlctl reinit_config -h  was not showing help #8369
* Port #8422 to main branch #8745
* [vtctl] Add missing call to `flag.Parse` in `commandGetRoutingRules` #8795
* Fix for padding in OrderAndCheckPartitions  #8873
* tabletmanager: correct implementations of deprecated RPCs for backwards compatibility #9059
#### General
* Avoid some pollution of CLI argument namespace across vtgate and vttablet #9026
#### Observability
* schema engine: reset table size stats when a table is dropped #8634
#### Query Serving
* Fix for function calls in DEFAULT value of CREATE TABLE statement in main #8477
* Fixing multiple issues related to onlineddl/lifecycle #8500
* boolean values should not be parenthesised in default clause #8502
* mysql/server: abort connection if secure transport is required #8504
* Clear only the current shard's schemacopy rows  during schema reload #8519
* go/mysql/auth: fix error message for wrong auth method. #8525
* Fix handshake protocol with MySQL client #8537
* Copy parameter data from byte buffer in COM_STMT_SEND_LONG_DATA #8562
* Negative float default #8587
* [vtexplain] prevent panic on show tables/show full tables #8601
* OnlineDDL: better scheduling/cancellation logic #8603
* Fix parsing of FIRST clause in ALTER TABLE #8614
* onlineddl Executor: build schema with DBA user #8624
* Handle subquery merging with references correctly #8662
* Fixing error on deletes from owning table for a lookup being populated by CreateLookupVindex after a SwitchWrites #8701
* Fixing a panic in vtgate with OLAP mode #8722
* default to primary tablet if not set in VStream api #8755
* Fix bug in parsing of version comments #8770
* Proper merge of the SysTableTableName fields when joining two routes #8771
* fix parsing of MySQL Server Version flag #8791
* Fix typo in pool config syntax.  Fixes #8819 #8820
* VtGate schema tracker to use provided user #8857
* fix merging dba query as a subquery both in v3 and gen4 #8871
* sql_select_limit support #9027
* [12.0] Handle sql_mode setting to compare modes in any order #9041
* [12.0] Fix for schema tracker not recognizing RENAME for schema reloading #9043
* [12.0] Fix savepoint support with reserved connections #9047
* [12.0] Gen4: Handle single column vindex correctly with multi column #9061
#### VReplication
* MoveTables: don't create unnecessary streams on the target for non-intersecting sources and targets #8090
* VDiff: Add BIT datatype to list of byte comparable types #8401
* Fix vreplication error metric #8483
* Return from throttler goroutine if context is cancelled to prevent goroutine leaks #8489
* Fix VReplication logging to file and db #8521
* Fix passing the wrong cell/cells variable to CreateLookupVindex #8590
* Refresh SrvVSchema after an ExternalizeVindex: was missing #8670
* Fixing missing argument in function call #8692
* Fix how we identify MySQL generated columns #8763
#### VTAdmin
* [vtadmin-web] Set Backup status indicators to the correct colours  #8410
* [vtadmin] Fix bad copy-paste in pool config flag parsing #8518
#### vtctl
* Print actual current datetime for vtctl dry run commands #8778
#### vttestserver
* Remove lingering mysqld sock files on vttestserver container startup #8463
* Fixed filename for the configuration file in Vttestserver #8809
### CI/Build
#### Build/CI
* Update release process #7759
* [ci] Add `errcheck` to golangci-lint #7996
* gomod: do not replace GRPC #8416
* consolidation: Fix flaky test #8417
* Small build improvements #8418
* proto: upgrade vtprotobuf version #8454
* Automatically use the latest tag of Vitess for cluster upgrade E2E test #8471
* Addition of known issues to release notes #8482
* Fix Cluster 14 flakiness #8494
* add question on backporting back to PR template #8541
* Enable GitHub Actions concurrency feature #8546
* Updated Makefile do_release script to include godoc steps #8550
* Changing codahale/hdrhistogram import path #8637
* Upgrade to Go 1.17 #8640
* Fixes for reparent endtoend test flakiness #8642
* hooks: remove govet because it already runs as part of golangci-lint #8674
* Enhancement of the release instructions #8790
* Remove consul-api usage in favor of official consul/api #8794
* Enhancement of the release notes generation #8877
#### Governance
* update MAINTAINERS and CODEOWNERS for deepthi, pH14 and vmg #8675
#### Query Serving
* Cleanup: import of gh-ost test suite is complete #8459
#### VReplication
* VReplication support for non-PRIMARY KEY (Online DDL context) #8364
### Documentation
#### Build/CI
* maintainers: add Messaging as area of expertise #8578
#### Cluster management
* Enhance k8stopo flag documentation  #8464
* Update some vtctl/vtctlclient command help #8560
* Trivial:  fix the GenerateShardRanges vtctl[client] help #8586
#### Examples
* examples: update README to use new vreplication command syntax #8825
#### Governance
* update governance #8609
#### Query Serving
* vtexplain examples #8652
#### VReplication
* vtctl help/usage update #8879
#### VTAdmin
* [vtadmin] Remove outdated commands from vtadmin-web README #8496
* Initial pass at some vtadmin docs #8526
### Enhancement
#### Backup and Restore
* Add Support for Restoring Specific Backups #8824
#### Build/CI
* Initial support for stress testing in end-to-end tests #8406
* CI: Remove mariadb101 which is well past eol and no longer available in distributions #8446
* Adding GitHub Self Hosted Runner tests #8721
#### Cluster management
* Vitess mixin improvements #7499
* Expose topo_consul_lock_session_checks to allow customized consul session check #8298
* Handle lock release with SIGHUP in VTGR #8472
* Enhance PRS error message #8529
* srvtopo: resilient watcher #8583
* Add `-restart_before_backup` parameter for vtbackup #8608
* srvtopo: allow unwatching from watch callbacks #8633
* introduced cluster_operation as a new error code in vtrpc #8646
* servenv: add `--onclose_timeout` flag #8651
* Improve determinism of bootstrap #8840
* OnlineDDL in v12: remove legacy code deprecated in earlier version #8972
#### Observability
* [vtadmin] cluster debug pages #8427
* Export memstats for statsd backend #8777
#### Query Serving
* [tablet, queryrules] Extend query rules to check comments #8233
* Add "show global status" support #8344
* added batch lookup param to lookup vindex #8398
* Online DDL/VReplication: able to read from replica #8405
* multishard autocommit should work for bypass routing #8428
* Add "no-scatter" flag to prohibit the use of scatter queries #8439
* OnlineDDL: -skip-topo is always 'true' #8450
* Add `rows` as keyword in sqlparser #8467
* Periodic update of gh-ost binary #8470
* fix when show tables record too much error log "Got unhandled packet.." #8478
* SHOW VITESS_MIGRATION '...' LOGS, retain logs for 24 hours #8493
* NativeDDL: analyzing added&removed unique keys #8495
* Refactor authentication server plugin mechanism #8503
* Update to planetscale/tengo v0.10.1-ps.v4 #8516
* query serving to continue when topo server restarts - main #8534
* support grpc reflection on APIs #8551
* Customized CreateLookUpVindex to be Compatible with non-consistent Lookup Vindex #8570
* Add additional options for configuring SSL modes as a client #8588
* Fail plan for unsupported aggregate function #8593
* gateway: use keyspace events when buffering requests #8700
* Add optional query annotations, i.e. prefix SQL comments indicating the #8783
* Slight improvement of the ACL error message #8805
* Changed parsing for unions #8821
* Use vt_dba user for online schema migration cutover phase #8836
* Add parsing support for column list with subquery alias #8884
* Add planner-version flag to vtexplain #8979
* [Backport 12.0]: Add planner-version flag to vtexplain #9003
#### TabletManager
* Allow min TLS version for tablet to mysqld conns #8757
#### VReplication
* VReplication: Add ability to tag workflows #8388
* Ignore SBR statements from pt-table-checksum #8396
* Tablet Picker: add metric to record lack of available tablets #8403
* VReplication: support different keys on source and target tables (different covered columns) #8423
* VStream API: handle reparenting and unhealthy tablets #8445
* Change local example to use v2 vreplication flows and change default to v2 from v1 #8553
* Customized CreateLookUpVindex to Include a Flag to Continue Vreplication After Backfill with an Owner Provided #8572
* Update MoveTables CLI help to reflect change we made at the beginning of #8597
* VStreamer Field/Row Events: add Keyspace/Shard #8598
* VStream API: Add flag to stop streaming on a reshard #8628
#### VTAdmin
* [vtadmin] cluster rpc pools #8421
* Add support for passing custom interceptors to vtadmin grpcserver #8507
### Feature Request
#### Cluster management
* [vtctldserver] Migrate remaining ServingGraph rpcs to VtctldServer #8249
* [grpctmclient] Add support for (bounded) per-host connection reuse #8368
* VTGR: Vitess + MySQL group replication #8387
* Add isActive flag to vtgr to support multi-cell topology #8780
* [vtctld] Add `SetWritable`, `StartReplication` and `StopReplication` rpcs #8816
* [vtctld] sleep/ping tablets #8826
* [vtctld] migrate more util rpcs #8843
* [vtctld] migrate validator rpcs #8849
* [vtctld] localvtctldclient #8882
* [vtctldserver] Migrate RunHealthCheck #8892
* [vtctl] run new commands as standalone binary #8893
#### VTAdmin
* [vtadmin] [experimental] add per-api RBAC implementation #8515
* [vtadmin] shard replication positions #8775
* [vtadmin] GetVtctlds #8792
### Internal Cleanup
#### Build/CI
* Bump aws-sdk-go to v1.34.2 #8632
* Upgrade consul api: `go get github.com/hashicorp/consul/api@v1.10.1` #8784
#### Cluster management
* topodata: remove deprecated field 'ServedTypes' #8566
* Upgrade to k8s 1.18 client #8762
* [vtctl] command deprecations #8967
#### Deployments
* Remove the deprecated Helm charts and related code #8868
#### Examples
* Bump ini from 1.3.5 to 1.3.8 in /vitess-mixin/e2e #8823
* backport operator updates to v12 branch . #9057
#### General
* Migrate k8s topo CRD to v1 api (v12 branch) #9046
#### Governance
* fix copyright #8611
#### Query Serving
* Adds flag to vttablet to disallow online DDL statements #8433
* Remove vtrpc.LegacyErrorCode #8456
* Allow for configuration of the minimal TLS version #8460
* engine: allow retrying partial primitives #8727
* srvtopo: expose WatchSrvKeyspace #8752
* Clean up Primitive interface implementations #8901
#### VReplication
* LegacySplitCloneWorker is no longer used in any code paths #8867
#### VTAdmin
* [vtadmin] Add Options struct to wrap grpc/http options #8461
* Update vtadmin local scripts to enable basic rbac #8801
#### vtctl
* Correctly identify backup timestamp var as a string #8891
### Other
#### Examples
* Improve the Docker local and compose examples #8685
### Performance
#### Cluster management
* throttler: don't allocate any resources unless it is actually enabled #8643
#### Query Serving
* cache: track memory usage more closely #8804
#### VTAdmin
* Use `fmt.Fprintf` instead of `fmt.Sprintf` #8922
### Testing
#### Cluster management
* Alter tests to reuse cluster components #8276
* Adds some more orchestrator tests to vtorc #8535
#### Query Serving
* increase conn killer check to double the tx timeout value #8649
* Added UNION testcases and auxiliary code for running tests #8797
* fixed regression in v3 for grouping by integer functions #8856
#### VTAdmin
* [vtadmin] Add a vtctld Dialer unit test #8455


The release includes 1351 commits (excluding merges)

Thanks to all our contributors: @Anders-PlanetScale, @GuptaManan100, @Johnny-Three, @Juneezee, @ajm188, @aquarapid, @askdba, @bnu0, @carsonoid, @choo-stripe, @dbussink, @dctrwatson, @deepthi, @demmer, @dependabot[bot], @derekperkins, @doeg, @eeSeeGee, @falun, @fatih, @frouioui, @hallaroo, @harshit-gangal, @ilikeorangutans, @mattlord, @rafael, @ritwizsinha, @rohit-nayak-ps, @sahutd, @shlomi-noach, @sonne5, @systay, @tdakkota, @tokikanno, @utk9, @vmg