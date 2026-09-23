# Changelog of Vitess v23.0.6

### Bug fixes 
#### Build/CI
 * [release-23.0] test: fix topo-flavor e2e shards silently running zero tests (#20556) [#20636](https://github.com/vitessio/vitess/pull/20636) 
#### Documentation
 * [release-23.0] `reparentutil`: order reparent candidates by GTID dominance for a consistent sort (#20728) [#20757](https://github.com/vitessio/vitess/pull/20757) 
#### Query Serving
 * [release-23.0] mysql/json: fix MarshalTo discarding accumulated output for nested blob and bit values (#20625) [#20655](https://github.com/vitessio/vitess/pull/20655) 
#### VDiff
 * [release-23.0] VDiff: stop pre-seeding a bogus category in TableDiffPhaseTimings (#20713) [#20716](https://github.com/vitessio/vitess/pull/20716)
 * [release-23.0] Properly handle vstream filter predicates with multi-col PKs (#20858) [#20864](https://github.com/vitessio/vitess/pull/20864) 
#### VReplication
 * [release-23.0] vstreamer: don't drop a SET column's 64th member (#20450) [#20455](https://github.com/vitessio/vitess/pull/20455)
 * [release-23.0] VReplication: reconcile in-memory workflow state metric from the persisted row (#20442) [#20500](https://github.com/vitessio/vitess/pull/20500)
 * [release-23.0] binlog: escape JSON diff paths when generating diff SQL (#20461) [#20528](https://github.com/vitessio/vitess/pull/20528)
 * [release-23.0] workflow: escape tenant id and column name in getTenantClause (#20413) [#20532](https://github.com/vitessio/vitess/pull/20532)
 * [release-23.0] VReplication: Avoid sending mixed batch of row changes to bulk insert or bulk delete in vplayer batch mode (#20565) [#20587](https://github.com/vitessio/vitess/pull/20587)
 * [release-23.0] VReplication: only build a bulk-delete plan for insertNormal table plans (#20889) [#20920](https://github.com/vitessio/vitess/pull/20920)
 * [release-23.0] VReplication: don't count throttled time in the vplayer stall deadline (#20925) [#20949](https://github.com/vitessio/vitess/pull/20949) 
#### VTGate
 * [release-23.0] Preserve query hints in field/impossible queries (#20366) [#20398](https://github.com/vitessio/vitess/pull/20398)
 * [release-23.0] Enforce recursion-depth limit on the streaming recursive CTE path (#20432) [#20468](https://github.com/vitessio/vitess/pull/20468)
 * [release-23.0] vtgate: copy bindVars per source in streaming Concatenate (#20436) [#20490](https://github.com/vitessio/vitess/pull/20490)
 * [release-23.0] evalengine: don't append a NUL byte to TO_BASE64 output at exact 57 byte multiples (#20474) [#20502](https://github.com/vitessio/vitess/pull/20502)
 * [release-23.0] mysql: encode zero DATE/DATETIME/TIMESTAMP values as zero-length in the binary protocol (#20460) [#20506](https://github.com/vitessio/vitess/pull/20506)
 * [release-23.0] vtgate: copy execute options per scatter call for FetchLastInsertId (#20439) [#20523](https://github.com/vitessio/vitess/pull/20523)
 * [release-23.0] vtgate: do not autocommit per-chunk in streaming insert-select (#20497) [#20525](https://github.com/vitessio/vitess/pull/20525)
 * [release-23.0] evalengine: round DOUBLE ties half to even in ROUND() like MySQL (#20476) [#20530](https://github.com/vitessio/vitess/pull/20530)
 * [release-23.0] grpcvtgateconn: stop mutating shared dial options slice (#20580) [#20583](https://github.com/vitessio/vitess/pull/20583)
 * [release-23.0] Fix ExecuteMulti timeout context reuse (#20445) [#20606](https://github.com/vitessio/vitess/pull/20606)
 * [release-23.0] vtgate: support prepared statements with a leading WITH clause (#20665) [#20705](https://github.com/vitessio/vitess/pull/20705)
 * [release-23.0] evalengine: stop RPAD growing its operand in place (#20697) [#20707](https://github.com/vitessio/vitess/pull/20707)
 * [release-23.0] evalengine: stop the binary bitwise operators writing over and marking their result (#20696) [#20709](https://github.com/vitessio/vitess/pull/20709)
 * [release-23.0] evalengine: collapse the VM stack when a later LOCATE or SUBSTRING operand is NULL (#20645) [#20714](https://github.com/vitessio/vitess/pull/20714) 
#### VTTablet
 * [release-23.0] vttablet: roll back canceled schema reload on a non-cancelable context (#20385) [#20400](https://github.com/vitessio/vitess/pull/20400)
 * [release-23.0] vreplication: prevent vttablet panic on malformed `RowChange` images (#20377) [#20408](https://github.com/vitessio/vitess/pull/20408)
 * [release-23.0] Enforce stored-procedure safety checks on the streaming CALL path (#20372) [#20423](https://github.com/vitessio/vitess/pull/20423)
 * [release-23.0] txthrottler: don't panic on target_replication_lag_sec of 1 (#20554) [#20558](https://github.com/vitessio/vitess/pull/20558)
 * [release-23.0] vttablet: perform an initial heartbeat read when opening the heartbeat reader (#20868) [#20905](https://github.com/vitessio/vitess/pull/20905) 
#### vtctl
 * [release-23.0] `reparentutil`: keep nil-alias tablets out of candidate ordering (#20762) [#20767](https://github.com/vitessio/vitess/pull/20767)
### CI/Build 
#### Build/CI
 * [release-23.0] ci: fix mysql57 setup by bumping removed libtinfo5 pin, fail fast on download errors (#20481) [#20483](https://github.com/vitessio/vitess/pull/20483)
 * [release-23.0] ci: fix self-references in query_serving_queries_2 change-detection filters (#20482) [#20488](https://github.com/vitessio/vitess/pull/20488)
 * [release-23.0] ci: try each resolved IP when downloading from archive.ubuntu.com in setup-mysql (#20539) [#20543](https://github.com/vitessio/vitess/pull/20543)
### Compatibility Bug 
#### VTGate
 * [release-23.0] evalengine: use MySQL 1-based ordinal for ENUM in numeric context (#20454) [#20458](https://github.com/vitessio/vitess/pull/20458)
### Dependencies 
#### Docker
 * [release-23.0] Upgrade the Golang version to `go1.25.12` [#20519](https://github.com/vitessio/vitess/pull/20519)
 * [release-23.0] Upgrade the Golang version to `go1.25.13` [#20835](https://github.com/vitessio/vitess/pull/20835)
### Enhancement 
#### Backup and Restore
 * [release-23.0] mysqlctl: force TZ=UTC for mysqlbinlog during point-in-time restore (#20463) [#20508](https://github.com/vitessio/vitess/pull/20508) 
#### Documentation
 * [release-23.0] go/mysql: streaming errors no longer surface as connection loss (#20383) [#20540](https://github.com/vitessio/vitess/pull/20540) 
#### VTGate
 * [release-23.0] `go/mysql`: send ERR instead of teardown after an OK carrying `SERVER_MORE_RESULTS_EXISTS` (#20563) [#20666](https://github.com/vitessio/vitess/pull/20666)
### Internal Cleanup 
#### General
 * [release-23.0] Update maintainers and code owners project lists (#20772) [#20821](https://github.com/vitessio/vitess/pull/20821)
### Release 
#### General
 * [release-23.0] Code Freeze for `v23.0.6` [#20992](https://github.com/vitessio/vitess/pull/20992)
### Security 
#### Build/CI
 * [release-23.0] CI: pass GitHub context to run scripts via env vars instead of template expansion (#20784) [#20787](https://github.com/vitessio/vitess/pull/20787)
 * [release-23.0] CI: tighten workflow token permissions and checkout credentials (#20785) [#20795](https://github.com/vitessio/vitess/pull/20795)
 * [release-23.0] CI: scope app tokens to steps instead of exporting via GITHUB_ENV (#20786) [#20797](https://github.com/vitessio/vitess/pull/20797) 
#### Documentation
 * [release-23.0] VReplication: Remove internal undocumented VRLog feature (#20467) [#20589](https://github.com/vitessio/vitess/pull/20589)

