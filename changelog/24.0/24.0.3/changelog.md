# Changelog of Vitess v24.0.3

### Bug fixes 
#### Build/CI
 * [release-24.0] test: fix topo-flavor e2e shards silently running zero tests (#20556) [#20637](https://github.com/vitessio/vitess/pull/20637) 
#### Documentation
 * [release-24.0] `reparentutil`: order reparent candidates by GTID dominance for a consistent sort (#20728) [#20758](https://github.com/vitessio/vitess/pull/20758) 
#### Query Serving
 * [release-24.0] mysql: end the streaming result set when an error packet arrives (#20495) [#20505](https://github.com/vitessio/vitess/pull/20505)
 * [release-24.0] mysql/json: fix MarshalTo discarding accumulated output for nested blob and bit values (#20625) [#20656](https://github.com/vitessio/vitess/pull/20656)
 * [release-24.0] charset: ensure DecodeRune always advances on malformed input (#20753) [#20766](https://github.com/vitessio/vitess/pull/20766) 
#### VDiff
 * [release-24.0] VDiff: stop pre-seeding a bogus category in TableDiffPhaseTimings (#20713) [#20717](https://github.com/vitessio/vitess/pull/20717)
 * [release-24.0] Properly handle vstream filter predicates with multi-col PKs (#20858) [#20865](https://github.com/vitessio/vitess/pull/20865) 
#### VReplication
 * [release-24.0] vstreamer: don't drop a SET column's 64th member (#20450) [#20456](https://github.com/vitessio/vitess/pull/20456)
 * [release-24.0] VReplication: reconcile in-memory workflow state metric from the persisted row (#20442) [#20501](https://github.com/vitessio/vitess/pull/20501)
 * [release-24.0] binlog: escape JSON diff paths when generating diff SQL (#20461) [#20529](https://github.com/vitessio/vitess/pull/20529)
 * [release-24.0] workflow: escape tenant id and column name in getTenantClause (#20413) [#20533](https://github.com/vitessio/vitess/pull/20533)
 * [release-24.0] VReplication: Avoid sending mixed batch of row changes to bulk insert or bulk delete in vplayer batch mode (#20565) [#20588](https://github.com/vitessio/vitess/pull/20588)
 * [release-24.0] VReplication: only build a bulk-delete plan for insertNormal table plans (#20889) [#20921](https://github.com/vitessio/vitess/pull/20921)
 * [release-24.0] VReplication: LookupVindex streams must not follow TABLES journals (#20917) [#20937](https://github.com/vitessio/vitess/pull/20937)
 * [release-24.0] VReplication: don't count throttled time in the vplayer stall deadline (#20925) [#20950](https://github.com/vitessio/vitess/pull/20950) 
#### VTAdmin
 * Backport #20410: vtadmin: fix tab navigation URL corruption in splat routes [#20581](https://github.com/vitessio/vitess/pull/20581) 
#### VTGate
 * [release-24.0] Preserve query hints in field/impossible queries (#20366) [#20399](https://github.com/vitessio/vitess/pull/20399)
 * [release-24.0] Enforce recursion-depth limit on the streaming recursive CTE path (#20432) [#20469](https://github.com/vitessio/vitess/pull/20469)
 * [release-24.0] vtgate: copy bindVars per source in streaming Concatenate (#20436) [#20491](https://github.com/vitessio/vitess/pull/20491)
 * [release-24.0] evalengine: don't append a NUL byte to TO_BASE64 output at exact 57 byte multiples (#20474) [#20503](https://github.com/vitessio/vitess/pull/20503)
 * [release-24.0] mysql: encode zero DATE/DATETIME/TIMESTAMP values as zero-length in the binary protocol (#20460) [#20507](https://github.com/vitessio/vitess/pull/20507)
 * [release-24.0] vtgate: copy execute options per scatter call for FetchLastInsertId (#20439) [#20524](https://github.com/vitessio/vitess/pull/20524)
 * [release-24.0] vtgate: do not autocommit per-chunk in streaming insert-select (#20497) [#20526](https://github.com/vitessio/vitess/pull/20526)
 * [release-24.0] evalengine: round DOUBLE ties half to even in ROUND() like MySQL (#20476) [#20531](https://github.com/vitessio/vitess/pull/20531)
 * [release-24.0] vtgate: do not start an implicit transaction on prepare (#20538) [#20551](https://github.com/vitessio/vitess/pull/20551)
 * [release-24.0] grpcvtgateconn: stop mutating shared dial options slice (#20580) [#20584](https://github.com/vitessio/vitess/pull/20584)
 * [release-24.0] Fix ExecuteMulti timeout context reuse (#20445) [#20607](https://github.com/vitessio/vitess/pull/20607)
 * [release-24.0] vtgate: support prepared statements with a leading WITH clause (#20665) [#20706](https://github.com/vitessio/vitess/pull/20706)
 * [release-24.0] evalengine: stop RPAD growing its operand in place (#20697) [#20708](https://github.com/vitessio/vitess/pull/20708)
 * [release-24.0] evalengine: stop the binary bitwise operators writing over and marking their result (#20696) [#20710](https://github.com/vitessio/vitess/pull/20710)
 * [release-24.0] evalengine: collapse the VM stack when a later LOCATE or SUBSTRING operand is NULL (#20645) [#20715](https://github.com/vitessio/vitess/pull/20715) 
#### VTTablet
 * [release-24.0] vttablet: roll back canceled schema reload on a non-cancelable context (#20385) [#20401](https://github.com/vitessio/vitess/pull/20401)
 * [release-24.0] vreplication: prevent vttablet panic on malformed `RowChange` images (#20377) [#20409](https://github.com/vitessio/vitess/pull/20409)
 * [release-24.0] grpctmclient: evict a failed dedicated-pool dial so the next call redials (#20414) [#20422](https://github.com/vitessio/vitess/pull/20422)
 * [release-24.0] Enforce stored-procedure safety checks on the streaming CALL path (#20372) [#20424](https://github.com/vitessio/vitess/pull/20424)
 * [release-24.0] txthrottler: don't panic on target_replication_lag_sec of 1 (#20554) [#20559](https://github.com/vitessio/vitess/pull/20559)
 * [release-24.0] vttablet: perform an initial heartbeat read when opening the heartbeat reader (#20868) [#20906](https://github.com/vitessio/vitess/pull/20906) 
#### vtctl
 * [release-24.0] `reparentutil`: keep nil-alias tablets out of candidate ordering (#20762) [#20768](https://github.com/vitessio/vitess/pull/20768)
### CI/Build 
#### Build/CI
 * [release-24.0] ci: fix mysql57 setup by bumping removed libtinfo5 pin, fail fast on download errors (#20481) [#20484](https://github.com/vitessio/vitess/pull/20484)
 * [release-24.0] ci: fix self-references in query_serving_queries_2 change-detection filters (#20482) [#20489](https://github.com/vitessio/vitess/pull/20489)
 * [release-24.0] ci: try each resolved IP when downloading from archive.ubuntu.com in setup-mysql (#20539) [#20544](https://github.com/vitessio/vitess/pull/20544)
 * [release-24.0] test: gate TestReplicationStopped on vtctld version for upgrade/downgrade [#20729](https://github.com/vitessio/vitess/pull/20729)
 * [release-24.0] ci: bump the removed libtinfo5 pin to 6.3-2ubuntu0.3 (#20986) [#20990](https://github.com/vitessio/vitess/pull/20990)
### Compatibility Bug 
#### VTGate
 * [release-24.0] evalengine: use MySQL 1-based ordinal for ENUM in numeric context (#20454) [#20459](https://github.com/vitessio/vitess/pull/20459)
### Dependencies 
#### Build/CI
 * [release-24.0] Remove Antithesis related code (#20396) [#20403](https://github.com/vitessio/vitess/pull/20403) 
#### Docker
 * [release-24.0] Upgrade the Golang version to `go1.26.5` [#20517](https://github.com/vitessio/vitess/pull/20517)
 * [release-24.0] Upgrade the Golang version to `go1.26.6` [#20834](https://github.com/vitessio/vitess/pull/20834)
 * [release-24.0] Upgrade the Golang version to `go1.26.7` [#20874](https://github.com/vitessio/vitess/pull/20874)
### Enhancement 
#### Backup and Restore
 * [release-24.0] mysqlctl: force TZ=UTC for mysqlbinlog during point-in-time restore (#20463) [#20509](https://github.com/vitessio/vitess/pull/20509) 
#### Documentation
 * [release-24.0] go/mysql: streaming errors no longer surface as connection loss (#20383) [#20541](https://github.com/vitessio/vitess/pull/20541) 
#### VDiff
 * [release-24.0] VDiff: save a sample for every drained extra row so reconciliation can match them (#20855) [#20943](https://github.com/vitessio/vitess/pull/20943) 
#### VTGate
 * [release-24.0] `go/mysql`: send ERR instead of teardown after an OK carrying `SERVER_MORE_RESULTS_EXISTS` (#20563) [#20667](https://github.com/vitessio/vitess/pull/20667)
### Internal Cleanup 
#### General
 * [release-24.0] Update maintainers and code owners project lists (#20772) [#20822](https://github.com/vitessio/vitess/pull/20822)
### Release 
#### General
 * [release-24.0] Bump to `v24.0.3-SNAPSHOT` after the `v24.0.2` release [#20393](https://github.com/vitessio/vitess/pull/20393)
 * [release-24.0] Code Freeze for `v24.0.3` [#20996](https://github.com/vitessio/vitess/pull/20996)
### Security 
#### Build/CI
 * [release-24.0] CI: pass GitHub context to run scripts via env vars instead of template expansion (#20784) [#20788](https://github.com/vitessio/vitess/pull/20788)
 * [release-24.0] CI: tighten workflow token permissions and checkout credentials (#20785) [#20796](https://github.com/vitessio/vitess/pull/20796)
 * [release-24.0] CI: scope app tokens to steps instead of exporting via GITHUB_ENV (#20786) [#20798](https://github.com/vitessio/vitess/pull/20798)
 * [release-24.0] CI: generate error code docs from trusted commits only (#20789) [#20808](https://github.com/vitessio/vitess/pull/20808) 
#### Documentation
 * [release-24.0] VReplication: Remove internal undocumented VRLog feature (#20467) [#20590](https://github.com/vitessio/vitess/pull/20590)
### Testing 
#### Cluster management
 * [release-24.0] Fix TestInitShardPrimary flakiness from racing startup replication queries (#20434) [#20441](https://github.com/vitessio/vitess/pull/20441)

