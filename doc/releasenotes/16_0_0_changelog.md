# Changelog of Vitess v16.0.0-rc1

### Announcement 
#### Governance
 * Move inactive maintainers to "Past" section, change Areas to include more active maintainers [#11744](https://github.com/vitessio/vitess/pull/11744)
 * add frances to maintainers [#11865](https://github.com/vitessio/vitess/pull/11865)
 * add Arthur to the maintainers file [#11937](https://github.com/vitessio/vitess/pull/11937) 
#### Query Serving
 * deprecate V3 planner [#11635](https://github.com/vitessio/vitess/pull/11635) 
#### VTAdmin
 * [vtadmin] deprecated vtexplain [#12163](https://github.com/vitessio/vitess/pull/12163)
### Bug fixes 
#### Backup and Restore
 * Detect redo log location dynamically based on presence [#11555](https://github.com/vitessio/vitess/pull/11555)
 * [main] mysqlctl: flags should be added to vtbackup (#12048) [#12060](https://github.com/vitessio/vitess/pull/12060) 
#### Build/CI
 * Fix the script `check_make_sizegen` [#11465](https://github.com/vitessio/vitess/pull/11465)
 * Skip `TestComparisonSemantics` test [#11474](https://github.com/vitessio/vitess/pull/11474)
 * Docker Image Context Fix [#11628](https://github.com/vitessio/vitess/pull/11628)
 * Addition of a CI tool to detect dead links in test/config.json [#11668](https://github.com/vitessio/vitess/pull/11668)
 * Fix files changes filtering in CI [#11714](https://github.com/vitessio/vitess/pull/11714)
 * Fix `codeql` workflow timeout issue [#11760](https://github.com/vitessio/vitess/pull/11760)
 * Trigger OnlineDDL workflow when test data changes [#11827](https://github.com/vitessio/vitess/pull/11827) 
#### Cluster management
 * BugFix: Errant GTID detection for a single replica [#12024](https://github.com/vitessio/vitess/pull/12024)
 * BugFix: Fix race in `IsFlagProvided` [#12042](https://github.com/vitessio/vitess/pull/12042) 
#### General
 * [main] Stats Flags: include stats flags in the correct binaries (#11450) [#11453](https://github.com/vitessio/vitess/pull/11453)
 * Test flags: Update logic for parsing test flags to run unit tests within GoLand and to parse test flags in vtgate to allow running unit tests [#11551](https://github.com/vitessio/vitess/pull/11551)
 * Fix missing flag usage for vault credentials flags [#11582](https://github.com/vitessio/vitess/pull/11582)
 * fix vdiff release notes [#11595](https://github.com/vitessio/vitess/pull/11595) 
#### Observability
 * stats/prometheus: normalize labels for single-label implementations [#12057](https://github.com/vitessio/vitess/pull/12057) 
#### Online DDL
 * Parse binlog variable length encoded columns [#11871](https://github.com/vitessio/vitess/pull/11871) 
#### Operator
 * [main] Increase the memory limit of the vitess-operator (#11548) [#11550](https://github.com/vitessio/vitess/pull/11550)
 * Fix rbac config in the vtop example [#12034](https://github.com/vitessio/vitess/pull/12034) 
#### Query Serving
 * Fix query list override issue on mysql restart [#11309](https://github.com/vitessio/vitess/pull/11309)
 * make MySQL56-flavor schema queries forward-compatible [#11323](https://github.com/vitessio/vitess/pull/11323)
 * Plan order by `COUNT(X)` [#11420](https://github.com/vitessio/vitess/pull/11420)
 * Fix #11455 - skip vindex operations for `DELETE` statements against unsharded tables [#11461](https://github.com/vitessio/vitess/pull/11461)
 * Fix aggregation end-to-end test PRIMARY KEY [#11473](https://github.com/vitessio/vitess/pull/11473)
 * collations: fix coercion semantics according to 8.0.31 changes [#11487](https://github.com/vitessio/vitess/pull/11487)
 * fix: reserve connection to follow query timeout when outside of transaction [#11490](https://github.com/vitessio/vitess/pull/11490)
 * [main] bug fix: using self-referencing columns in HAVING should not overflow (#11499) [#11504](https://github.com/vitessio/vitess/pull/11504)
 * Fix `HAVING` rewriting made in #11306 [#11515](https://github.com/vitessio/vitess/pull/11515)
 * fix: fail over reconnect in stream execution for connection with transaction [#11517](https://github.com/vitessio/vitess/pull/11517)
 * [15.0] Fix: concatenate engine in transaction [#11534](https://github.com/vitessio/vitess/pull/11534)
 * [main] Redact bind variables in mysql errors (#11540) [#11545](https://github.com/vitessio/vitess/pull/11545)
 * Fix ordering when error happens during stream setup [#11592](https://github.com/vitessio/vitess/pull/11592)
 * Do not multiply `AggregateRandom` in `JOIN`s [#11633](https://github.com/vitessio/vitess/pull/11633)
 * [bugfix] Allow VTExplain to handle shards that are not active during resharding [#11640](https://github.com/vitessio/vitess/pull/11640)
 * Fix generating invalid alter table for comments [#11645](https://github.com/vitessio/vitess/pull/11645)
 * sqltypes: handle leading zeroes [#11650](https://github.com/vitessio/vitess/pull/11650)
 * Send errors in stream instead of a grpc error from streaming rpcs when transaction or reserved connection is acquired [#11656](https://github.com/vitessio/vitess/pull/11656)
 * schemadiff: normalize index option value (string) [#11675](https://github.com/vitessio/vitess/pull/11675)
 * improve handling of ORDER BY/HAVING rewriting [#11691](https://github.com/vitessio/vitess/pull/11691)
 * query timeout hints in unsharded cases [#11709](https://github.com/vitessio/vitess/pull/11709)
 * Online DDL: adding error check; more verbose error messages [#11789](https://github.com/vitessio/vitess/pull/11789)
 * Accept no more data in session state change as ok [#11796](https://github.com/vitessio/vitess/pull/11796)
 * Fix: return allowed transaction isolation level value on select query [#11804](https://github.com/vitessio/vitess/pull/11804)
 * semantics: Use a BitSet [#11819](https://github.com/vitessio/vitess/pull/11819)
 * BugFix: Escaping Percentage and Underscore require special handling [#11823](https://github.com/vitessio/vitess/pull/11823)
 * Simplify recursive data structure in CFC Vindex [#11843](https://github.com/vitessio/vitess/pull/11843)
 * Fix MySQL56 GTID parsing when SID/UUID repeats [#11888](https://github.com/vitessio/vitess/pull/11888)
 * Online DDL: fix 'vtctlclient OnlineDDL' template queries [#11889](https://github.com/vitessio/vitess/pull/11889)
 * Fix CheckMySQL by setting the correct wanted state [#11895](https://github.com/vitessio/vitess/pull/11895)
 * Onlineddl: formalize "immediate operations", respect `--postpone-completion` strategy flag [#11910](https://github.com/vitessio/vitess/pull/11910)
 * bugfix: allow predicates without dependencies with derived tables to be handled correctly [#11911](https://github.com/vitessio/vitess/pull/11911)
 * Online DDL: ensure message is valid `utf8` in `updateMigrationMessage()` [#11914](https://github.com/vitessio/vitess/pull/11914)
 * Fix sending a ServerLost error when reading a packet fails [#11920](https://github.com/vitessio/vitess/pull/11920)
 * only expand when we have full information [#11998](https://github.com/vitessio/vitess/pull/11998)
 * Remove unnecessary logging [#12000](https://github.com/vitessio/vitess/pull/12000)
 * Fix parsing and normalization of floating point types [#12009](https://github.com/vitessio/vitess/pull/12009)
 * OnlineDDL: scheduleNextMigration() to only read reviewed migrations [#12014](https://github.com/vitessio/vitess/pull/12014)
 * Keywords must be unique and can't be reused [#12044](https://github.com/vitessio/vitess/pull/12044)
 * Fix: Date math with Interval keyword [#12082](https://github.com/vitessio/vitess/pull/12082)
 * OnlineDDL: support integer-to-enum conversion in `vitess` migrations [#12098](https://github.com/vitessio/vitess/pull/12098)
 * Keep the correct case for the charset for canonical string [#12105](https://github.com/vitessio/vitess/pull/12105)
 * BugFix: Cast expression translation by evaluation engine [#12111](https://github.com/vitessio/vitess/pull/12111)
 * [Gen4] Fix lookup vindexes with `autocommit` enabled [#12172](https://github.com/vitessio/vitess/pull/12172)
 * handle system databases other that information_schema correctly [#12175](https://github.com/vitessio/vitess/pull/12175)
 * VTGate: Ensure HealthCheck Cache Secondary Maps Stay in Sync With Authoritative Map on Tablet Delete [#12178](https://github.com/vitessio/vitess/pull/12178)
 * schemadiff: fix scenario where no tables exist in schema and with just views reading from DUAL [#12189](https://github.com/vitessio/vitess/pull/12189)
 * Fix parsing of bitnum values larger than 64 bit [#12191](https://github.com/vitessio/vitess/pull/12191)
 * Online DDL: fix deadlock by releasing mutex before calling callback [#12211](https://github.com/vitessio/vitess/pull/12211) 
#### VReplication
 * VReplication:  escape identifiers when renaming source tables [#11670](https://github.com/vitessio/vitess/pull/11670)
 * VReplication: Prevent Orphaned VDiff2 Jobs [#11768](https://github.com/vitessio/vitess/pull/11768)
 * VDiff2: Properly Apply --only_pks Report Flag [#12025](https://github.com/vitessio/vitess/pull/12025)
 * VReplication: Improve Error/Status Reporting [#12052](https://github.com/vitessio/vitess/pull/12052)
 * VReplication: Propagate Binlog Stream Errors [#12095](https://github.com/vitessio/vitess/pull/12095) 
#### VTorc
 * Fix VTOrc holding locks after shutdown [#11442](https://github.com/vitessio/vitess/pull/11442)
 * [15.0] Fix VTOrc to handle multiple failures [#11489](https://github.com/vitessio/vitess/pull/11489)
 * VTOrc running PRS when database_instance empty bug fix. [#12019](https://github.com/vitessio/vitess/pull/12019)
 * Fix: VTOrc forgetting old instances [#12089](https://github.com/vitessio/vitess/pull/12089)
 * Fix insert query of blocked_recovery table in VTOrc [#12091](https://github.com/vitessio/vitess/pull/12091) 
#### vtctl
 * Switch ApplySchema `--sql` argument to be `StringArray` instead of `StringSlice` [#11790](https://github.com/vitessio/vitess/pull/11790) 
#### vtexplain
 * Use Gen4 as the default planner version for VTExplain [#12021](https://github.com/vitessio/vitess/pull/12021) 
#### vttestserver
 * Fix vttestserver run script defaults [#12004](https://github.com/vitessio/vitess/pull/12004)
 * Add missing backslash to run.sh script [#12033](https://github.com/vitessio/vitess/pull/12033)
### CI/Build 
#### Backup and Restore
 * docker/lite: +zstd dep [#11997](https://github.com/vitessio/vitess/pull/11997) 
#### Build/CI
 * unit test: use require and assert [#11252](https://github.com/vitessio/vitess/pull/11252)
 * Skip CI with the `Skip CI` label [#11514](https://github.com/vitessio/vitess/pull/11514)
 * Update GitHub Actions workflows to latest versions [#11525](https://github.com/vitessio/vitess/pull/11525)
 * Removing SharedPitr_tls and Backup_transfrom test from CI [#11611](https://github.com/vitessio/vitess/pull/11611)
 * Add automation to change vitess version in the docker-release script [#11682](https://github.com/vitessio/vitess/pull/11682)
 * Fix two additional flaky test sources in endtoend tests [#11743](https://github.com/vitessio/vitess/pull/11743)
 * Update latest protobuf [#11782](https://github.com/vitessio/vitess/pull/11782)
 * Update test runners to run all tests including outside package [#11787](https://github.com/vitessio/vitess/pull/11787)
 * Update to latest etcd release [#11791](https://github.com/vitessio/vitess/pull/11791)
 * Migrate to GitHub OIDC based auth for Launchable [#11808](https://github.com/vitessio/vitess/pull/11808)
 * Fix the golangci-lint config [#11812](https://github.com/vitessio/vitess/pull/11812)
 * Add instructions on how to fix a self-hosted runner running out of disk space [#11839](https://github.com/vitessio/vitess/pull/11839)
 * Fix deprecated usage of set-output [#11844](https://github.com/vitessio/vitess/pull/11844)
 * update golangci-lint to 1.50.1 [#11873](https://github.com/vitessio/vitess/pull/11873)
 * CODEOWNERS: Add vrepl team members for vtgate vstream and tablet picker [#11950](https://github.com/vitessio/vitess/pull/11950)
 * Upgrade all the CI runners to Ubuntu 22.04 [#11985](https://github.com/vitessio/vitess/pull/11985)
 * Add lauchable to unit tests as well and remove OIDC [#12031](https://github.com/vitessio/vitess/pull/12031)
 * consolidating OnlineDDL 'declarative' tests into 'scheduler' tests: part 1 [#12061](https://github.com/vitessio/vitess/pull/12061)
 * OnlineDDL CI: remove 'revertible' tests (part 2) [#12192](https://github.com/vitessio/vitess/pull/12192)
 * Update vtadmin dependencies [#12201](https://github.com/vitessio/vitess/pull/12201)
 * Update Go dependencies [#12215](https://github.com/vitessio/vitess/pull/12215) 
#### Cluster management
 * Endtoend cluster improvements [#11859](https://github.com/vitessio/vitess/pull/11859)
 * CI, tabletmanager throttler topo tests: polling until status received [#12107](https://github.com/vitessio/vitess/pull/12107) 
#### General
 * [deps] go get golang.org/x/text && go mod tidy [#11466](https://github.com/vitessio/vitess/pull/11466)
 * Upgrade to `go1.19.3` [#11655](https://github.com/vitessio/vitess/pull/11655)
 * Code freeze of release-16.0 [#12232](https://github.com/vitessio/vitess/pull/12232) 
#### Governance
 * codeowners: have at least two for almost every package [#11639](https://github.com/vitessio/vitess/pull/11639)
 * added code owners for go.mod and go.sum files [#11711](https://github.com/vitessio/vitess/pull/11711)
 * Add more codeowners to the `/test` directory [#11762](https://github.com/vitessio/vitess/pull/11762) 
#### Query Serving
 * Consistent sorting in Online DDL Vrepl suite test [#11821](https://github.com/vitessio/vitess/pull/11821)
 * Flakes: Properly Test HealthCheck Cache Response Handling [#12226](https://github.com/vitessio/vitess/pull/12226) 
#### TabletManager
 * Update throttler-topo workflow file [#11784](https://github.com/vitessio/vitess/pull/11784)
 * Fix closing the body for HTTP requests [#11842](https://github.com/vitessio/vitess/pull/11842) 
#### VReplication
 * update jsonparser dependency [#11694](https://github.com/vitessio/vitess/pull/11694) 
#### VTorc
 * Move vtorc runners back to normal github runners [#11482](https://github.com/vitessio/vitess/pull/11482)
### Dependabot 
#### Java
 * build(deps): Bump protobuf-java from 3.19.4 to 3.19.6 in /java [#11439](https://github.com/vitessio/vitess/pull/11439) 
#### VTAdmin
 * build(deps): Bump @xmldom/xmldom from 0.7.5 to 0.7.8 in /web/vtadmin [#11615](https://github.com/vitessio/vitess/pull/11615)
 * build(deps): Bump loader-utils from 1.4.0 to 1.4.1 in /web/vtadmin [#11659](https://github.com/vitessio/vitess/pull/11659)
 * build(deps): Bump loader-utils from 1.4.1 to 1.4.2 in /web/vtadmin [#11725](https://github.com/vitessio/vitess/pull/11725)
### Documentation 
#### Build/CI
 * Update release notes summary for the new default MySQL version [#12222](https://github.com/vitessio/vitess/pull/12222) 
#### CLI
 * [vtadmin] Do not backtick binary name [#11464](https://github.com/vitessio/vitess/pull/11464)
 * [vtctldclient|docs] apply doc feedback based on website PR feedback [#12030](https://github.com/vitessio/vitess/pull/12030) 
#### Documentation
 * Upgrades the release notes for v15.0.0 [#11567](https://github.com/vitessio/vitess/pull/11567)
 * Copy design docs over from website [#12071](https://github.com/vitessio/vitess/pull/12071) 
#### VReplication
 * Mark VDiff V2 as GA in v16 [#12084](https://github.com/vitessio/vitess/pull/12084)
### Enhancement 
#### Authn/z
 * VTGate: Set immediate caller id from gRPC static auth username [#12050](https://github.com/vitessio/vitess/pull/12050) 
#### Backup and Restore
 * Incremental logical backup and point in time recovery [#11097](https://github.com/vitessio/vitess/pull/11097)
 * vtbackup: disable redo log before starting replication [#11330](https://github.com/vitessio/vitess/pull/11330)
 * remove excessive backup decompression logging [#11479](https://github.com/vitessio/vitess/pull/11479)
 * vtbackup: add --disable-redo-log flag (default false) [#11594](https://github.com/vitessio/vitess/pull/11594)
 * remove backup_hook from flag help [#12029](https://github.com/vitessio/vitess/pull/12029) 
#### Build/CI
 * Move CI workflow to use latest community version of mysql 8.0 [#11493](https://github.com/vitessio/vitess/pull/11493)
 * Upgrade the `release_notes_label` workflow for `v16.0.0` [#11544](https://github.com/vitessio/vitess/pull/11544)
 * CODEOWNERS: Add maintainers to `.github/workflows` and `.github` [#11781](https://github.com/vitessio/vitess/pull/11781)
 * Allow override of build git env in docker/base builds [#11968](https://github.com/vitessio/vitess/pull/11968)
 * Add vtorc port to vitess local docker run [#12001](https://github.com/vitessio/vitess/pull/12001)
 * Update the MySQL version used by our Docker images [#12054](https://github.com/vitessio/vitess/pull/12054)
 * Fail CI when a PR is labeled with `NeedsWebsiteDocsUpdate` or `NeedsDescriptionUpdate` [#12062](https://github.com/vitessio/vitess/pull/12062) 
#### CLI
 * Add GenerateShardRanges to vtctldclient [#11492](https://github.com/vitessio/vitess/pull/11492)
 * Properly deprecate flags and fix default for `--cell` [#11501](https://github.com/vitessio/vitess/pull/11501)
 * Allow version to be accessible via the -v shorthand [#11512](https://github.com/vitessio/vitess/pull/11512) 
#### Cluster management
 * Create new api for topo lock shard exists [#11269](https://github.com/vitessio/vitess/pull/11269)
 * Deprecating VExec part1: removing client-side references [#11955](https://github.com/vitessio/vitess/pull/11955) 
#### Driver
 * Implement the RowsColumnTypeScanType interface in the go sql driver [#12007](https://github.com/vitessio/vitess/pull/12007) 
#### Examples
 * Give all permissions in rbac in examples [#11463](https://github.com/vitessio/vitess/pull/11463)
 * Fix Vitess Operator example [#11546](https://github.com/vitessio/vitess/pull/11546) 
#### General
 * removing unncessary flags across binaries [#11495](https://github.com/vitessio/vitess/pull/11495)
 * [release-15.0] Upgrade to `go1.18.7` [#11507](https://github.com/vitessio/vitess/pull/11507)
 * vttablet sidecar schema:use schemadiff to reach desired  schema on tablet init replacing the withDDL-based approach [#11520](https://github.com/vitessio/vitess/pull/11520)
 * Removing redundant flags across binaries [#11522](https://github.com/vitessio/vitess/pull/11522)
 * Remove `EnableTracingOpt` and `--grpc_enable_tracing` [#11543](https://github.com/vitessio/vitess/pull/11543)
 * Add default lower stack limit [#11569](https://github.com/vitessio/vitess/pull/11569)
 * Upgrade to `go1.19.4` [#11905](https://github.com/vitessio/vitess/pull/11905)
 * Add structure logging to Vitess [#11960](https://github.com/vitessio/vitess/pull/11960)
 * Revert changes made in #11960 [#12219](https://github.com/vitessio/vitess/pull/12219) 
#### Governance
 * Add manan and florent to Docker files CODEOWNERS [#11981](https://github.com/vitessio/vitess/pull/11981) 
#### Query Serving
 * ComBinlogDumpGTID and downstream replication protocol [#10066](https://github.com/vitessio/vitess/pull/10066)
 * Document error code in `vtgate/planbuilder` [#10738](https://github.com/vitessio/vitess/pull/10738)
 * opt in/out of query consolidation [#11080](https://github.com/vitessio/vitess/pull/11080)
 * Online DDL: more (async) log visibility into cut-over phase [#11253](https://github.com/vitessio/vitess/pull/11253)
 * optionally disable verify-after-insert behavior of lookup vindexes [#11313](https://github.com/vitessio/vitess/pull/11313)
 * resource pool: resource max lifetime timeout [#11337](https://github.com/vitessio/vitess/pull/11337)
 * feat: added query timeout to vtgate default and per session [#11429](https://github.com/vitessio/vitess/pull/11429)
 * [evalengine] add rewrites for nullif and ifnull [#11431](https://github.com/vitessio/vitess/pull/11431)
 * Handle aliasing of collation names [#11433](https://github.com/vitessio/vitess/pull/11433)
 * vitess Online DDL atomic cut-over [#11460](https://github.com/vitessio/vitess/pull/11460)
 * Keep track of expanded columns in the semantic analysis [#11462](https://github.com/vitessio/vitess/pull/11462)
 * feat: deconstruct tuple comparisons so we can use them for routing decisions [#11500](https://github.com/vitessio/vitess/pull/11500)
 * Add Gauge For CheckMySQL Running [#11524](https://github.com/vitessio/vitess/pull/11524)
 * Optimize List Support In Vindex Functions [#11531](https://github.com/vitessio/vitess/pull/11531)
 * add option to disable lookup read lock [#11538](https://github.com/vitessio/vitess/pull/11538)
 * [refactor] Predicate push down [#11552](https://github.com/vitessio/vitess/pull/11552)
 * planner: better bindvar names for auto-parameterized queries [#11571](https://github.com/vitessio/vitess/pull/11571)
 * planner enhancement: nice bindvar names for update [#11581](https://github.com/vitessio/vitess/pull/11581)
 * Online DDL: more support for INSTANT DDL [#11591](https://github.com/vitessio/vitess/pull/11591)
 * vtgate: route create table statements to vschema keyspace [#11602](https://github.com/vitessio/vitess/pull/11602)
 * Dynamic tablet throttler config: enable/disable, set metrics query/threshold [#11604](https://github.com/vitessio/vitess/pull/11604)
 * Cleanup copying of proto results to sqltypes.Result [#11607](https://github.com/vitessio/vitess/pull/11607)
 * Move horizon planning to operators [#11622](https://github.com/vitessio/vitess/pull/11622)
 * normalize more expressions [#11631](https://github.com/vitessio/vitess/pull/11631)
 * Fix `OR 1=0` causing queries to scatter [#11653](https://github.com/vitessio/vitess/pull/11653)
 * Online DDL: normalize/idempotentize CHECK CONSTRAINTs in ALTER TABLE statement [#11663](https://github.com/vitessio/vitess/pull/11663)
 * add support for transaction isolation level and make it vitess aware setting [#11673](https://github.com/vitessio/vitess/pull/11673)
 * don't reuse bindvars for LIMIT and OFFSET [#11689](https://github.com/vitessio/vitess/pull/11689)
 * Online DDL: more scheduler triggering following successful operations [#11701](https://github.com/vitessio/vitess/pull/11701)
 * Add support for transaction access mode [#11704](https://github.com/vitessio/vitess/pull/11704)
 * rewrite predicates to expose routing opportunities [#11765](https://github.com/vitessio/vitess/pull/11765)
 * find IN route possibility in ORs [#11775](https://github.com/vitessio/vitess/pull/11775)
 * [planner] Better AST equality [#11867](https://github.com/vitessio/vitess/pull/11867)
 * optimize joins, redirect dml for reference tables [#11875](https://github.com/vitessio/vitess/pull/11875)
 * VExplain statement [#11892](https://github.com/vitessio/vitess/pull/11892)
 * Simplify `getPlan` and `gen4CompareV3` [#11903](https://github.com/vitessio/vitess/pull/11903)
 * Better clone of the VCursor [#11926](https://github.com/vitessio/vitess/pull/11926)
 * Better clone of the VCursor [#11926](https://github.com/vitessio/vitess/pull/11926)
 * [planner] Schema information on the information_schema views [#11941](https://github.com/vitessio/vitess/pull/11941)
 * schemadiff: foreign key validation (tables and columns) [#11944](https://github.com/vitessio/vitess/pull/11944)
 * OnlineDDL: support --unsafe-allow-foreign-keys strategy flag [#11976](https://github.com/vitessio/vitess/pull/11976)
 * support transaction isolation modification through reserved connection system settings [#11987](https://github.com/vitessio/vitess/pull/11987)
 * **unsafe**: Online DDL support for `--unsafe-allow-foreign-keys` strategy flag [#11988](https://github.com/vitessio/vitess/pull/11988)
 * vtgate advertised mysql server version to 8.0.31 [#11989](https://github.com/vitessio/vitess/pull/11989)
 * schemadiff: normalize `PRIMARY KEY` definition [#12016](https://github.com/vitessio/vitess/pull/12016)
 * schemadiff: validate and apply foreign key indexes [#12026](https://github.com/vitessio/vitess/pull/12026)
 * OnlineDDL: 'mysql' strategy, managed by the scheduler, but executed via normal MySQL statements [#12027](https://github.com/vitessio/vitess/pull/12027)
 * Refactor sqlparser.Rewrite uses [#12059](https://github.com/vitessio/vitess/pull/12059)
 * Online DDL: --in-order-completion ddl strategy and logic [#12113](https://github.com/vitessio/vitess/pull/12113)
 * schemadiff: TableCharsetCollateStrategy hint [#12137](https://github.com/vitessio/vitess/pull/12137)
 * Support BETWEEN in the evalengine [#12150](https://github.com/vitessio/vitess/pull/12150)
 * Use schema for the information_schema views [#12171](https://github.com/vitessio/vitess/pull/12171)
 * vtgateconn: add DeregisterDialer hook [#12213](https://github.com/vitessio/vitess/pull/12213) 
#### VReplication
 * VReplication Copy Phase: Parallelize Bulk Inserts [#10828](https://github.com/vitessio/vitess/pull/10828)
 * VSCopy: Resume the copy phase consistently from given GTID and lastpk [#11103](https://github.com/vitessio/vitess/pull/11103)
 * For partial MoveTables, setup reverse shard routing rules on workflow creation [#11415](https://github.com/vitessio/vitess/pull/11415)
 * Use unique rows in copy_state to support parallel replication [#11451](https://github.com/vitessio/vitess/pull/11451)
 * Log which tablet copy_state optimization failed on [#11521](https://github.com/vitessio/vitess/pull/11521)
 * Allow users to control VReplication DDL handling [#11532](https://github.com/vitessio/vitess/pull/11532)
 * VReplication: Defer Secondary Index Creation [#11700](https://github.com/vitessio/vitess/pull/11700)
 * VSCopy: Send COPY_COMPLETED events when the copy operation is done [#11740](https://github.com/vitessio/vitess/pull/11740)
 * Add `VStreamerCount` stat to `vttablet` [#11978](https://github.com/vitessio/vitess/pull/11978) 
#### VTAdmin
 * [VTAdmin] `Validate`, `ValidateShard`, `ValidateVersionShard`, `GetFullStatus` [#11438](https://github.com/vitessio/vitess/pull/11438)
 * Full Status tab improvements for VTAdmin [#11470](https://github.com/vitessio/vitess/pull/11470)
 * [15.0] Add VTGate debug/status page link to VTAdmin [#11541](https://github.com/vitessio/vitess/pull/11541)
 * VTAdmin: display workflow type in workflows list [#11685](https://github.com/vitessio/vitess/pull/11685) 
#### VTorc
 * Timeout Fixes and VTOrc Improvement [#11881](https://github.com/vitessio/vitess/pull/11881)
 * Also log error on a failure in DiscoverInstance [#11936](https://github.com/vitessio/vitess/pull/11936)
 * VTOrc Code Cleanup - generate_base, replace cluster_name with keyspace and shard. [#12012](https://github.com/vitessio/vitess/pull/12012)
 * Move vtorc from go-sqlite3 to modernc.org/sqlite [#12214](https://github.com/vitessio/vitess/pull/12214)
### Feature Request 
#### Evalengine
 * evalengine: Support built-in MySQL function for string functions and operations [#11185](https://github.com/vitessio/vitess/pull/11185) 
#### Query Serving
 * Add support for views in vtgate [#11195](https://github.com/vitessio/vitess/pull/11195)
 * Add support for Views DDL [#11896](https://github.com/vitessio/vitess/pull/11896)
 * notify view change to vtgate [#12115](https://github.com/vitessio/vitess/pull/12115)
 * Views Support: Updating Views in VSchema for query serving [#12124](https://github.com/vitessio/vitess/pull/12124) 
#### Admin Web UI
 * [VTAdmin] Topology Browser [#11496](https://github.com/vitessio/vitess/pull/11496)
### Internal Cleanup 
#### Backup and Restore
 * backup: remove deprecated hook support [#12066](https://github.com/vitessio/vitess/pull/12066) 
#### Build/CI
 * Update all the Go dependencies [#11741](https://github.com/vitessio/vitess/pull/11741)
 * Remove building Docker containers with MariaDB [#12040](https://github.com/vitessio/vitess/pull/12040)
 * Add TOC to the summary docs [#12225](https://github.com/vitessio/vitess/pull/12225) 
#### CLI
 * moved missed flags to pflags in vtgate [#11966](https://github.com/vitessio/vitess/pull/11966)
 * Migrate missed vtctld flags to pflag and immediately deprecate them [#11974](https://github.com/vitessio/vitess/pull/11974)
 * Remove Dead Legacy Workflow Manager Code [#12085](https://github.com/vitessio/vitess/pull/12085) 
#### Cluster management
 * Adding deprecate message to backup hooks [#11491](https://github.com/vitessio/vitess/pull/11491)
 * Orchestrator Integration Removal and `orc_client_user` removal [#11503](https://github.com/vitessio/vitess/pull/11503)
 * [15.0] Deprecate InitShardPrimary command [#11557](https://github.com/vitessio/vitess/pull/11557)
 * mysqlctl is a command-line client so remove server flags [#12022](https://github.com/vitessio/vitess/pull/12022)
 * Remove replication manager and run VTOrc in all e2e tests [#12149](https://github.com/vitessio/vitess/pull/12149) 
#### General
 * Improve Codeowners File [#11428](https://github.com/vitessio/vitess/pull/11428)
 * Remove example script that caused some confusion [#11529](https://github.com/vitessio/vitess/pull/11529)
 * Remove unused ioutil2 code [#11661](https://github.com/vitessio/vitess/pull/11661)
 * Fix some linter errors [#11773](https://github.com/vitessio/vitess/pull/11773)
 * Remove Deprecated flags, code and stats. [#12083](https://github.com/vitessio/vitess/pull/12083) 
#### Governance
 * Correct minor inaccuracies in governing docs [#11933](https://github.com/vitessio/vitess/pull/11933) 
#### Online DDL
 * [cleanup] Explicitly include DDLStrategySetting in the sizegen target [#11857](https://github.com/vitessio/vitess/pull/11857)
 * OnlineDDL: avoid schema_migrations AUTO_INCREMENT gaps by pre-checking for existing migration [#12169](https://github.com/vitessio/vitess/pull/12169) 
#### Query Serving
 * [gen4 planner] Operator refactoring [#11498](https://github.com/vitessio/vitess/pull/11498)
 * [gen4]: small refactoring around Compact [#11537](https://github.com/vitessio/vitess/pull/11537)
 * change CreatePhysicalOperator to use the rewriteBottomUp() functionality [#11542](https://github.com/vitessio/vitess/pull/11542)
 * [refactor planner] Columns and predicates on operators [#11606](https://github.com/vitessio/vitess/pull/11606)
 * Move initialization of metrics to be static [#11608](https://github.com/vitessio/vitess/pull/11608)
 * planner operators refactoring [#11680](https://github.com/vitessio/vitess/pull/11680)
 * sqlparser: new Equality API [#11906](https://github.com/vitessio/vitess/pull/11906)
 * sqlparser: `QueryMatchesTemplates` uses canonical string [#11990](https://github.com/vitessio/vitess/pull/11990)
 * Move more rewriting to SafeRewrite [#12063](https://github.com/vitessio/vitess/pull/12063)
 * store transaction isolation level in upper case [#12099](https://github.com/vitessio/vitess/pull/12099)
 * Generating copy-on-rewrite logic [#12135](https://github.com/vitessio/vitess/pull/12135)
 * Clean up ColumnType uses [#12139](https://github.com/vitessio/vitess/pull/12139) 
#### TabletManager
 * Table GC: rely on tm state to determine operation mode [#11972](https://github.com/vitessio/vitess/pull/11972)
 * Mark VReplicationExec Client Command as Deprecated [#12070](https://github.com/vitessio/vitess/pull/12070) 
#### VReplication
 * Leverage pFlag's Changed function to detect user specified flag [#11677](https://github.com/vitessio/vitess/pull/11677)
 * VReplication: Remove Deprecated V1 Client Commands [#11705](https://github.com/vitessio/vitess/pull/11705) 
#### VTAdmin
 * move react-scripts to dev dependencies [#11767](https://github.com/vitessio/vitess/pull/11767) 
#### Admin Web UI
 * [vtctld2] Remove vtctld2 UI and vtctld server components that serve the app UI [#11851](https://github.com/vitessio/vitess/pull/11851)
### Performance 
#### Cluster management
 * Bug fix: Cache filtered out tablets in topology watcher to avoid unnecessary GetTablet calls to topo [#12194](https://github.com/vitessio/vitess/pull/12194) 
#### Online DDL
 * Speedup DDLs by not reloading table size stats [#11601](https://github.com/vitessio/vitess/pull/11601) 
#### Query Serving
 * DDL: do not Reload() for 'CREATE TEMPORARY' and 'DROP TEMPORARY' statements [#12144](https://github.com/vitessio/vitess/pull/12144) 
#### VReplication
 * mysql: Improve MySQL 5.6 GTID parsing performance [#11570](https://github.com/vitessio/vitess/pull/11570) 
#### vttestserver
 * vttestserver: make tablet_refresh_interval configurable and reduce default value [#11918](https://github.com/vitessio/vitess/pull/11918)
### Release 
#### Build/CI
 * Improve the release process [#12056](https://github.com/vitessio/vitess/pull/12056)
 * Use Ubuntu 20.04 for Release Builds [#12202](https://github.com/vitessio/vitess/pull/12202)
 * Use Ubuntu 20.04 for Release Builds [#12202](https://github.com/vitessio/vitess/pull/12202) 
#### Documentation
 * Fix release notes summary links [#11508](https://github.com/vitessio/vitess/pull/11508)
 * Release notes summary of `14.0.4` [#11849](https://github.com/vitessio/vitess/pull/11849)
 * Release notes for `v15.0.2` [#11963](https://github.com/vitessio/vitess/pull/11963) 
#### General
 * Release notes for 15.0.0-rc1 and update SNAPSHOT version to 16.0.0 [#11445](https://github.com/vitessio/vitess/pull/11445)
 * fix anchors for release notes and summary [#11578](https://github.com/vitessio/vitess/pull/11578)
 * update release notes after 15.0 [#11584](https://github.com/vitessio/vitess/pull/11584)
 * Mention the `--db-config-*-*` flag in the release notes [#11610](https://github.com/vitessio/vitess/pull/11610)
 * Release notes for 15.0.1 [#11850](https://github.com/vitessio/vitess/pull/11850)
 * updating summary and release notes for v15.0.1 [#11852](https://github.com/vitessio/vitess/pull/11852)
 * [main] Update the release `15.0.2` summary doc (#11954) [#11956](https://github.com/vitessio/vitess/pull/11956)
### Testing 
#### Backup and Restore
 * go/vt/mysqlctl: add compression benchmarks [#11994](https://github.com/vitessio/vitess/pull/11994) 
#### Build/CI
 * endtoend: fix race when closing vtgate [#11707](https://github.com/vitessio/vitess/pull/11707)
 * [ci issue] Tests are running on older versions that do not support the query [#11923](https://github.com/vitessio/vitess/pull/11923)
 * consolidating OnlineDDL 'singleton' tests into 'scheduler' tests: part 1 [#12055](https://github.com/vitessio/vitess/pull/12055)
 * Internal: Fix Bad Merge [#12087](https://github.com/vitessio/vitess/pull/12087)
 * add debug tooling [#12126](https://github.com/vitessio/vitess/pull/12126)
 * Remove the semgrep action [#12148](https://github.com/vitessio/vitess/pull/12148)
 * CI cleanup: remove onlineddl_declarative, onlineddl_singleton (cleanup part 2) [#12182](https://github.com/vitessio/vitess/pull/12182)
 * Online DDL CI: consolidated revertible and revert CI tests (part 1) [#12183](https://github.com/vitessio/vitess/pull/12183)
 * Allow manually kicking off CodeQL [#12200](https://github.com/vitessio/vitess/pull/12200) 
#### General
 * endtoend: fix dbconfig initialization for endtoend tests [#11609](https://github.com/vitessio/vitess/pull/11609) 
#### Query Serving
 * Add additional unit test with state changes swapped [#11192](https://github.com/vitessio/vitess/pull/11192)
 * Use JSON for plan tests [#11430](https://github.com/vitessio/vitess/pull/11430)
 * Add a PRIMARY KEY to the aggregation E2E tests [#11459](https://github.com/vitessio/vitess/pull/11459)
 * Change the indexes in `TestEmptyTableAggr` to be unique [#11485](https://github.com/vitessio/vitess/pull/11485)
 * Readable plan tests [#11708](https://github.com/vitessio/vitess/pull/11708)
 * test: deflake TestQueryTimeoutWithTables [#11772](https://github.com/vitessio/vitess/pull/11772)
 * more unit tests for QueryMatchesTemplates() [#11894](https://github.com/vitessio/vitess/pull/11894)
 * remove e2e test from partial_keyspace config [#12005](https://github.com/vitessio/vitess/pull/12005) 
#### VReplication
 * VDiff2: Migrate VDiff1 Unit Tests [#11916](https://github.com/vitessio/vitess/pull/11916)
 * VReplication: Test Migrations From MariaDB to MySQL [#12036](https://github.com/vitessio/vitess/pull/12036)

