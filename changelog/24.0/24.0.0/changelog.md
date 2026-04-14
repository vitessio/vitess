# Changelog of Vitess v24.0.0

### Announcement 
#### Governance
 * Add Nick as Vitess Maintainer [#19094](https://github.com/vitessio/vitess/pull/19094)
### Bug fixes 
#### Backup and Restore
 * BuiltinBackupEngine: Retry file close and fail backup when we cannot [#18848](https://github.com/vitessio/vitess/pull/18848)
 * fix(backup): propagate file hashes to manifest after retry [#19336](https://github.com/vitessio/vitess/pull/19336)
 * fix: Add super_read_only handling in init_clone [#19741](https://github.com/vitessio/vitess/pull/19741) 
#### Build/CI
 * Fix minor upgrade logic in go upgrade tool  [#19176](https://github.com/vitessio/vitess/pull/19176) 
#### CLI
 * `vtbench`: add `--db-credentials-*` flags [#18913](https://github.com/vitessio/vitess/pull/18913) 
#### Cluster management
 * Improve Semi-Sync Monitor Behavior to Prevent Errant ERS [#18884](https://github.com/vitessio/vitess/pull/18884) 
#### Docker
 * docker: install mysql-shell from Oracle repo and fix shellcheck warnings [#19456](https://github.com/vitessio/vitess/pull/19456) 
#### Evalengine
 * evalengine: make `JSON_EXTRACT` work with non-static arguments [#19035](https://github.com/vitessio/vitess/pull/19035)
 * evalengine: Fix `NULL` document handling in JSON functions [#19052](https://github.com/vitessio/vitess/pull/19052) 
#### General
 * Escape control bytes in JSON strings [#19270](https://github.com/vitessio/vitess/pull/19270) 
#### Observability
 * Improve cgroup metric management [#18791](https://github.com/vitessio/vitess/pull/18791) 
#### Online DDL
 * copy_state: use a mediumblob instead of a smaller varbinary for lastpk [#18852](https://github.com/vitessio/vitess/pull/18852)
 * Online DDL fix: trip all `--in-order` migrations on dependency error [#19027](https://github.com/vitessio/vitess/pull/19027)
 * Increase GTID position column size to LONGBLOB for VReplication [#19119](https://github.com/vitessio/vitess/pull/19119)
 * increment migration metrics [#19297](https://github.com/vitessio/vitess/pull/19297)
 * vreplication: fix infinite retry loop when terminal error message contains binary data [#19423](https://github.com/vitessio/vitess/pull/19423) 
#### Query Serving
 * Fix handling of tuple bind variables in filtering operations. [#18736](https://github.com/vitessio/vitess/pull/18736)
 * Fix incorrect escaping of dual in canonical formatting [#18891](https://github.com/vitessio/vitess/pull/18891)
 * [Querythrottler]Remove noisy config loading error log [#18904](https://github.com/vitessio/vitess/pull/18904)
 * Properly Strip Keyspace Table Qualifiers in FK Constraints [#18926](https://github.com/vitessio/vitess/pull/18926)
 * bugfix: Rewriter not visiting new operators [#18932](https://github.com/vitessio/vitess/pull/18932)
 * Fix LAST_INSERT_ID() not being updated in ExecutePrimitive [#18948](https://github.com/vitessio/vitess/pull/18948)
 * Fix cross shard/keyspace joins with derived tables containing a `UNION`. [#19046](https://github.com/vitessio/vitess/pull/19046)
 * Skip FOR...UPDATE Clause in Warming Reads Queries [#19060](https://github.com/vitessio/vitess/pull/19060)
 * Re-add `options` to `QueryService` interface [#19075](https://github.com/vitessio/vitess/pull/19075)
 * Fix column offset tracking for `UNION`s to be case insensitive. [#19139](https://github.com/vitessio/vitess/pull/19139)
 * vtgate: defer implicit transaction start until after query planning [#19277](https://github.com/vitessio/vitess/pull/19277)
 * planbuilder: fix panic when SELECT has duplicate subqueries [#19581](https://github.com/vitessio/vitess/pull/19581)
 * sqlparser: preserve current_user in SHOW GRANTS USING clause [#19755](https://github.com/vitessio/vitess/pull/19755) 
#### TabletManager
 * Fix `ReloadSchema` incorrectly using `DisableBinlogs` value in `grpctmclient` [#19085](https://github.com/vitessio/vitess/pull/19085) 
#### VDiff
 * VDiff: Prevent division by 0 when reconciling mismatches for reference tables [#19160](https://github.com/vitessio/vitess/pull/19160)
 * Address a few VDiff concerns [#19413](https://github.com/vitessio/vitess/pull/19413) 
#### VReplication
 * VReplication: Ensure proper handling of keyspace/database names with dashes [#18762](https://github.com/vitessio/vitess/pull/18762)
 * VReplication: Treat ER_BINLOG_CREATE_ROUTINE_NEED_SUPER as unrecoverable [#18784](https://github.com/vitessio/vitess/pull/18784)
 * Run VStream copy only when VGTID requires it, use TablesToCopy in those cases [#18938](https://github.com/vitessio/vitess/pull/18938)
 * VDiff: Handle the case where a workflow's table has been dropped on the source [#18985](https://github.com/vitessio/vitess/pull/18985)
 * MoveTables: Verify that source & target keyspaces are distinct [#19012](https://github.com/vitessio/vitess/pull/19012)
 * vreplication: surface aggregated errors from parallel copy workers [#19166](https://github.com/vitessio/vitess/pull/19166)
 * VReplication: Properly Handle Sequence Table Initialization For Empty Tables [#19226](https://github.com/vitessio/vitess/pull/19226)
 * Normalize the --on-ddl param for MoveTables [#19445](https://github.com/vitessio/vitess/pull/19445)
 * VStreamer: Flush Buffered Events on Filtered Terminal Markers [#19666](https://github.com/vitessio/vitess/pull/19666)
 * workflow: finish switch traffic after post-journal cancel [#19672](https://github.com/vitessio/vitess/pull/19672)
 * VReplication: Improve historian / schema tracker [#19697](https://github.com/vitessio/vitess/pull/19697) 
#### VTAdmin
 * Fix `vtadmin` `package-lock.json` [#18919](https://github.com/vitessio/vitess/pull/18919)
 * VStream: move vstream schema change handling from best effort to reliable [#19204](https://github.com/vitessio/vitess/pull/19204)
 * `VTAdmin`: regenerate vtadmin protos following #19197 [#19418](https://github.com/vitessio/vitess/pull/19418) 
#### VTGate
 * show vitess_keyspaces should not trip over deleted keyspaces [#19055](https://github.com/vitessio/vitess/pull/19055)
 * workflows: avoid accidental deletion to routing rules [#19121](https://github.com/vitessio/vitess/pull/19121)
 * vtgate: Add bounds check in `visitUnion` for mismatched column counts [#19476](https://github.com/vitessio/vitess/pull/19476)
 * vtgate: set ServerStatusAutocommit in handshake status flags [#19628](https://github.com/vitessio/vitess/pull/19628)
 * VTGate: fix warming reads timeout context [#19674](https://github.com/vitessio/vitess/pull/19674)
 * planner: push DISTINCT through Window [#19694](https://github.com/vitessio/vitess/pull/19694) 
#### VTOrc
 * vtorc: add `StaleTopoPrimary` analysis and recovery [#19173](https://github.com/vitessio/vitess/pull/19173)
 * `vtorc`: detect errant GTIDs for replicas not connected to primary [#19224](https://github.com/vitessio/vitess/pull/19224)
 * vtorc: Add a timeout to `DemotePrimary` RPC [#19432](https://github.com/vitessio/vitess/pull/19432)
 * vtorc: add timeout helpers for remaining recovery topo/tmc calls [#19520](https://github.com/vitessio/vitess/pull/19520) 
#### VTTablet
 * Fix bug where query consolidator returns empty result without error when the waiter cap exceeded [#18782](https://github.com/vitessio/vitess/pull/18782)
 * repltracker: reset replica lag when we are primary [#18800](https://github.com/vitessio/vitess/pull/18800)
 * connpool: fix connection leak during idle connection reopen [#18967](https://github.com/vitessio/vitess/pull/18967)
 * Change connection pool idle expiration logic [#19004](https://github.com/vitessio/vitess/pull/19004)
 * binlog_json: fix opaque value parsing to read variable-length [#19102](https://github.com/vitessio/vitess/pull/19102)
 * Bug fix: Add missing db_name filters to vreplication and vdiff queries [#19378](https://github.com/vitessio/vitess/pull/19378)
 * vttablet: handle applier metadata init failures in relay-log recovery [#19560](https://github.com/vitessio/vitess/pull/19560)
 * tabletmanager: handle nil Cnf in MysqlHostMetrics to prevent panic [#19752](https://github.com/vitessio/vitess/pull/19752) 
#### schema management
 * `schemadiff`: support views with CTE [#18893](https://github.com/vitessio/vitess/pull/18893)
 * Fix `DROP CONSTRAINT` to work the same way as MySQL [#19183](https://github.com/vitessio/vitess/pull/19183)
 * sidecardb: make ALTER TABLE algorithm version-aware [#19358](https://github.com/vitessio/vitess/pull/19358)
 * OnlineDDL: Make ENUM/SET eligibility for INSTANT DDL more robust [#19425](https://github.com/vitessio/vitess/pull/19425) 
#### vtctl
 * vschema revert: initialize as nil so that nil checks do not pass later [#19114](https://github.com/vitessio/vitess/pull/19114)
 * vtctl: fix nil pointer dereference in SNAPSHOT keyspace creation [#19120](https://github.com/vitessio/vitess/pull/19120)
 * Restart IO threads on replicas after ERS failure [#19805](https://github.com/vitessio/vitess/pull/19805) 
#### vtctldclient
 * Fix zero timestamp if NO_ZERO_DATE is not enabled [#19185](https://github.com/vitessio/vitess/pull/19185)
### CI/Build 
#### Build/CI
 * `pre-commit` hook: support non-`GOPATH` `goimports` [#18741](https://github.com/vitessio/vitess/pull/18741)
 * make build system name configurable [#18779](https://github.com/vitessio/vitess/pull/18779)
 * ci: use the newest mysql apt config package [#18790](https://github.com/vitessio/vitess/pull/18790)
 * ci: revert to using the oracle apt repo for mysql-8.0 builds [#18813](https://github.com/vitessio/vitess/pull/18813)
 * ci: DRY up MySQL Setup [#18815](https://github.com/vitessio/vitess/pull/18815)
 * gha: bump golang version of release-23.0 [#18821](https://github.com/vitessio/vitess/pull/18821)
 * ci: extract os tuning [#18824](https://github.com/vitessio/vitess/pull/18824)
 * Use `golangci/golangci-lint-action@v9` in Static Code checks [#18928](https://github.com/vitessio/vitess/pull/18928)
 * add percona84 [#18950](https://github.com/vitessio/vitess/pull/18950)
 * Add a workflow to run backport/forwardports. [#18953](https://github.com/vitessio/vitess/pull/18953)
 * Run tests with gotestsum [#19076](https://github.com/vitessio/vitess/pull/19076)
 * `e2e` tests: use `t.Context()` from `testing` as `context.Context` [#19081](https://github.com/vitessio/vitess/pull/19081)
 * Restore `/vthook` to dot-ignore files [#19082](https://github.com/vitessio/vitess/pull/19082)
 * `e2e`: disable gRPC `GOAWAY`/`too_many_pings` keepalive errors in e2e CI [#19083](https://github.com/vitessio/vitess/pull/19083)
 * Support Go 1.26 and later with Swiss maps always enabled [#19088](https://github.com/vitessio/vitess/pull/19088)
 * Fix backport workflow [#19091](https://github.com/vitessio/vitess/pull/19091)
 * Add missing end-to-end tests to config [#19105](https://github.com/vitessio/vitess/pull/19105)
 * Save some time on some tests using synctest [#19112](https://github.com/vitessio/vitess/pull/19112)
 * Split error codes workflow [#19141](https://github.com/vitessio/vitess/pull/19141)
 * Fix backport conflict author [#19144](https://github.com/vitessio/vitess/pull/19144)
 * Move `${{ }}` in error codes workflow into env [#19148](https://github.com/vitessio/vitess/pull/19148)
 * Add dependabot config [#19150](https://github.com/vitessio/vitess/pull/19150)
 * Update golang version CI: remove deprecated `--workflow-update` flag and v20 [#19171](https://github.com/vitessio/vitess/pull/19171)
 * Update go-upgrade to update docker image digests [#19178](https://github.com/vitessio/vitess/pull/19178)
 * switch end-to-end tests to gotestsum [#19182](https://github.com/vitessio/vitess/pull/19182)
 * Bump the all group with 4 updates [#19213](https://github.com/vitessio/vitess/pull/19213)
 * Switch gotestsum output format [#19215](https://github.com/vitessio/vitess/pull/19215)
 * Fix go upgrade workflow [#19216](https://github.com/vitessio/vitess/pull/19216)
 * [release-22.0] CI: Use new Percona Server repo name [#19221](https://github.com/vitessio/vitess/pull/19221)
 * Fix some linting issues [#19246](https://github.com/vitessio/vitess/pull/19246)
 * Address most Docker image vulnerabilities [#19249](https://github.com/vitessio/vitess/pull/19249)
 * Build bootstrap image locally in ci [#19255](https://github.com/vitessio/vitess/pull/19255)
 * Consolidate CI test workflows [#19259](https://github.com/vitessio/vitess/pull/19259)
 * `ci`: cache Minio package in workflows, make action [#19278](https://github.com/vitessio/vitess/pull/19278)
 * `ci`: cache `percona-release` debian pkg, make an action [#19280](https://github.com/vitessio/vitess/pull/19280)
 * Fix go upgrade tool [#19290](https://github.com/vitessio/vitess/pull/19290)
 * Don't add "Skip CI" label for Go upgrade PRs [#19307](https://github.com/vitessio/vitess/pull/19307)
 * Build boostrap image for local/region example CI [#19310](https://github.com/vitessio/vitess/pull/19310)
 * `ci`: run PR labeler after the `add_initial_labels` action, add label cases [#19312](https://github.com/vitessio/vitess/pull/19312)
 * Explicitly pass local image tags in example CI [#19320](https://github.com/vitessio/vitess/pull/19320)
 * Add lite image build CI job [#19321](https://github.com/vitessio/vitess/pull/19321)
 * try to fix setup mysql [#19371](https://github.com/vitessio/vitess/pull/19371)
 * CI: Fix workflows that install xtrabackup [#19383](https://github.com/vitessio/vitess/pull/19383)
 * Bump the all group across 1 directory with 4 updates [#19409](https://github.com/vitessio/vitess/pull/19409)
 * Fix vtadmin proto check [#19419](https://github.com/vitessio/vitess/pull/19419)
 * `ci`: run code coverage CI only on go packages that had changes [#19431](https://github.com/vitessio/vitess/pull/19431)
 * `e2e`: fix race in `TestFailingReplication` [#19547](https://github.com/vitessio/vitess/pull/19547)
 * Bump the all group with 5 updates [#19556](https://github.com/vitessio/vitess/pull/19556)
 * `ci`: fix whitespace in workflow, lint workflows [#19576](https://github.com/vitessio/vitess/pull/19576)
 * Upgrade `libperconaserverclient22` to `libperconaserverclient24` [#19631](https://github.com/vitessio/vitess/pull/19631)
 * `ci`: only cache `action/setup-go` action on `main` [#19634](https://github.com/vitessio/vitess/pull/19634)
 * ci: use `bash -e {0}` in composite actions  [#19707](https://github.com/vitessio/vitess/pull/19707)
 * ci: add workflow to rerun failed CI jobs on open PRs  [#19715](https://github.com/vitessio/vitess/pull/19715)
 * `ci`: set dependabot group name [#19723](https://github.com/vitessio/vitess/pull/19723)
 * `ci`: add instructions for Copilot [#19724](https://github.com/vitessio/vitess/pull/19724)
 * `ci`: skip Code Coverage CI on backports [#19726](https://github.com/vitessio/vitess/pull/19726)
 * build(deps): bump the actions group with 2 updates [#19732](https://github.com/vitessio/vitess/pull/19732)
 * ci: use zdiff3 conflict style in backport workflow [#19766](https://github.com/vitessio/vitess/pull/19766)
 * docker: use shared buildkit cache scope for bootstrap images [#19770](https://github.com/vitessio/vitess/pull/19770)
 * ci: add `setup-go` composite action [#19784](https://github.com/vitessio/vitess/pull/19784)
 * go-upgrade: fix Go image digest rewrite matching [#19820](https://github.com/vitessio/vitess/pull/19820) 
#### Docker
 * Bump node from `3746b7c` to `32f4586` in /docker/lite in the all group across 1 directory [#19408](https://github.com/vitessio/vitess/pull/19408)
 * docker: upgrade lite base images from Debian Bookworm to Trixie [#19440](https://github.com/vitessio/vitess/pull/19440)
 * Bump the all group across 2 directories with 2 updates [#19555](https://github.com/vitessio/vitess/pull/19555)
 * build(deps): bump node from `6bee70a` to `04190ed` in /docker/lite in the docker group across 1 directory [#19733](https://github.com/vitessio/vitess/pull/19733)
 * build(deps): bump the docker group across 3 directories with 3 updates [#19815](https://github.com/vitessio/vitess/pull/19815) 
#### Documentation
 * `CLAUDE.md`: add missing patterns from maintainer review feedback [#19813](https://github.com/vitessio/vitess/pull/19813) 
#### General
 * [main] Upgrade the Golang version to `go1.25.3` [#18738](https://github.com/vitessio/vitess/pull/18738)
 * [main] Upgrade the Golang version to `go1.25.6` [#19180](https://github.com/vitessio/vitess/pull/19180)
 * [main] Upgrade the Golang version to `go1.26.1` [#19599](https://github.com/vitessio/vitess/pull/19599) 
#### Java
 * update java packages to use central instead of ossrh [#18765](https://github.com/vitessio/vitess/pull/18765) 
#### Topology
 * `golangci-lint`: add `prealloc` linter, part 3 [#19812](https://github.com/vitessio/vitess/pull/19812) 
#### VTOrc
 * `vtorc`: add `prealloc` linter [#19665](https://github.com/vitessio/vitess/pull/19665) 
#### vtctldclient
 * `golangci-lint`: add `prealloc` linter, part 2 [#19749](https://github.com/vitessio/vitess/pull/19749)
### Compatibility Bug 
#### Query Serving
 * fix streaming binary row corruption in prepared statements [#19381](https://github.com/vitessio/vitess/pull/19381)
 * vtgate: Reject unqualified `*` after comma in `SELECT` list [#19475](https://github.com/vitessio/vitess/pull/19475)
 * sqlparser: replace nested tokenizer with inline versioned comment scanning [#19725](https://github.com/vitessio/vitess/pull/19725) 
#### VTGate
 * fix sqlSelectLimit propagating to subqueries [#18716](https://github.com/vitessio/vitess/pull/18716)
 * vtgate: fix handling of session variables on targeted connections [#19318](https://github.com/vitessio/vitess/pull/19318)
### Dependencies 
#### Backup and Restore
 * Backups: Replace usage of deprecated AWS go SDK s3 features [#19391](https://github.com/vitessio/vitess/pull/19391) 
#### Build/CI
 * Pin actions and docker images to SHAs [#19143](https://github.com/vitessio/vitess/pull/19143)
 * Bump the all group with 9 updates [#19157](https://github.com/vitessio/vitess/pull/19157)
 * Bump the all group with 4 updates [#19269](https://github.com/vitessio/vitess/pull/19269)
 * `ci`: stop installing `etcd` twice in some workflows, skip `protoc` [#19332](https://github.com/vitessio/vitess/pull/19332)
 * `ci`: do not install java where it is not needed [#19339](https://github.com/vitessio/vitess/pull/19339)
 * Update golangci-lint to v2.9.0 for go1.26 support [#19368](https://github.com/vitessio/vitess/pull/19368)
 * Bump github/codeql-action from 4.32.3 to 4.32.4 in the all group [#19469](https://github.com/vitessio/vitess/pull/19469)
 * `e2e`: replace defunct fake data generator [#19526](https://github.com/vitessio/vitess/pull/19526)
 * Bump the all group with 5 updates [#19618](https://github.com/vitessio/vitess/pull/19618)
 * build(deps): bump the all group with 8 updates [#19662](https://github.com/vitessio/vitess/pull/19662)
 * build(deps): bump google.golang.org/grpc from 1.79.2 to 1.79.3 [#19670](https://github.com/vitessio/vitess/pull/19670)
 * build(deps): bump the all group with 2 updates [#19705](https://github.com/vitessio/vitess/pull/19705)
 * Update dependabot minimum-version-age to 14 days (30 days for npm) [#19768](https://github.com/vitessio/vitess/pull/19768)
 * build(deps): bump actions/setup-go from 6.3.0 to 6.4.0 in the actions group [#19772](https://github.com/vitessio/vitess/pull/19772) 
#### Docker
 * Bump the all group across 4 directories with 3 updates [#19268](https://github.com/vitessio/vitess/pull/19268)
 * [main] Upgrade the Golang version to `go1.25.7` [#19306](https://github.com/vitessio/vitess/pull/19306)
 * Bump the all group across 3 directories with 2 updates [#19351](https://github.com/vitessio/vitess/pull/19351)
 * [main] Upgrade the Golang version to `go1.26.0` [#19360](https://github.com/vitessio/vitess/pull/19360)
 * Docker: Address build file CI warnings [#19363](https://github.com/vitessio/vitess/pull/19363)
 * Bump the all group across 3 directories with 2 updates [#19467](https://github.com/vitessio/vitess/pull/19467)
 * Bump the all group across 2 directories with 2 updates [#19616](https://github.com/vitessio/vitess/pull/19616)
 * build(deps): bump the all group across 3 directories with 3 updates [#19661](https://github.com/vitessio/vitess/pull/19661)
 * build(deps): bump golang from `96b2878` to `ce3f1c8` in /docker/lite in the all group across 1 directory [#19702](https://github.com/vitessio/vitess/pull/19702)
 * build(deps): bump the docker group across 2 directories with 3 updates [#19821](https://github.com/vitessio/vitess/pull/19821)
 * [main] Upgrade the Golang version to `go1.26.2` [#19830](https://github.com/vitessio/vitess/pull/19830) 
#### Documentation
 * Add `--log-format json|text` [#19395](https://github.com/vitessio/vitess/pull/19395) 
#### General
 * Dockerfile: bump to go 1.25.3 with everything else [#18828](https://github.com/vitessio/vitess/pull/18828)
 * `golangci-lint`: add `mirror`+`nolintlint` linters + fixes [#18863](https://github.com/vitessio/vitess/pull/18863)
 * Bump the all group across 1 directory with 39 updates [#19196](https://github.com/vitessio/vitess/pull/19196)
 * Bump the all group with 2 updates [#19235](https://github.com/vitessio/vitess/pull/19235)
 * Bump the all group with 9 updates [#19262](https://github.com/vitessio/vitess/pull/19262)
 * Bump the all group with 12 updates [#19350](https://github.com/vitessio/vitess/pull/19350)
 * Bump filippo.io/edwards25519 from 1.1.0 to 1.1.1 [#19421](https://github.com/vitessio/vitess/pull/19421)
 * Bump the all group across 1 directory with 15 updates [#19468](https://github.com/vitessio/vitess/pull/19468)
 * Bump the all group across 1 directory with 17 updates [#19617](https://github.com/vitessio/vitess/pull/19617)
 * trace: add OpenTelemetry backend, deprecate OpenTracing [#19619](https://github.com/vitessio/vitess/pull/19619)
 * build(deps): bump the all group with 7 updates [#19659](https://github.com/vitessio/vitess/pull/19659)
 * build(deps): bump the all group with 10 updates [#19703](https://github.com/vitessio/vitess/pull/19703)
 * build(deps): bump the go group across 1 directory with 8 updates [#19763](https://github.com/vitessio/vitess/pull/19763)
 * Upgrade the Golang Dependencies [#19774](https://github.com/vitessio/vitess/pull/19774) 
#### Java
 * Bump the all group in /java with 31 updates [#19154](https://github.com/vitessio/vitess/pull/19154)
 * Bump the all group in /java with 11 updates [#19267](https://github.com/vitessio/vitess/pull/19267)
 * Bump the all group in /java with 3 updates [#19348](https://github.com/vitessio/vitess/pull/19348)
 * Bump org.apache.maven.plugins:maven-surefire-plugin from 3.5.4 to 3.5.5 in /java in the all group [#19465](https://github.com/vitessio/vitess/pull/19465)
 * Bump the all group in /java with 2 updates [#19552](https://github.com/vitessio/vitess/pull/19552)
 * Bump the all group in /java with 2 updates [#19614](https://github.com/vitessio/vitess/pull/19614)
 * build(deps): bump the all group in /java with 8 updates [#19658](https://github.com/vitessio/vitess/pull/19658)
 * build(deps): bump the all group in /java with 2 updates [#19700](https://github.com/vitessio/vitess/pull/19700)
 * build(deps): bump io.netty:netty-handler from 4.2.11.Final to 4.2.12.Final in /java in the java group [#19729](https://github.com/vitessio/vitess/pull/19729)
 * [release-24.0] build(deps): bump org.apache.logging.log4j:log4j-core from 2.25.3 to 2.25.4 in /java (#19839) [#19840](https://github.com/vitessio/vitess/pull/19840) 
#### Observability
 * Bump go.opentelemetry.io/otel/sdk from 1.39.0 to 1.40.0 [#19525](https://github.com/vitessio/vitess/pull/19525)
 * build(deps): bump go.opentelemetry.io/otel/sdk from 1.42.0 to 1.43.0 [#19824](https://github.com/vitessio/vitess/pull/19824) 
#### Query Serving
 * sqlparser: extract goyacc into github.com/vitessio/goyacc [#19706](https://github.com/vitessio/vitess/pull/19706)
 * sqlparser: bump goyacc version [#19717](https://github.com/vitessio/vitess/pull/19717)
 * sqlparser: upgrade goyacc and fix incorrect `$$` usage in list rules [#19744](https://github.com/vitessio/vitess/pull/19744) 
#### VTAdmin
 * Bump lodash-es from 4.17.21 to 4.17.23 in /web/vtadmin [#19191](https://github.com/vitessio/vitess/pull/19191)
 * Bump lodash from 4.17.21 to 4.17.23 in /web/vtadmin [#19192](https://github.com/vitessio/vitess/pull/19192)
 * Bump the all group across 1 directory with 38 updates [#19197](https://github.com/vitessio/vitess/pull/19197)
 * Bump the all group in /web/vtadmin with 18 updates [#19466](https://github.com/vitessio/vitess/pull/19466)
 * Bump rollup in /web/vtadmin [#19492](https://github.com/vitessio/vitess/pull/19492)
 * Bump the all group in /web/vtadmin with 10 updates [#19553](https://github.com/vitessio/vitess/pull/19553)
 * Bump immutable from 5.0.3 to 5.1.5 in /web/vtadmin [#19571](https://github.com/vitessio/vitess/pull/19571)
 * Bump svgo from 4.0.0 to 4.0.1 in /web/vtadmin [#19574](https://github.com/vitessio/vitess/pull/19574)
 * Bump the all group in /web/vtadmin with 8 updates [#19615](https://github.com/vitessio/vitess/pull/19615)
 * build(deps): bump undici from 7.22.0 to 7.24.1 in /web/vtadmin [#19641](https://github.com/vitessio/vitess/pull/19641)
 * build(deps): bump 8 vtadmin web dependencies [#19660](https://github.com/vitessio/vitess/pull/19660)
 * build(deps-dev): bump flatted from 3.3.3 to 3.4.2 in /web/vtadmin [#19685](https://github.com/vitessio/vitess/pull/19685)
 * build(deps): bump picomatch in /web/vtadmin [#19714](https://github.com/vitessio/vitess/pull/19714)
 * build(deps): bump path-to-regexp from 8.3.0 to 8.4.0 in /web/vtadmin [#19748](https://github.com/vitessio/vitess/pull/19748)
 * VTAdmin: migrate from `eslint-config-react-app` to ESLint 9 flat config [#19767](https://github.com/vitessio/vitess/pull/19767)
 * VTAdmin: upgrade npm dependencies [#19776](https://github.com/vitessio/vitess/pull/19776)
 * build(deps-dev): bump lodash from 4.17.23 to 4.18.1 in /web/vtadmin [#19780](https://github.com/vitessio/vitess/pull/19780)
 * build(deps-dev): bump vite from 8.0.3 to 8.0.5 in /web/vtadmin [#19806](https://github.com/vitessio/vitess/pull/19806)
 * build(deps): bump protobufjs from 7.x to 8.0 in /web/vtadmin [#19817](https://github.com/vitessio/vitess/pull/19817) 
#### VTGate
 * feat: add support for `COM_BINLOG_DUMP_GTID` in the `vtgate` MySQL server [#18731](https://github.com/vitessio/vitess/pull/18731)
### Documentation 
#### Documentation
 * `vtorc`: deprecate snapshot topology feature in v24 [#19070](https://github.com/vitessio/vitess/pull/19070)
 * Add PlanetScale to `ADOPTERS.MD` [#19111](https://github.com/vitessio/vitess/pull/19111)
 * Add Uber to ADOPTERS.MD [#19123](https://github.com/vitessio/vitess/pull/19123)
 * docs: Add changelog entries for MySQL CLONE support and restore hook backup engine variable [#19436](https://github.com/vitessio/vitess/pull/19436)
 * docs: Add changelog entry for removal of deprecated VTOrc `/api/replication-analysis` endpoint [#19498](https://github.com/vitessio/vitess/pull/19498)
 * Add missing v24.0.0 changelog entries for #19460 and #19427 [#19499](https://github.com/vitessio/vitess/pull/19499)
 * Add `resolve-backport` skill [#19639](https://github.com/vitessio/vitess/pull/19639) 
#### Evalengine
 * docs: Add changelog entry for JSON_EXTRACT dynamic path arguments [#19045](https://github.com/vitessio/vitess/pull/19045) 
#### Examples
 * fix: update teardown script reference in examples README [#19580](https://github.com/vitessio/vitess/pull/19580) 
#### Governance
 * Use existing maintainers for steering committee [#19773](https://github.com/vitessio/vitess/pull/19773)
 * Add Mohamed as a Vitess maintainer [#19783](https://github.com/vitessio/vitess/pull/19783) 
#### Query Serving
 * Comments on subquery builder [#19218](https://github.com/vitessio/vitess/pull/19218)
 * sqlparser: document codegen in AGENTS.md [#19764](https://github.com/vitessio/vitess/pull/19764) 
#### VTGate
 * add extra documentation for `buffer_window` flag [#19039](https://github.com/vitessio/vitess/pull/19039)
 * docs: Add changelog entry for binlog streaming support [#19832](https://github.com/vitessio/vitess/pull/19832) 
#### VTOrc
 * docs: Add changelog entry for VTOrc logging and metric improvements [#19016](https://github.com/vitessio/vitess/pull/19016)
 * docs: Add changelog entry for VTOrc --cell flag [#19067](https://github.com/vitessio/vitess/pull/19067)
### Enhancement 
#### Backup and Restore
 * add hostname to backup manifest [#18847](https://github.com/vitessio/vitess/pull/18847)
 * mysqlctl,vttablet,vtbackup: support restore tablet and vtbackup restore phase with CLONE [#19084](https://github.com/vitessio/vitess/pull/19084)
 * go/vt/mysqlctl: run backup init sql after catchup, disable super readonly [#19713](https://github.com/vitessio/vitess/pull/19713) 
#### Build/CI
 * Don't hardcode the go version to use for upgrade/downgrade tests. [#18920](https://github.com/vitessio/vitess/pull/18920)
 * Fix some issues with the backport / forwardport workflow [#18954](https://github.com/vitessio/vitess/pull/18954)
 * Port `vitess-bot` to Actions [#19048](https://github.com/vitessio/vitess/pull/19048)
 * Delete .github/workflows/backport.yml [#19051](https://github.com/vitessio/vitess/pull/19051)
 * Fix git hooks with worktrees [#19132](https://github.com/vitessio/vitess/pull/19132)
 * `ci`: try `actions/labeler` to auto-label PRs [#19300](https://github.com/vitessio/vitess/pull/19300) 
#### CLI
 * tests: add unit tests for vtbench package [#19025](https://github.com/vitessio/vitess/pull/19025) 
#### Docker
 * docker: add arm64 support to `install_dependencies.sh` [#19739](https://github.com/vitessio/vitess/pull/19739) 
#### Documentation
 * docs: Add changelog entry for OnlineDDL in_order_completion_pending_count field [#19031](https://github.com/vitessio/vitess/pull/19031)
 * docs: Add changelog entry for tablet shutdown tracking and validation [#19037](https://github.com/vitessio/vitess/pull/19037)
 * Add structured logging with `slog` [#19327](https://github.com/vitessio/vitess/pull/19327) 
#### General
 * Create e2e tests AI skill [#19396](https://github.com/vitessio/vitess/pull/19396)
 * stats: expose full Go runtime/metrics via Prometheus [#19653](https://github.com/vitessio/vitess/pull/19653) 
#### Observability
 * Change tracingService methods to use contexts [#19093](https://github.com/vitessio/vitess/pull/19093) 
#### Online DDL
 * OnlineDDL: add in-order-completion count to show outputs [#18966](https://github.com/vitessio/vitess/pull/18966)
 * OnlineDDL: Improve Safety of CREATE/DROP TABLE phases [#19242](https://github.com/vitessio/vitess/pull/19242)
 * OnlineDDL: always close lock connection [#19586](https://github.com/vitessio/vitess/pull/19586) 
#### Query Serving
 * [QueryThrottler]Add query attributes extraction [#18706](https://github.com/vitessio/vitess/pull/18706)
 * planner: allow pushdown of window functions partitioned by unique vindex [#18903](https://github.com/vitessio/vitess/pull/18903)
 * Fix query planning for complex queries with impossible conditions. [#19003](https://github.com/vitessio/vitess/pull/19003)
 * Add support for SO_REUSEPORT to vtgate [#19168](https://github.com/vitessio/vitess/pull/19168)
 * sqlparser: enforce bare `*` restriction in grammar [#19585](https://github.com/vitessio/vitess/pull/19585)
 * asthelpergen: order value-type comparisons before complex ones in equals [#19601](https://github.com/vitessio/vitess/pull/19601) 
#### TabletManager
 * Add new `force` flag to `DemotePrimary` to force a demotion even when blocked on waiting for semi-sync acks [#18714](https://github.com/vitessio/vitess/pull/18714) 
#### Throttler
 * [QueryThrottler]Replace polling with topo server watch for config updates [#18979](https://github.com/vitessio/vitess/pull/18979) 
#### Topology
 * Add ZooKeeper connection metrics to zk2topo [#19757](https://github.com/vitessio/vitess/pull/19757) 
#### VDiff
 * vdiff: do not sort by table name in summary, it is not necessary [#18972](https://github.com/vitessio/vitess/pull/18972)
 * VDiff: Do not intentionally timeout query, and display diff sample binary columns as hex [#19044](https://github.com/vitessio/vitess/pull/19044) 
#### VReplication
 * VReplication: Handle compatible DDL changes [#18739](https://github.com/vitessio/vitess/pull/18739)
 * VReplication: Handle vstream stalls more effectively [#18752](https://github.com/vitessio/vitess/pull/18752)
 * VReplication: Show all tables in copy progress output [#18825](https://github.com/vitessio/vitess/pull/18825)
 * VStream: Prevent buffering entire transactions (OOM risk), instead send chunks to client [#18849](https://github.com/vitessio/vitess/pull/18849)
 * vstreamer: improve error message when table not found due to keyspace… [#18961](https://github.com/vitessio/vitess/pull/18961)
 * VDiff: wait for workflow lock with exponential backoff [#18998](https://github.com/vitessio/vitess/pull/18998)
 * VReplication: Add --shards flag to MoveTables/Reshard start and stop commands [#19023](https://github.com/vitessio/vitess/pull/19023)
 * Add support for view routing rules [#19104](https://github.com/vitessio/vitess/pull/19104) 
#### VTAdmin
 * movetables mirrortraffic: let read-only queries bypass denied tables [#18775](https://github.com/vitessio/vitess/pull/18775)
 * VReplication/VTAdmin: Clean up VReplication lag related metrics [#18802](https://github.com/vitessio/vitess/pull/18802)
 * VReplication: Add support for binlog_rows_query_log_events [#19441](https://github.com/vitessio/vitess/pull/19441) 
#### VTCombo
 * tests: add unit tests for vtcombo package [#19029](https://github.com/vitessio/vitess/pull/19029) 
#### VTGate
 * add DialCustom for overridden runtime behavior [#18733](https://github.com/vitessio/vitess/pull/18733)
 * Performance: use `IsSingleShard()` check in `pushDerived` instead of just `engine.EqualUnique` opcode [#18974](https://github.com/vitessio/vitess/pull/18974)
 * VTTablet: do not return query results on warming reads [#19656](https://github.com/vitessio/vitess/pull/19656) 
#### VTOrc
 * `vtorc`: improve logging in `DiscoverInstance`, remove old metric [#19010](https://github.com/vitessio/vitess/pull/19010)
 * `vtorc`: add cell/location context [#19047](https://github.com/vitessio/vitess/pull/19047)
 * VTOrc: Monitor for flappy / overloaded primaries and PRS->ERS as needed [#19328](https://github.com/vitessio/vitess/pull/19328)
 * Add flag for cells to watch to VTOrc [#19354](https://github.com/vitessio/vitess/pull/19354)
 * `vtorc`: support analysis ordering, improve semi-sync rollout [#19427](https://github.com/vitessio/vitess/pull/19427)
 * `vtorc`: improvements to analysis ordering, handle semi-sync disable [#19488](https://github.com/vitessio/vitess/pull/19488)
 * vtorc: change `StaleTopoPrimary` recovery to be best-effort [#19502](https://github.com/vitessio/vitess/pull/19502)
 * vtorc: use `Ping` for `IncapacitatedPrimary` [#19514](https://github.com/vitessio/vitess/pull/19514) 
#### VTTablet
 * Add metrics to track requests throttled in QueryThrottler [#18740](https://github.com/vitessio/vitess/pull/18740)
 * `grpctmclient`: validate tablet record on dial [#19009](https://github.com/vitessio/vitess/pull/19009)
 * vttablet: set time when we start a tablet for use later [#19018](https://github.com/vitessio/vitess/pull/19018)
 * `tabletmanager`: return semi-sync state in `StopReplicationAndGetStatus` RPC [#19074](https://github.com/vitessio/vitess/pull/19074)
 * Pass backup engine to vttablet_restore_done hook [#19405](https://github.com/vitessio/vitess/pull/19405)
 * Fire vttablet_restore_done hook from RestoreFromBackup RPC path [#19592](https://github.com/vitessio/vitess/pull/19592)
 * OnlineDDL: set `wait_timeout` on cutover connections [#19630](https://github.com/vitessio/vitess/pull/19630)
 * fix: Add sqlerror.ERRestartServerFailed to clone errors [#19759](https://github.com/vitessio/vitess/pull/19759)
 * feat: add workflow_type label to VReplication Prometheus metrics [#19786](https://github.com/vitessio/vitess/pull/19786) 
#### vtctl
 * `EmergencyReparentShard`: require stop replication error to be from `PRIMARY` [#19515](https://github.com/vitessio/vitess/pull/19515)
### Feature 
#### Backup and Restore
 * Backups: Add Support for Backup Init SQL Queries [#18883](https://github.com/vitessio/vitess/pull/18883)
 * mysqlctl: add `CloneFromDonor`  [#19064](https://github.com/vitessio/vitess/pull/19064) 
#### Cluster management
 * Topo lookup tablet type [#18777](https://github.com/vitessio/vitess/pull/18777) 
#### Evalengine
 * evalengine: Implement `JSON_REMOVE` [#19041](https://github.com/vitessio/vitess/pull/19041) 
#### Query Serving
 * Add session algorithm to tablet balancer [#19007](https://github.com/vitessio/vitess/pull/19007)
 * sqlparser: Proper parsing for various `SHOW` statements [#19252](https://github.com/vitessio/vitess/pull/19252) 
#### VDiff
 * vdiff: add per-shard per-table state to BuildSummary [#19359](https://github.com/vitessio/vitess/pull/19359) 
#### VReplication
 * vreplication: ignore tablets with unrecoverable errors and try others [#19374](https://github.com/vitessio/vitess/pull/19374) 
#### VTCombo
 * vttest: add gateway-initial-tablet-timeout flag to enable faster startup [#18887](https://github.com/vitessio/vitess/pull/18887) 
#### VTGate
 * Add another tablet load balancer algorithm: random [#18787](https://github.com/vitessio/vitess/pull/18787)
 * Implement "Tablet Targeting" RFC [#18809](https://github.com/vitessio/vitess/pull/18809)
 * vtgate: expose `BinlogDumpGTID` via gRPC [#19689](https://github.com/vitessio/vitess/pull/19689) 
#### VTTablet
 * mysqlctl: add MySQL CLONE support infrastructure [#19062](https://github.com/vitessio/vitess/pull/19062)
 * feat: add connection pool waiter cap to limit queued requests [#19811](https://github.com/vitessio/vitess/pull/19811)
### Internal Cleanup 
#### Backup and Restore
 * Backups: Cap the number of backups to list [#19142](https://github.com/vitessio/vitess/pull/19142) 
#### Build/CI
 * Fix expired MySQL GPG keys in Docker builds [#18795](https://github.com/vitessio/vitess/pull/18795)
 * `vttest`: reduce flakiness from `randomPort()` [#18899](https://github.com/vitessio/vitess/pull/18899)
 * ci: remove code patching for error differences between versions [#18990](https://github.com/vitessio/vitess/pull/18990)
 * `golangci-lint`: fix `QF1008` stragglers from PR #18929 [#19049](https://github.com/vitessio/vitess/pull/19049)
 * Update `golangci-lint`, add `gofumpt` and `modernize`, and misc. [#19208](https://github.com/vitessio/vitess/pull/19208) 
#### Docker
 * `ci`: use `etcd` v3.5.25, add retries [#19015](https://github.com/vitessio/vitess/pull/19015) 
#### Documentation
 * fix: replace deprecated maven-badges.herokuapp.com with maven-badges [#19382](https://github.com/vitessio/vitess/pull/19382)
 * `CLAUDE.md`: require git commit signoff and go formatting [#19401](https://github.com/vitessio/vitess/pull/19401)
 * `CLAUDE.md`: required code is formatted by `goimports` [#19417](https://github.com/vitessio/vitess/pull/19417)
 * `CLAUDE.md`: add further recommendations based on testing  [#19671](https://github.com/vitessio/vitess/pull/19671) 
#### Examples
 * Delete examples/compose directory [#19260](https://github.com/vitessio/vitess/pull/19260) 
#### General
 * `golangci-lint`: add `copyloopvar` linter and auto-fixes  [#18829](https://github.com/vitessio/vitess/pull/18829)
 * `golangci-lint`: add `whitespace` linter and auto-fixes [#18834](https://github.com/vitessio/vitess/pull/18834)
 * `golangci-lint`: add `sqlclosecheck` linter and fixes [#18844](https://github.com/vitessio/vitess/pull/18844)
 * `golangci-lint`: add `nosprintfhostport` linter, fix problems [#18902](https://github.com/vitessio/vitess/pull/18902)
 * Add `timvaillancourt` to more `CODEOWNERS` [#18917](https://github.com/vitessio/vitess/pull/18917)
 * Cleanup `.golangci.yml` [#18929](https://github.com/vitessio/vitess/pull/18929)
 * Update repo dotfile configs [#19071](https://github.com/vitessio/vitess/pull/19071)
 * Lint only changed packages [#19397](https://github.com/vitessio/vitess/pull/19397)
 * Fix golangci-lint changed package arg  [#19406](https://github.com/vitessio/vitess/pull/19406)
 * Move `.claude/skills` to `.agents/skills` and symlink [#19787](https://github.com/vitessio/vitess/pull/19787) 
#### Observability
 * Cleanup dead tracing code [#19092](https://github.com/vitessio/vitess/pull/19092) 
#### Online DDL
 * Remove old GC table name format [#18981](https://github.com/vitessio/vitess/pull/18981)
 * OnlineDDL: Set eta_seconds to 0 when transitioning to ready_to_complete [#19454](https://github.com/vitessio/vitess/pull/19454) 
#### VReplication
 * VDiff: Minor Improvements to Error Logging [#19006](https://github.com/vitessio/vitess/pull/19006)
 * fix: move nil guard before BinlogSource dereference in stopSourceStreams [#19387](https://github.com/vitessio/vitess/pull/19387) 
#### VTAdmin
 * `vtadmin`: remove `graphql` and `rest` imports [#18909](https://github.com/vitessio/vitess/pull/18909) 
#### VTGate
 * `vtgate`: default to `--legacy-replication-lag-algorithm=false` [#18915](https://github.com/vitessio/vitess/pull/18915)
 * remove: grpc-send-session-in-streaming flag [#19443](https://github.com/vitessio/vitess/pull/19443) 
#### VTOrc
 * `vtorc`: use `*topoprotopb.TabletAlias` for tablet alias [#18389](https://github.com/vitessio/vitess/pull/18389)
 * `vtorc`: address CodeQL scanning alerts [#18753](https://github.com/vitessio/vitess/pull/18753) 
#### VTTablet
 * `tabletmanager`: remove v22 backwards-compatibility for `vterrors` migration [#18986](https://github.com/vitessio/vitess/pull/18986)
 * connpool: use context.AfterFunc to avoid goroutine-per-query [#19654](https://github.com/vitessio/vitess/pull/19654)
### Other 
#### Documentation
 * docs: Add changelog entry for VReplication automatic tablet retry for tablet-specific errors [#19797](https://github.com/vitessio/vitess/pull/19797)
 * docs: Add changelog entry for extended Go runtime metrics via Prometheus [#19798](https://github.com/vitessio/vitess/pull/19798) 
#### Other
 * go/cmd/vtbackup: add `--restore-from-clone` support to `vtbackup` [#19089](https://github.com/vitessio/vitess/pull/19089)
### Performance 
#### General
 * `golangci-lint`: add `perfsprint` linter and auto-fixes [#18830](https://github.com/vitessio/vitess/pull/18830)
 * stats: reduce counter lock contention with RWMutex and atomics [#19652](https://github.com/vitessio/vitess/pull/19652) 
#### Performance
 * [Stats] Optimize GetSnakeName with sync.Map for concurrent reads [#19146](https://github.com/vitessio/vitess/pull/19146) 
#### Query Serving
 * planner: Stop searching for alternatives once we found a merge opportunity [#18898](https://github.com/vitessio/vitess/pull/18898)
 * sqlparser: remove strings.Clone from NewIdentifierCS [#19683](https://github.com/vitessio/vitess/pull/19683)
 * sqlparser: eagerly compute IdentifierCI.lowered at construction [#19691](https://github.com/vitessio/vitess/pull/19691) 
#### VTGate
 * vtgate: replace heap with tournament tree for k-way merging [#19398](https://github.com/vitessio/vitess/pull/19398) 
#### VTTablet
 * [gRPC-TMClient]Optimizes the gRPC tablet manager client by splitting a shared mutex into two independent locks [#18933](https://github.com/vitessio/vitess/pull/18933)
### Regression 
#### Driver
 * vitessdriver: send string for binary result values [#19542](https://github.com/vitessio/vitess/pull/19542)
 * [release-24.0] vitessdriver: send string for binary result values (#19542) [#19842](https://github.com/vitessio/vitess/pull/19842)
### Release 
#### Docker
 * Docker: Fix mysql80 docker build [#19289](https://github.com/vitessio/vitess/pull/19289)
 * Docker: Add percona84 to the docker build images workflow [#19357](https://github.com/vitessio/vitess/pull/19357) 
#### Documentation
 * docs: Add changelog entry for QueryThrottler event-driven configuration updates [#19190](https://github.com/vitessio/vitess/pull/19190)
 * [main] Copy `v23.0.2` release notes [#19355](https://github.com/vitessio/vitess/pull/19355)
 * [main] Copy `v23.0.3` release notes [#19506](https://github.com/vitessio/vitess/pull/19506)
 * [main] Copy `v22.0.4` release notes [#19511](https://github.com/vitessio/vitess/pull/19511) 
#### General
 * Bump to `v24.0.0-SNAPSHOT` after the `v23.0.0-RC1` release [#18730](https://github.com/vitessio/vitess/pull/18730)
 * [main] Copy `v23.0.0-RC1` release notes [#18758](https://github.com/vitessio/vitess/pull/18758)
 * [main] Copy `v23.0.0-RC2` release notes [#18841](https://github.com/vitessio/vitess/pull/18841)
 * [main] Copy `v23.0.0` release notes [#18865](https://github.com/vitessio/vitess/pull/18865)
 * [main] Copy `v21.0.6` release notes [#18879](https://github.com/vitessio/vitess/pull/18879)
 * [main] Copy `v22.0.2` release notes [#18880](https://github.com/vitessio/vitess/pull/18880)
 * [main] Copy `v22.0.3` release notes [#19283](https://github.com/vitessio/vitess/pull/19283)
 * [main] Copy `v23.0.1` release notes [#19287](https://github.com/vitessio/vitess/pull/19287)
 * [release-24.0] Code Freeze for `v24.0.0-RC1` [#19837](https://github.com/vitessio/vitess/pull/19837)
 * Bump to `v25.0.0-SNAPSHOT` after the `v24.0.0-RC1` release [#19838](https://github.com/vitessio/vitess/pull/19838) 
#### Query Serving
 * docs: Add changelog entry for QueryThrottler observability metrics [#19036](https://github.com/vitessio/vitess/pull/19036)
### Security 
#### Backup and Restore
 * Address dir traversal in file backup storage `GetBackups` RPC [#18814](https://github.com/vitessio/vitess/pull/18814)
 * Restore: make loading compressor commands from `MANIFEST` opt-in [#19460](https://github.com/vitessio/vitess/pull/19460)
 * `backupengine`: disallow path traversals via backup `MANIFEST` on restore [#19470](https://github.com/vitessio/vitess/pull/19470)
 * `mysqlshellbackupengine`: use `fileutil.SafePathJoin(...)` to build path [#19484](https://github.com/vitessio/vitess/pull/19484) 
#### Build/CI
 * Resolve `Pinned-Dependencies` CodeQL issues in CI [#18853](https://github.com/vitessio/vitess/pull/18853)
 * Use `git` command to fetch pinned SHAs, pin `goimports` [#18980](https://github.com/vitessio/vitess/pull/18980)
 * CI: Add harden-runner step to all workflows [#19145](https://github.com/vitessio/vitess/pull/19145)
 * `ci`: pin dependencies for Antithesis tests  [#19782](https://github.com/vitessio/vitess/pull/19782) 
#### General
 * Bump golang.org/x/crypto from 0.42.0 to 0.45.0 [#18918](https://github.com/vitessio/vitess/pull/18918)
 * Bump the all group across 6 directories with 4 updates [#19184](https://github.com/vitessio/vitess/pull/19184) 
#### Java
 * Resolve `commons-lang` vulnerability in Java driver [#18768](https://github.com/vitessio/vitess/pull/18768)
 * Bump org.apache.logging.log4j:log4j-core from 2.24.1 to 2.25.3 in /java [#19063](https://github.com/vitessio/vitess/pull/19063) 
#### VTAdmin
 * Address `Moderate` dependabot vulns in VTAdmin [#18744](https://github.com/vitessio/vitess/pull/18744)
 * vtadmin: upgrade vite to the latest [#18803](https://github.com/vitessio/vitess/pull/18803)
 * Bump js-yaml from 4.1.0 to 4.1.1 in /web/vtadmin [#18908](https://github.com/vitessio/vitess/pull/18908)
 * Drop dependency on `npm`, bump version of `glob`. [#18931](https://github.com/vitessio/vitess/pull/18931)
 * Potential fix for code scanning alert no. 3944: Database query built from user-controlled sources [#18941](https://github.com/vitessio/vitess/pull/18941) 
#### VTTablet
 * `filebackupstorage`: use `fileutil.SafePathJoin` for all path building [#19479](https://github.com/vitessio/vitess/pull/19479)
 * `vttablet`: harden `ExecuteHook` RPC and backup engine flag inputs [#19486](https://github.com/vitessio/vitess/pull/19486) 
#### vtctldclient
 * Potential fix for code scanning alert no. 2992: Clear-text logging of sensitive information [#18754](https://github.com/vitessio/vitess/pull/18754)
 * `vtctldclient GetPermissions`: hide `authentication_string` from response [#18771](https://github.com/vitessio/vitess/pull/18771)
### Testing 
#### Build/CI
 * Stop using Equinix Metal self hosted runners [#18942](https://github.com/vitessio/vitess/pull/18942)
 * Test backports with the bot [#18968](https://github.com/vitessio/vitess/pull/18968)
 * CI: Improve reliability of codecov workflow with larger runner [#18992](https://github.com/vitessio/vitess/pull/18992)
 * tests(tools): add unit tests for astfmtgen tool [#19073](https://github.com/vitessio/vitess/pull/19073)
 * Generate race unit tests [#19078](https://github.com/vitessio/vitess/pull/19078)
 * Skip flaky `TestRedial` test [#19106](https://github.com/vitessio/vitess/pull/19106)
 * CI: Look for expected log message rather than code in Backup tests [#19199](https://github.com/vitessio/vitess/pull/19199)
 * CI: Use new Percona Server repo name [#19214](https://github.com/vitessio/vitess/pull/19214)
 * CI: Deflake two flaky tests [#19364](https://github.com/vitessio/vitess/pull/19364)
 * CI: Deflake Code Coverage workflow [#19388](https://github.com/vitessio/vitess/pull/19388)
 * Fix TestSelectScatterPartialOLAP data race [#19390](https://github.com/vitessio/vitess/pull/19390)
 * CI: Use larger runners for vreplication workflows [#19433](https://github.com/vitessio/vitess/pull/19433)
 * test: Add missing unit tests in `vtgate/cli` [#19439](https://github.com/vitessio/vitess/pull/19439)
 * Testing: Antithesis Test reach Setup Complete [#19577](https://github.com/vitessio/vitess/pull/19577)
 * Testing: Antithesis 1 primary 2 replica setup [#19684](https://github.com/vitessio/vitess/pull/19684) 
#### Cluster management
 * tests(cmd): add tests for mysqlctl position command [#18895](https://github.com/vitessio/vitess/pull/18895)
 * test: fix test flakiness in `TestEmergencyReparentWithBlockedPrimary` [#18959](https://github.com/vitessio/vitess/pull/18959) 
#### General
 * test: showcase using `synctest` [#18701](https://github.com/vitessio/vitess/pull/18701)
 * Fix flaky tests [#18835](https://github.com/vitessio/vitess/pull/18835)
 * Move `TabletManagerClient` mock to `tmclient/mock` package [#19698](https://github.com/vitessio/vitess/pull/19698) 
#### Operator
 * VTOp: Add support for cluster and keyspace level backup schedules [#19734](https://github.com/vitessio/vitess/pull/19734) 
#### Query Serving
 * Add info schema regression test case [#19362](https://github.com/vitessio/vitess/pull/19362) 
#### VReplication
 * CI: Address our two flakiest tests [#19587](https://github.com/vitessio/vitess/pull/19587) 
#### VTGate
 * Fix sporadic TestServingKeyspaces panic on context cancellation [#19163](https://github.com/vitessio/vitess/pull/19163)
 * test: add tests for scatter DML with LIMIT bypass behavior [#19799](https://github.com/vitessio/vitess/pull/19799) 
#### VTOrc
 * vtorc: use syscall.Dup3 instead of Dup2 in test [#19455](https://github.com/vitessio/vitess/pull/19455)
 * vtorc: change `syscall.Dup3` to `unix.Dup2` [#19489](https://github.com/vitessio/vitess/pull/19489)
 * Testing: Add Antithesis integration for Vitess [#19522](https://github.com/vitessio/vitess/pull/19522)

