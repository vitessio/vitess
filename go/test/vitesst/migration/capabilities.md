# vitesst e2e migration capability matrix

Every capability that any classified package needs, whether the framework already
provides it, and which wave first depends on it. "First wave" means the earliest
wave containing a package that cannot run without the capability.

## Capabilities the framework already has

| Capability | First wave used | Heaviest consumers |
|---|---|---|
| bootstrap | 0 | everything |
| sharded | 0 | pilots, all vtgate query suites, onlineddl/vrepl, vreplication |
| vschema | 0 | pilots, all vtgate suites |
| extra-args | 1 | nearly every package (flag variation is the norm) |
| vtctldclient-exec | 1 | vtgate, informationschema, routing-rule suites, all control-plane waves |
| component vars/metrics scraping (vtgate + tablet) | 1 | vtgate, preparestmt, consolidator, viewsdisabled; later messaging, twopc/metric, scheduler, vtorc |
| compare-mysql | 1 | gen4, plan_tests, all queries/* compare suites, prscomplex, twopc |
| schema-tracking flags | 1 | gen4, tpch, schematracker/*, orderby, subquery, union |
| DialVTGate / vtgate gRPC | 1 | godriver, grpc_api, multi_query; later twopc vstream, reference |
| container file injection | 1 | grpc_api and keyspace_watches auth/ACL JSON, vtgate/vschema config file, mysqlserver |
| init-db-custom | 1 | schematracker/sharded + unsharded sidecar names; later recovery, vault, backup init_db rewrites |
| multi-keyspace | 1 | gen4, multi_ks, dml (sharded+unsharded pairs) |
| multi-cell | 2 | readafterwrite, tabletbalancer; wave 3 cellalias, keyspace, topoconncache, reparent |
| multi-vtgate | 2 | tabletbalancer, reuseport; wave 3 cellalias; wave 5 throttler_topo |
| vtgate-restart | 2 | rollback, aggregation, timeout, prevent_cross_keyspace_reads, clustertest; wave 3 cellalias, encryptedtransport |
| tablet-process-control | 2 | createdb_plugin, tablet_healthcheck*, reservedconn, clustertest; waves 3-5 everywhere |
| mysqld-process-control | 2 | transaction/restart, errors_as_warnings, prscomplex; waves 3-5 reparent, backup, tabletmanager |
| vttablet-restart-newflags | 2 | schematracker/restarttablet, twopc/metric RestartOnlyTablet; wave 5 tabletmanager/primary |
| prs-ers (raw vtctldclient exec) | 2 | messaging, twopc, tabletchange, buffer/reparent; wave 3 reparent/vtorc suites |
| tablet status/type waits | 2 | tablet_healthcheck, servingchange; universal from wave 3 on |
| WaitForHealthyShard | 2 | lifecycle suites; wave 6 vreplication |
| TabletProto | 2 | twopc raw tablet dials; wave 5 tabletmanager |

## Capabilities that must be built (gaps)

| Gap | First wave | Needed by | Notes |
|---|---|---|---|
| sequences (vschema autoincrement + sequence tables helper) | 1 | concurrentdml, preparestmt, sequence, dml | Likely thin sugar over vschema, verify it just works |
| grpc-api-host (vtgate gRPC endpoint for arbitrary raw clients, custom dial opts, client certs) | 1 | godriver, grpc_api, multi_query; later encryptedtransport, twopc | DialVTGate exists but raw endpoint exposure with auth/TLS options does not |
| semi-sync durability policy setup | 2 | transaction, twopc, benchmark; wave 3 reparent/vtorc; wave 4 backup | Mostly SetKeyspaceDurabilityPolicy plumbing plus plugin checks |
| tmclient-grpc (direct tablet gRPC / tabletconn / TabletManagerClient) | 2 | binlogdump, twopc; wave 5 tabletmanager, replication_manager | Tablet gRPC ports must be reachable from the test process |
| querylog / component log access | 2 | queries/misc, trace, schematracker/loadkeyspace; wave 3 vtorc log parsing, vault | Needs a log-read or log-tail API equivalent to reading files off disk |
| buffering (failover buffering flags + vars assertions) | 2 | buffer/reparent; wave 3 newfeaturetest; wave 5 buffer/reshard | Composed of extra-args plus vtgate vars, mostly validation work |
| add-tablet-live-shard | 2 | tablet_healthcheck, tablet_healthcheck_cache, tabletbalancer; waves 3-5 reparent, recovery, tabletmanager | Provision an extra tablet against an existing shard at runtime |
| vtadmin-api (vtadmin component + HTTP API) | 2 | vtadmin, transaction/twopc | |
| signal-level process control (SIGTERM drain, SIGSTOP/SIGCONT freeze, SIGKILL by pid) | 2 | connectiondrain, reuseport; wave 3 emergencyreparent; wave 5 twopc fuzz/stress | Container stop is not the same as freeze or kill-now |
| shared test-to-component filesystem (fault-injection files, MANIFEST reads, config rewrites) | 2 | twopc commit-delay files; wave 4 vtctlbackup corruption/vthook; wave 5 schema-change-dir | Probably one shared-volume design serving all three waves |
| raw topo client access (expose topo address/root, topo.OpenServer) | 2 | clustertest etcd client; wave 3 topotest/*, vtorc, topoconncache | |
| vtorc (process, HTTP API, recovery enable/disable, dynamic config) | 3 | vtorc/*, cellalias, tabletgateway, sharded_prs, mysqldown; wave 4 backup suites | |
| topo flavors consul/zk + topo-restart | 3 | topotest/consul, topotest/zk2, topotest/etcd2, vtgate/unsharded | Stop/start the topo container while the cluster keeps serving |
| TLS cert generation (vttlstest) + mTLS flags on all components | 3 | encryption/*, vault | |
| cnf-injection (EXTRA_MY_CNF fragments before mysqld start) | 3 | encryptedreplication; wave 4 backup/clone, clone; wave 5 foreignkey, replication_manager; wave 6 binlog compression | |
| vault component | 3 | vault | Vault container plus env-var wiring and log assertion |
| backup-volume (shared, path-addressable backup storage) | 4 | all backup/*, recovery/* | Must be readable and corruptible from the test process |
| vtbackup component | 4 | backup/vtbackup, backup/clone | |
| xtrabackup engine | 4 | pitr_xtrabackup, xtrabackup, xtrabackupstream, recovery/xtrabackup, vtctlbackup | Host binary or baked into the mysqld image |
| mysqlshell engine | 4 | pitr_mysqlshell, vtctlbackup | Socket paths embedded in flags need a container-aware form |
| minio (S3 emulation) | 4 | backup/s3 | |
| pitr helpers (incremental backup, restore-to-pos/timestamp) | 4 | pitr*, mysqlctld, unshardedrecovery | |
| MySQL CLONE plugin support | 4 | backup/clone, clone | Plugin cnf fragment plus version gating |
| vthook script injection | 4 | vtctlbackup restore hook; wave 5 tabletmanager ExecuteHook | Writable vthook dir inside the vttablet image |
| onlineddl (schema-change-dir controller, migration status helpers) | 5 | onlineddl/*, schemadiff/vrepl, foreignkey/stress, vtgate/schema, twopc fuzz/stress | |
| throttler control (CheckThrottler, topo config, status waits) | 5 | onlineddl/*, throttler_topo, schemadiff/vrepl; wave 6 vreplication | |
| foreign-keys (FK-aware flags plus replica mysqld backdoor SQL) | 5 | foreignkey, foreignkey/stress, scheduler; wave 6 vreplication | |
| direct mysqld socket / host mysql-client path | 5 | vrepl_suite, vrepl_stress_suite, schemadiff/vrepl | The single hardest onlineddl porting problem |
| movetables / reshard / materialize workflows with state and lag polling | 5 | queries/reference, queries/benchmark, buffer/reshard, twopc fuzz/stress, migration | CLI-driven, but needs workflow-state polling helpers |
| vdiff + full vreplication harness (cross-cell, multi-tenant, custom sidecar, cross-version binaries) | 6 | vreplication | Includes binlog-compression cnf and per-test VitessCluster construction |

## Gaps by first wave needing them

- wave 1: sequences helper (vschema autoincrement + sequence tables) — concurrentdml, preparestmt, sequence
- wave 1: grpc-api-host — vtgate gRPC endpoint reachable by arbitrary raw clients with custom dial options and client certs (godriver, grpc_api, multi_query; later encryptedtransport, twopc)
- wave 2: semi-sync durability-policy setup (transaction, twopc; prerequisite for waves 3-4)
- wave 2: tmclient-grpc — direct tablet gRPC/tabletconn access (binlogdump, twopc; later tabletmanager)
- wave 2: querylog/log access — read or tail component log files (queries/misc, trace, loadkeyspace; later vtorc, vault)
- wave 2: buffering flags + buffering vars assertions (buffer/reparent; later newfeaturetest, buffer/reshard)
- wave 2: add-tablet-live-shard — provision an extra tablet into an existing shard at runtime (tablet_healthcheck*, tabletbalancer; later reparent, recovery, tabletmanager)
- wave 2: vtadmin-api — vtadmin component with HTTP API (vtadmin, transaction/twopc)
- wave 2: signal-level process control — SIGTERM drain, SIGSTOP/SIGCONT freeze, immediate SIGKILL of a specific mysqld/vttablet (connectiondrain, reuseport; later emergencyreparent, twopc fuzz/stress)
- wave 2: shared test-to-component filesystem/volume — fault-injection communication files, backup MANIFEST reads/corruption, schema-change-dir (twopc; later vtctlbackup, onlineddl)
- wave 2: raw topo client access — expose topo address/root for topo.OpenServer and native clients (clustertest; later topotest/*, vtorc, topoconncache)
- wave 3: vtorc — process, HTTP API, recovery enable/disable, dynamic config reload (vtorc/*, cellalias, tabletgateway, mysqldown; later backup suites)
- wave 3: topo flavors consul/zk plus topo-restart — stop/start the topo server independently (topotest/*, vtgate/unsharded)
- wave 3: TLS cert generation (vttlstest) and mTLS flags on vtgate/vttablet/tmclient (encryption/*, vault)
- wave 3: cnf-injection — EXTRA_MY_CNF fragments into mysqld before start (encryptedreplication; later clone, foreignkey, replication_manager, vreplication binlog compression)
- wave 3: vault component with env-var wiring (vault)
- wave 4: backup-volume — shared, path-addressable backup storage the test can list, read, and corrupt (all backup/*, recovery/*)
- wave 4: vtbackup component (backup/vtbackup, backup/clone)
- wave 4: xtrabackup engine availability (pitr_xtrabackup, xtrabackup, xtrabackupstream, recovery/xtrabackup, vtctlbackup)
- wave 4: mysqlshell engine with container-aware socket flags (pitr_mysqlshell, vtctlbackup)
- wave 4: minio/S3 emulation (backup/s3)
- wave 4: pitr incremental-backup/restore helpers (pitr*, mysqlctld, unshardedrecovery)
- wave 4: MySQL CLONE plugin support (backup/clone, clone)
- wave 4: vthook script injection into the vttablet image (vtctlbackup restore hook; later tabletmanager ExecuteHook)
- wave 5: onlineddl — schema-change-dir controller and migration-status helpers (onlineddl/*, schemadiff/vrepl, foreignkey/stress, vtgate/schema, twopc fuzz/stress)
- wave 5: throttler control — CheckThrottler, topo throttler config, status waits (onlineddl/*, throttler_topo; later vreplication)
- wave 5: foreign-keys support including replica-mysqld backdoor SQL (foreignkey, foreignkey/stress, scheduler)
- wave 5: direct mysqld socket / host mysql-client execution path (vrepl_suite, vrepl_stress_suite, schemadiff/vrepl)
- wave 5: movetables/reshard/materialize workflows with state and lag polling (queries/reference, queries/benchmark, buffer/reshard, twopc fuzz/stress, migration)
- wave 6: vdiff plus the full vreplication harness — per-test VitessCluster, cross-cell, multi-tenant, custom sidecar DB name, cross-version MySQL binaries, binlog-compression cnf (vreplication)

## Flagged packages

- vitess.io/vitess/go/test/endtoend/utils — the legacy helper package itself: not migrated, deleted once the last consumer moves off it; its own tests are raw-mysqld mysqlctl tests with a port-reservation ordering assumption that no container framework will replicate
- vitess.io/vitess/go/test/endtoend/utils/mysqlvsvitess — tests the legacy utils.NewMySQL helper itself; decide whether to port it as a compare-mysql smoke test or drop it with the utils package
- No CI entry in test/config.json (confirm intent before spending migration effort): binlogdump, clone, migration, stress, transaction/benchmark, vtgate/queries/benchmark, vtgate/trace, vtgate/reuseport
- Benchmarks: transaction/benchmark and vtgate/queries/benchmark are bench-only (not run under plain go test); binlogdump also carries a bench file that dials the mysqld unix socket directly
- backup/s3 — not a cluster e2e at all (fakesqldb + MinIO host binary against mysqlctl internals); consider relocating to go/vt/mysqlctl tests instead of migrating
- vtctldclient — in-process fake gRPC server, effectively a unit test; port unchanged or move out of endtoend
- encryption/unmanagedsslcheck — vttest-based near-unit test of tabletenv.Config.Verify; needs only a bare TLS-required MySQL, not the cluster framework
- vtorc/readtopologyinstance — white-box test linking vtorc internals into the test binary; cannot run against a containerized vtorc, needs a decision (keep as host-process test or rewrite)
- reparent/newfeaturetest — TestSemiSyncBlockDueToDisruption uses sudo pfctl on macOS and is already CI-skipped; treat as non-portable
- vtorc/primaryfailure — two permanently t.Skip'd tests cover no live behavior; candidates for deletion during migration
- vtgate/reuseport — depends on two vtgates sharing one port via SO_REUSEPORT in the same network namespace; may be impossible in a container-per-component model
- transaction/twopc/fuzz and transaction/twopc/stress — highest-risk packages: raw PID-file SIGKILL of mysqld plus concurrent PRS/ERS/reshard/movetables; need explicit kill-now primitives before wave 5
- Classified list is missing packages that DO have CI entries: mysqlctl, mysqlctld, tabletmanager/disk_health_monitor, vtcombo, vtcombo/recreate, versionupgrade — these were never classified and need triage
