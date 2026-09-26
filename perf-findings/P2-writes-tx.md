# P2-writes-tx: write and transaction path through vtgate (2-shard sbtest)

Investigator P2, BASE=30000, base commit aa9ccf9. Base binaries `/home/vt/bin`, patched `/home/vt/bin-P2`.
Patches: `findings/P2-writes-tx.patch` (recommended changes + harness), `findings/P2-prototypes.patch`
(env-var-gated measurement prototypes: parallel MULTI commit, vtgate sequence block cache, TWOPC without
semi-sync). Both are left uncommitted in the worktree `agent-a95937447cb83f6bc`. Raw bench lines: `findings/P2-raw/`.

## TL;DR

1. **Autocommit UPDATE/DELETE by primary key costs 3 MySQL round trips; make it 1** (code, S). vttablet plans every
   single-table UPDATE/DELETE without LIMIT as `UpdateLimit`/`DeleteLimit` and runs it outside a transaction as
   `begin; <dml> limit 10001; commit` (execAsTransaction), only so it can roll back when more than
   `--queryserver-config-max-result-size` rows change. When the WHERE clause pins every (integer) primary key column
   with `=` to a literal or bind variable, at most one row can change, so the patch plans it as plain `Update`/`Delete`
   (one autocommit statement). Measured (A/B 2, 8 threads, 3 rounds): oltp_update_index **tablets −14%, mysqld −16%,
   vtgate −4% CPU/statement, TPS +8%**; at 1 thread (A/B 2b) **latency −19% avg / −25% p95 / −30% p99, TPS +23%**;
   write_only in autocommit mode −10% tablet / −13% mysqld CPU per event. vtexplain shows the effect directly
   (`begin; delete ... limit 10001; commit` → `delete ...`). Same result as `--queryserver-config-passthrough-dmls`
   (A/B 1) without giving up the row-limit protection for other DMLs.
2. **Sequence-backed inserts pay a full extra vtgate→tablet RPC per insert; a vtgate-side block cache removes it**
   (prototype, M). With `P2_SEQ_BLOCK=1000` (vtgate reserves 1000 values per `select next :n values`) a sequence insert
   costs the same as an explicit-id insert: **vtgate −27..−31%, tablets −34..−37% CPU/insert, TPS +32..+36%, latency
   −25..−27%** (A/B 3d). Needs design work (ID ordering across vtgates, ResetSequences after MoveTables).
3. **Defer BEGIN into the first statement (new tablet flag `--queryserver-config-defer-begin`, default off)** (code, M).
   Sends `begin;<first statement>` as one multi-statement COM_QUERY; the tablet-side BEGIN latency drops from 0.14 ms
   to 0.02 ms per shard-transaction (1.75 per write_only tx). End to end the effect is small and mostly within noise:
   mysqld −2..−5% CPU/tx, latency −1..−4% at 1 thread. Keep as an option; not a headline win on a loopback socket
   (worth more with a TCP hop or higher MySQL RTT).
4. **MULTI (default) costs nothing for single-shard transactions; for 2-shard transactions its cost is the sequential
   commit** (1.2–1.5 ms vs 0.6–0.7 ms at 1 thread). Committing shard sessions in parallel (prototype) cut vtgate commit
   time 1.20 → 0.98 ms but raised per-tablet commit time on this CPU-starved 4-vCPU box; no end-to-end gain here.
   TWOPC is expensive (vs MULTI, same binary): **write_only CPU vtgate +43..+58%, tablets +69..+85%, mysqld
   +96..+122%, 18.7 vs 7.5 MySQL statements/tx, commit 5.2 ms vs 1.5 ms, TPS −33..−43%**; single-shard transactions are unaffected by TWOPC.
5. Negative/neutral: session proto serialization (not marshalled per statement for MySQL-protocol clients; 47 B per
   shard session for gRPC clients), tx pool (no waits, 5 µs Get), tx killer/throttler (idle), `SELECT … FOR UPDATE`
   (same cost as a plain select in a transaction), settings pool (`SET innodb_lock_wait_timeout`: no measurable cost),
   SET_VAR sysvars (`SET sql_mode`: mysqld +8% CPU/tx).

## Environment and method

- 4 vCPU shared VM. The other investigator ran VReplication benchmarks (BASE 40000) the whole time. Load average during
  runs was 5–27 (reported per row), mostly 10–20: latency/TPS swing ±20% between rounds. CPU µs per event
  (`/proc/<pid>/stat` utime+stime delta ÷ sysbench events) is the primary metric (typically ±3%).
- Cluster: `perf-findings/harness/P2-cluster.sh` (P1's cluster.sh with `restart` + an `addseq` command that adds an
  unsharded keyspace `seqks` with one tablet and a sequence table, and `sbtest9` in `sbtest` with
  `auto_increment` from `seqks.sbtest_seq`). 2 shards, 1 primary each, 4 × 100k rows, innodb_flush_log_at_trx_commit=2,
  sync_binlog=0.
- `P2-bench.sh WORKLOAD MODE THREADS TIME LABEL [sysbench args]` prints TPS, avg/p95/p99 latency per event (per
  transaction for oltp_write_only/read_write), CPU µs/event for vtgate, both tablets summed (+ sequence tablet),
  both mysqld summed; MySQL statements per event (`Questions`, `Com_begin`, `Com_commit` deltas); and, from
  `/debug/vars`, the average vtgate commit time per commit mode (`CommitModeTimings`) and the tablets' `Queries.BEGIN`
  / `Queries.COMMIT` times. `P2-ab.sh` alternates configs per round (restart vtgate+tablets, purge binlogs, 5 s
  warm-up). Custom workloads: `P2-tx1shard.lua` (BEGIN; SELECT [FOR UPDATE]; UPDATE same id; COMMIT → one shard),
  `P2-insert_seq.lua` (autocommit insert into the sequence-backed table, `SEQ=0` for explicit ids),
  `P2-setvar.lua` (runs a stock sysbench script after `SET SESSION <var>`).
- Incident: during the first A/B 2 attempt the shared disk filled up (both clusters' binlogs + the neighbour's copy),
  mysqld of shard -80 aborted on a binlog write (`binlog_error_action=ABORT_SERVER`) and restarted read-only. That
  attempt is discarded (`ab2-aborted-diskfull.txt`); the harness now purges binlogs before every config.

## Baseline: statements, round trips and CPU per transaction

| workload (8 thr, ps) | TPS | avg ms | vtgate µs/ev | tablets µs/ev | mysqld µs/ev | MySQL stmts/ev |
|---|---|---|---|---|---|---|
| oltp_write_only (BEGIN, 4 DML, COMMIT) | 626–790 | 10–14 | 1150–1230 | 1370–1470 | 1265–1400 | 7.50 |
| oltp_write_only, 32 thr | 908–1139 | 28–37 | 736–797 | 859–899 | 1119–1145 | 7.50 |
| oltp_write_only text protocol | 605 | 13.2 | 1307 | 1453 | 1386 | 7.50 |
| oltp_read_write (18 statements) | 146–175 | 46–59 | 5000–5500 | 5900–6400 | 5200–5300 | 26.05 |
| oltp_write_only --skip_trx (4 autocommit DML) | 756 | 10.6 | 927 | 1264 | 1505 | 10.00 |
| oltp_update_index (autocommit) | 2867–3091 | 2.6–2.8 | 234–246 | 333–346 | 406–421 | 3.00 |
| oltp_insert (autocommit) | 2698–3320 | 2.4–3.2 | 250–262 | 256–278 | 283–313 | 1.00 |
| tx1shard (BEGIN, SELECT, UPDATE, COMMIT) | 1120–1356 | 5.9–8.0 | 635–670 | 729–760 | 670–707 | 4.00 |

Round trips per oltp_write_only transaction (2 shards, MULTI):
- client ↔ vtgate: 6 (BEGIN is handled in vtgate without an RPC).
- vtgate → vttablet: the first statement per shard is a `BeginExecute` (BEGIN is piggybacked on the RPC), then
  `Execute`, then one `Commit` per shard: 1.75 BeginExecute + 2.25 Execute + 1.75 Commit = 5.75 RPCs (the tablets'
  `Queries.BEGIN`/`COMMIT` counts match `Com_begin`/`Com_commit` = 1.75 each).
- vttablet → MySQL: 4 DML + 1.75 `begin` + 1.75 `commit` = 7.5 statements, each its own round trip. No `select 1`,
  savepoints, `set autocommit` or other hidden statements (general log). In a transaction DMLs carry
  `limit 10001` (harmless).
- Autocommit single-row UPDATE/DELETE: 1 RPC, but **3 MySQL statements** (`begin`, `update ... limit 10001`,
  `commit`) — see finding 1. Autocommit INSERT: 1 RPC, 1 statement.
- Multi-shard commit (MULTI) is sequential: at 1 thread `CommitModeTimings` Multi = 1.20–1.50 ms vs Single
  0.59–0.71 ms; tablet `Queries.COMMIT` 0.3 ms, `Queries.BEGIN` 0.14–0.18 ms (1 thread) and 0.4–0.65 ms (8 threads).

Tx pool and friends (8 threads, write_only): `TransactionPoolWaitCount` 0, `TransactionPoolGetConnTime` ≈ 5 µs,
`Kills` 0, tx throttler not running (off by default), `TxSerializer*` 0. Transaction lifetime (`Transactions.commit`)
≈ 9 ms avg, of which the tablet COMMIT is ≈ 0.9 ms and BEGIN ≈ 0.4 ms under load.

### Profiles (oltp_write_only, ps, 8 threads, base)

vtgate (≈1200 µs/tx): the same fixed per-hop costs as P1's point select: `Syscall6` 18% flat, gRPC client
(`ClientConn.Invoke` 14%, loopy writer 13%, reader 13%), scheduler (`findRunnable` 10%). Transaction-specific code is
small: `handleTransactions` 6.8% (of which `TxConn.Commit` 5.9% is almost all the Commit RPC), `handleBegin` 0.7%,
`actionInfo` 0.7%. No Session marshalling, cloning or proto work shows up (MySQL-protocol sessions live in the
connection; `ExecuteOptions` marshal for the RPC is 0.7%).

vttablet (≈700 µs per tablet per tx): gRPC server 50% cumulative, `Syscall6` 22%, futex 10%. `BeginExecute` 17%
(of which `TabletServer.begin` 5.5% = the MySQL `begin` round trip), `Commit` 11% (`TxPool.Commit` 5%). Tx-pool
bookkeeping (`LogTransaction`, `txComplete`, `GetAndLock`, `NewQueryDetail`, logstats) each ≤ 1%.

Conclusion: per transaction, cost ≈ (round trips) × (per-hop fixed cost from P1). The wins are in removing round
trips, not in the Go code on the path.

## Hypotheses tested

| # | hypothesis | outcome |
|---|---|---|
| H1 | Extra statements on the MySQL connection (select 1, savepoints, set autocommit) | None in transactions. But autocommit UPDATE/DELETE runs as begin/DML/commit (3 statements) → finding 1. |
| H2 | BEGIN is piggybacked on vtgate→tablet RPCs | Yes (`BeginExecute`), but the tablet sends `begin` to MySQL in its own round trip → finding 3 (defer begin). |
| H3 | Sequential multi-shard commit (MULTI) doubles commit latency | Yes: Multi 1.2–1.5 ms vs Single 0.6–0.7 ms at 1 thread. Parallel-commit prototype: vtgate commit 1.20 → 0.98 ms, no end-to-end gain on this box (see A/B 2b). |
| H4 | MULTI (default) is costlier than SINGLE for single-shard transactions | No (A/B 3b: within noise). |
| H5 | TWOPC cost | Large: +43..+58% vtgate, +69..+85% tablets, +96..+122% mysqld CPU/tx, 2.5x MySQL statements, commit 3.5x slower (A/B 3b). Requires semi-sync in production (benchmark ran with a hack that ignores the semi-sync check). |
| H6 | Session proto grows with shard sessions and is marshalled per statement | Not for MySQL-protocol clients (no marshal/clone in the profile). gRPC clients send it both ways per call: 54 B (1 shard), 100 B (2), 1.5 KB (32), 12 KB (256 shards); marshal+unmarshal ≈ 1.4 µs (2 shards) … 70 µs (256 shards). Only matters for huge scatter transactions over the gRPC API. |
| H7 | Tx pool contention / killer / throttler | No waits, no kills, throttler off; negligible CPU. |
| H8 | Sequence-backed inserts cost an extra RPC | Yes: vtgate +33%, tablets +51% CPU/insert vs explicit ids (A/B 3a/3d). A vtgate-side block cache removes it (finding 2). |
| H9 | Reserved connections for `SET @@session.x` | In this version sharded keyspaces never reserve for SETs: SET_VAR-capable variables (sql_mode, …) are sent as `/*+ SET_VAR() */` hints, the rest (e.g. innodb_lock_wait_timeout) use the tablet settings pool (`ConnPoolGetSetting` counts). `--enable-system-settings=false` makes vtgate ignore the SET. Temporary tables are not supported in sharded keyspaces. Settings pool: no measurable cost; SET_VAR (sql_mode): mysqld +8.5%, vtgate +4% CPU/tx (A/B 3c). |
| H10 | `SELECT … FOR UPDATE` is costlier than a select in a transaction | No (A/B 3a: tx1shard with/without FOR UPDATE within noise). |
| H11 | Round-1/P1 findings apply here | Yes, unchanged: most per-statement CPU is gRPC + syscalls + scheduler (GOMAXPROCS, stream workers, GOGC, flow-control windows are covered by P1). |

Side finding (correctness, minor): `SET SESSION innodb_lock_wait_timeout = '26'` (a quoted integer, which MySQL
rejects with errno 1232) is accepted by vtgate, which then fails **every following query of the session** with
`Incorrect argument type to variable 'innodb_lock_wait_timeout'` when the tablet applies the setting. The error should
surface at SET time.

## A/B results

Mean of rounds; % vs the first config of each block; [min–max] for CPU columns. "ps" = prepared statements.

### A/B 1: `--queryserver-config-passthrough-dmls` (upper bound for finding 1), base binaries, 3 rounds, load 12–16

| workload | mode | thr | config | n | TPS | avg ms | p95 ms | p99 ms | vtgate µs/ev | tablets µs/ev | mysqld µs/ev | MySQL stmts/ev |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| oltp_update_index | ps | 8 | base | 3 | 1897 | 4.68 | 10.17 | 21.18 | 242 [231–248] | 330 [325–339] | 406 [397–416] | 3.00 |
| oltp_update_index | ps | 8 | passthrough | 3 | 3269 (+72%) | 2.45 (−48%) | 4.77 (−53%) | 7.34 (−65%) | 228 (−5.9%) | 278 (−15.8%) | 343 (−15.5%) | 1.00 |
| oltp_update_non_index | text | 8 | base | 3 | 1859 | 4.40 | 9.48 | 16.66 | 265 | 332 | 376 | 3.00 |
| oltp_update_non_index | text | 8 | passthrough | 3 | 2663 (+43%) | 3.21 (−27%) | 6.78 (−29%) | 11.74 (−30%) | 249 (−6.0%) | 270 (−18.8%) | 318 (−15.4%) | 1.00 |
| write_only --skip_trx | ps | 8 | base | 3 | 614 | 13.22 | 24.19 | 36.98 | 943 | 1265 | 1504 | 10.00 |
| write_only --skip_trx | ps | 8 | passthrough | 3 | 519 (−15%) | 15.55 (+18%) | 29.48 | 45.78 | 888 (−5.8%) | 1053 (−16.8%) | 1262 (−16.1%) | 4.01 |
| oltp_insert | ps | 8 | base | 3 | 2698 | 3.17 | 6.70 | 11.44 | 250 | 262 | 294 | 1.00 |
| oltp_insert | ps | 8 | passthrough | 3 | 2302 (−15%) | 3.65 | 8.25 | 15.66 | 251 (+0.1%) | 261 (−0.4%) | 291 (−0.9%) | 1.00 |
| oltp_write_only | ps | 8 | base | 3 | 576 | 15.06 | 27.93 | 44.49 | 1227 | 1430 | 1336 | 7.49 |
| oltp_write_only | ps | 8 | passthrough | 3 | 643 (+12%) | 12.68 | 23.44 | 36.04 | 1174 (−4.3%) | 1394 (−2.5%) | 1307 (−2.1%) | 7.50 |

(Insert and in-transaction write_only are controls: no change expected; their TPS swings show the latency noise.)

### A/B 2: patch (`p2` = PK DML planning), + `--queryserver-config-defer-begin` (`p2defer`), + parallel MULTI commit prototype (`p2par`); 3 rounds, 8/32 threads, load 10–15

| workload | mode | thr | config | n | TPS | avg ms | p95 ms | p99 ms | vtgate µs/ev | tablets µs/ev | mysqld µs/ev | MySQL stmts/ev |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| oltp_write_only | ps | 8 | base | 3 | 626 | 13.99 | 23.17 | 31.39 | 1196 [1148–1265] | 1417 [1372–1470] | 1327 [1265–1402] | 7.50 |
| oltp_write_only | ps | 8 | p2 | 3 | 615 (−1.8%) | 13.84 | 23.06 | 31.44 | 1177 (−1.6%) | 1465 (+3.4%) | 1340 (+1.0%) | 7.50 |
| oltp_write_only | ps | 8 | p2defer | 3 | 613 (−2.1%) | 14.10 | 24.57 | 35.96 | 1192 (−0.3%) | 1451 (+2.4%) | 1317 (−0.8%) | 7.50 |
| oltp_write_only | ps | 8 | p2par | 3 | 512 (−18%) | 16.77 | 29.21 | 39.84 | 1186 (−0.8%) | 1425 (+0.5%) | 1334 (+0.5%) | 7.50 |
| oltp_write_only | ps | 32 | base | 3 | 908 | 36.98 | 59.32 | 78.61 | 751 [736–778] | 879 [870–891] | 1129 [1121–1145] | 7.50 |
| oltp_write_only | ps | 32 | p2 | 3 | 1075 (+18%) | 29.87 | 45.98 | 58.04 | 712 (−5.1%) | 879 (−0.0%) | 1106 (−2.1%) | 7.50 |
| oltp_write_only | ps | 32 | p2defer | 3 | 1081 (+19%) | 29.66 | 46.76 | 61.27 | 721 (−4.0%) | 857 (−2.5%) | 1069 (−5.4%) | 7.50 |
| oltp_write_only | ps | 32 | p2par | 3 | 1060 (+17%) | 30.16 | 48.79 | 67.57 | 736 (−2.0%) | 848 (−3.5%) | 1074 (−4.9%) | 7.51 |
| oltp_read_write | ps | 8 | base | 3 | 146 | 59.39 | 89.30 | 112.80 | 5207 [4983–5651] | 6151 [5938–6506] | 5329 [5176–5560] | 26.05 |
| oltp_read_write | ps | 8 | p2 | 3 | 166 (+14%) | 50.09 | 75.81 | 97.71 | 4961 (−4.7%) | 6105 (−0.7%) | 5202 (−2.4%) | 26.04 |
| oltp_read_write | ps | 8 | p2defer | 3 | 162 (+11%) | 49.63 | 77.97 | 99.30 | 4923 (−5.5%) | 6106 (−0.7%) | 5168 (−3.0%) | 26.04 |
| oltp_read_write | ps | 8 | p2par | 3 | 163 (+12%) | 49.46 | 81.63 | 104.98 | 4878 (−6.3%) | 6025 (−2.0%) | 5179 (−2.8%) | 26.04 |
| oltp_update_index | ps | 8 | base | 3 | 2867 | 2.80 | 5.48 | 7.99 | 235 [233–238] | 336 [333–339] | 410 [407–413] | 3.00 |
| oltp_update_index | ps | 8 | p2 | 3 | 3089 (+7.7%) | 2.63 (−6.1%) | 5.29 | 7.92 | 225 (−4.1%) | 290 (−13.7%) | 344 (−16.0%) | 1.00 |
| oltp_update_index | ps | 8 | p2defer | 3 | 2649 (−7.6%) | 3.16 | 6.46 | 9.90 | 219 (−6.7%) | 283 (−15.7%) | 343 (−16.3%) | 1.00 |
| oltp_update_index | ps | 8 | p2par | 3 | 2261 (−21%) | 3.85 | 8.47 | 14.33 | 230 (−2.1%) | 296 (−11.8%) | 362 (−11.8%) | 1.00 |
| write_only --skip_trx | ps | 8 | base | 3 | 756 | 10.62 | 17.33 | 23.09 | 927 [907–944] | 1264 [1237–1283] | 1505 [1491–1523] | 10.00 |
| write_only --skip_trx | ps | 8 | p2 | 3 | 820 (+8.4%) | 9.77 (−7.9%) | 15.30 (−12%) | 20.06 (−13%) | 893 (−3.6%) | 1140 (−9.8%) | 1306 (−13.3%) | 4.00 |
| oltp_insert | ps | 8 | base | 3 | 2766 | 3.09 | 6.39 | 9.85 | 259 | 272 | 307 | 1.00 |
| oltp_insert | ps | 8 | p2 | 3 | 2933 (+6.0%) | 2.91 | 5.93 | 9.32 | 255 (−1.5%) | 281 (+3.3%) | 306 (−0.2%) | 1.00 |
| tx1shard | ps | 8 | base | 3 | 1138 | 7.66 | 14.08 | 21.31 | 670 | 760 | 707 | 4.00 |
| tx1shard | ps | 8 | p2 | 3 | 1163 (+2.2%) | 7.41 | 13.40 | 19.30 | 644 (−3.9%) | 764 (+0.5%) | 695 (−1.8%) | 4.00 |
| tx1shard | ps | 8 | p2defer | 3 | 957 (−16%) | 9.11 | 17.89 | 27.33 | 668 (−0.4%) | 759 (−0.2%) | 699 (−1.2%) | 4.00 |

(`p2` changes nothing in vtgate; its vtgate deltas and the TPS/latency swings of unaffected workloads — insert,
in-transaction write_only — show the noise floor: about ±5% CPU and ±20% TPS/latency at this load.)

### A/B 2b: 1 client thread (less queueing noise), 3 rounds, load 4–7

| workload | config | TPS | avg ms | p95 ms | p99 ms | vtgate µs/tx | tablets µs/tx | mysqld µs/tx | vtgate commit Single / Multi ms | tablet BEGIN / COMMIT ms |
|---|---|---|---|---|---|---|---|---|---|---|
| oltp_write_only | base | 214 | 4.68 | 6.19 | 8.08 | 2846 | 2790 | 1797 | 0.59 / 1.20 | 0.14 / 0.29 |
| oltp_write_only | p2 | 214 (+0.3%) | 4.67 | 6.08 | 8.10 | 2819 (−0.9%) | 2869 (+2.8%) | 1795 (−0.1%) | 0.58 / 1.22 | 0.14 / 0.29 |
| oltp_write_only | p2defer | 217 (+1.5%) | 4.63 (−1.0%) | 6.25 | 8.32 | 2740 (−3.7%) | 2766 (−0.9%) | 1740 (−3.2%) | 0.60 / 1.25 | **0.02** / 0.31 |
| oltp_write_only | p2par | 221 (+3.6%) | 4.56 (−2.5%) | 6.35 | 9.77 | 2609 (−8.3%) | 2627 (−5.8%) | 1730 (−3.7%) | 0.63 / **0.98** | 0.02 / 0.42 |
| oltp_read_write | base | 60 | 16.76 | 21.64 | 27.34 | 11544 | 11506 | 6094 | – / 1.22 | 0.15 / 0.31 |
| oltp_read_write | p2 | 57 (−3.9%) | 17.56 | 23.20 | 28.55 | 11055 (−4.2%) | 11539 (+0.3%) | 6078 (−0.3%) | – / 1.24 | 0.15 / 0.31 |
| oltp_read_write | p2defer | 62 (+3.7%) | 16.14 (−3.7%) | 19.90 (−8.0%) | 25.09 (−8.2%) | 11408 (−1.2%) | 11727 (+1.9%) | 5961 (−2.2%) | – / 1.13 | **0.02** / 0.27 |
| oltp_read_write | p2par | 57 (−5.3%) | 17.91 | 24.45 | 32.36 | 10891 (−5.7%) | 11450 (−0.5%) | 6132 (+0.6%) | – / **0.97** | 0.02 / 0.41 |
| tx1shard | base | 393 | 2.55 | 3.40 | 5.15 | 1533 | 1411 | 884 | 0.60 / – | 0.14 / 0.30 |
| tx1shard | p2defer | 401 (+1.8%) | 2.51 (−1.3%) | 3.49 | 5.46 | 1493 (−2.6%) | 1414 (+0.2%) | 863 (−2.4%) | 0.61 / – | **0.02** / 0.31 |
| oltp_update_index | base | 810 | 1.24 | 1.83 | 3.30 | 554 | 628 | 564 | – | – |
| oltp_update_index | p2 | 995 (+22.9%) | 1.01 (−18.8%) | 1.38 (−24.8%) | 2.33 (−29.5%) | 526 (−5.1%) | 521 (−17.0%) | 451 (−20.1%) | – | – |

Parallel commit: the vtgate Multi commit drops 1.20 → 0.98 ms, but the tablet COMMIT time rises 0.29 → 0.42 ms
because both tablets and both mysqlds now wake at the same moment on 4 oversubscribed vCPUs. On hosts with dedicated
cores or remote tablets the expected gain is the full extra commit RTT (≈ 0.6 ms at 1 thread here, ≈ 12% of a
write_only transaction).

### A/B 3b: transaction modes (3 rounds, load 11–16). `p2multi` = patched binary, MULTI; `twopc` = same binary, `--transaction-mode=TWOPC` (tablets ignore the semi-sync requirement via a benchmark-only env var)

| workload | thr | config | TPS | avg ms | p95 ms | vtgate µs/tx | tablets µs/tx | mysqld µs/tx | MySQL stmts/tx | commit ms (Single / Multi / TwoPC) |
|---|---|---|---|---|---|---|---|---|---|---|
| tx1shard | 8 | base (MULTI) | 1129 | 8.02 | 15.26 | 651 | 740 | 670 | 4.00 | 2.15 / – / – |
| tx1shard | 8 | single | 1096 (−3%) | 7.92 | 15.52 | 643 (−1.2%) | 725 (−2.1%) | 670 (0%) | 4.00 | 2.05 / – / – |
| tx1shard | 8 | twopc | 1317 (+17%) | 6.36 | 11.09 | 614 (−5.7%) | 744 (+0.6%) | 652 (−2.6%) | 4.00 | 1.63 / – / – |
| tx1shard | 1 | base | 368 | 2.73 | 3.86 | 1423 | 1333 | 897 | 4.00 | 0.68 / – / – |
| tx1shard | 1 | single | 363 (−1.3%) | 2.77 | 4.26 | 1364 (−4.1%) | 1283 (−3.7%) | 886 (−1.2%) | 4.00 | 0.73 / – / – |
| oltp_write_only | 8 | p2multi | 651 | 13.20 | 22.47 | 1130 | 1428 | 1291 | 7.50 | 1.80 / 3.74 / – |
| oltp_write_only | 8 | twopc | 373 (−43%) | 23.44 (+78%) | 39.71 | 1790 (+58%) | 2648 (+85%) | 2865 (+122%) | 18.74 | 1.89 / – / 16.97 |
| oltp_write_only | 1 | p2multi | 192 | 5.27 | 8.07 | 2513 | 2612 | 1768 | 7.51 | 0.76 / 1.51 / – |
| oltp_write_only | 1 | twopc | 128 (−33%) | 7.88 (+50%) | 11.49 | 3599 (+43%) | 4424 (+69%) | 3463 (+96%) | 18.68 | 0.61 / – / 5.24 |

(SINGLE rejects the 2-shard write_only transactions. Single-shard transactions cost the same in all modes.)

### A/B 3a/3c: session settings, FOR UPDATE (base binaries, 3 rounds, load 13–21)

| workload (ps, 8 thr) | TPS | avg ms | vtgate µs/ev | tablets µs/ev | mysqld µs/ev |
|---|---|---|---|---|---|
| write_only, no SET | 549 | 15.10 | 1160 | 1405 | 1324 |
| write_only, `SET innodb_lock_wait_timeout=25` (settings pool) | 655 | 12.93 | 1165 (+0.4%) | 1396 (−0.6%) | 1307 (−1.3%) |
| write_only, `SET sql_mode='STRICT_TRANS_TABLES'` (SET_VAR hint) | 419 | 19.07 | 1211 (+4.4%) | 1440 (+2.5%) | 1437 (+8.5%) |
| point_select, no SET | 3808 | 2.19 | 176 | 208 | 138 |
| point_select, settings pool | 4491 | 1.93 | 184 (+4.5%) | 211 (+1.4%) | 146 (+5.8%) |
| tx1shard (A/B 3a, base) | 1120 | 7.43 | 635 | 732 | 695 |
| tx1shard FOR UPDATE (A/B 3a, base) | 1106 | 7.38 | 636 (+0.2%) | 737 (+0.7%) | 705 (+1.4%) |

### A/B 3d: sequence-backed inserts, vtgate block cache prototype (`p2seq` = `P2_SEQ_BLOCK=1000`), text protocol, 3 rounds, load 16–19

| workload | thr | config | TPS | avg ms | p95 ms | p99 ms | vtgate µs/ins | tablets µs/ins (incl. seq tablet) | mysqld µs/ins |
|---|---|---|---|---|---|---|---|---|---|
| insert, id from sequence | 8 | base | 1964 | 4.36 | 8.83 | 13.87 | 344 | 409 | 316 |
| insert, id from sequence | 8 | p2seq | 2674 (+36%) | 3.19 (−27%) | 6.94 (−21%) | 11.62 (−16%) | 250 (−27%) | 268 (−34%) | 298 (−6%) |
| insert, explicit id (1 round) | 8 | base | 1910 | 4.18 | 9.73 | 17.01 | 259 | 270 | 291 |
| insert, id from sequence | 1 | base | 579 | 1.73 | 3.27 | 7.49 | 791 | 790 | 473 |
| insert, id from sequence | 1 | p2seq | 764 (+32%) | 1.30 (−25%) | 2.34 (−29%) | 5.39 (−28%) | 548 (−31%) | 496 (−37%) | 436 (−8%) |
| insert, explicit id (1 round) | 1 | p2seq | 747 | 1.33 | 2.39 | 5.88 | 570 | 509 | 451 |

(Several explicit-id runs failed on duplicate keys from `sysbench.rand.unique()` collisions across runs; the
surviving runs are consistent with A/B 3a's explicit-id rows: vtgate 238–259, tablets 248–270 µs.)

## Code changes

### In `P2-writes-tx.patch` (recommended)

1. **PK-pinned DML without row limit** — `go/vt/vttablet/tabletserver/planbuilder/builder.go`:
   `matchesAtMostOneRow(where, table)`: the top-level AND of the WHERE clause has `pk_col = <literal | :bindvar>`
   (either side) for every primary-key column, and every PK column is an integer type. Then `analyzeUpdate` /
   `analyzeDelete` return `PlanUpdate`/`PlanDelete` (no `limit :#maxLimit`), which run as one autocommit statement
   (`execAutocommit`) instead of `execAsTransaction`. Why integers only: a string PK compared with a number is compared
   as a double (`'1'`, `'01'`, `'1.0'` all match), and bind-variable types are only known at execution time.
   Tests: `TestDMLOnFullPrimaryKeySkipsRowLimit` (new; fails on main), fixture updates in tabletserver tests,
   `go/vt/vttablet/endtoend/queries_test.go`, `go/test/endtoend/transaction/twopc/twopc_test.go` (redo-log SQL loses
   ` limit 10001` for PK DMLs), vtexplain golden files.
   Gotchas: (a) query-stats labels change from `table.UpdateLimit`/`DeleteLimit` to `Update`/`Delete` for these
   statements (dashboards keyed on plan type); (b) the rewritten SQL in query logs and 2PC redo logs no longer has
   `limit 10001`; (c) with `--queryserver-config-max-result-size=0` a PK update used to fail the row check and now
   succeeds; (d) plan cache is already invalidated on schema reload, so a PK change is picked up. The endtoend suites
   (need MySQL) were not run here; their fixtures were updated by pattern and need a CI run.
2. **Deferred BEGIN** — new flag `--queryserver-config-defer-begin` (default false; `tabletenv.TabletConfig.DeferBegin`):
   - `go/mysql/query.go`: `Conn.ExecuteFetchWithPrefix(prefix, query, …)` sends `prefix;query` as one COM_QUERY and
     returns the result of `query`; errors are annotated with the statement that failed.
   - `connpool/dbconn.go`: `ExecWithPrefix` (same reconnect-and-retry-once semantics as `Exec`, i.e. as the old
     standalone `begin`; kills the connection on context expiry like `ExecOnce`); `execOnce` refactored into
     `execOnceWith`.
   - `tx_pool.go`: for non-reserved connections with DEFAULT/isolation-level begins, `createTransaction` stores the begin
     statement in `StatefulConnection.pendingBegin` instead of executing it. `Commit`/`Rollback` of a transaction that
     never executed a statement send nothing.
   - `stateful_connection.go`: `Exec` sends the pending begin with the first statement; `FlushPendingBegin` runs it alone
     before `ApplySetting` and before streaming/CALL on the raw connection (`query_executor.go`).
   - Not deferred: reserved connections (a BEGIN releases LOCK TABLES), consistent-snapshot transactions (need the
     begin's session-state GTID), ReserveBegin, the DBA connection used for 2PC redo.
   - Tests: `TestDBConnExecWithPrefix` (one round trip; query error annotated; failed prefix does not run the query),
     `TestTxPoolDeferBegin`. Flag listed in `go/flags/endtoend/{vttablet,vtcombo}.txt`.
   - Gotchas: the tablet's `Queries.BEGIN` timing now measures ~0 (the begin time moves into the first statement); the
     MySQL general/slow log shows `begin;` and the statement as a multi-statement packet; a failed `begin` now surfaces
     as the first statement's error.
3. Harness: `perf-findings/harness/P2-*` (cluster with `addseq`, bench with commit/begin latency, A/B driver with
   per-run env vars, lua workloads, binlog purge).

### In `P2-prototypes.patch` (measurement only, env-var gated, not for merge as is)

- `go/vt/vtgate/tx_conn.go`: `P2_PARALLEL_COMMIT=1` commits all MULTI shard sessions concurrently.
- `go/vt/vtgate/engine/insert_common.go`: `P2_SEQ_BLOCK=N` — vtgate reserves N sequence values per round trip and hands
  them out locally (per sequence, one mutex).
- `go/vt/vttablet/tabletserver/tabletserver.go`: `P2_TWOPC_IGNORE_SEMISYNC=1` lets TWOPC run without semi-sync
  replicas (benchmark hack).

Tests run: `go test ./go/vt/vttablet/tabletserver/...` (all pass except `vstreamer`, which needs a local MySQL and
fails the same way on main), `./go/vt/vtexplain/`, `./go/vt/vtgate/`, `./go/vt/vtgate/engine/`, `./go/mysql/`.

## Recommendations, ranked by user-visible impact

1. **Merge the PK-DML planning change** (S, low risk): every autocommit `UPDATE/DELETE … WHERE pk = ?` — the most common
   OLTP write — drops from 3 MySQL round trips to 1: tablet −14..−17%, mysqld −16..−20%, vtgate −4..−5% CPU per
   statement; −19% average and −25..−30% p95/p99 latency at low concurrency. Could be extended to unique keys and
   small `IN` lists (≤ max-result-size values) later.
2. **Sequence value caching in vtgate** (M): −27% vtgate, −34% tablet CPU and −25% latency per sequence-backed insert,
   i.e. parity with client-supplied ids. Needs an opt-in per-sequence setting (e.g. vschema `"cache": N` on the
   `auto_increment` spec) because IDs are no longer allocated in time order across vtgates, and a way to drop vtgate
   caches when a sequence is reset (MoveTables `ResetSequences`/`InitializeTargetSequences`).
3. **Keep MULTI as the default; document the costs**: single-shard transactions cost the same in SINGLE, MULTI and TWOPC;
   2-shard MULTI transactions pay one extra commit RTT per additional shard (sequential commit); TWOPC costs ~2x CPU and
   3.5x commit latency for 2-shard transactions.
4. **Deferred BEGIN** (M, opt-in flag): removes one MySQL round trip per shard-transaction (tablet BEGIN 0.14 → 0.02 ms).
   Measured end-to-end effect is small here (−2..−5% mysqld CPU, −1..−4% latency, mostly within noise); likely more
   useful when MySQL is reached over TCP. Low risk behind the flag; consider default-on after a release.
5. **Parallel MULTI commit** (S code, but a semantic change): only as an explicit opt-in. It removes (N−1) commit RTTs for
   N-shard transactions but breaks the "first shard failed → nothing committed" ordering and lets writers commit even
   when an earlier `SELECT … FOR UPDATE` shard lost its locks. No gain was measurable on this box.

## Follow-ups

- Extend finding 1 to unique secondary keys and to `IN (…)` lists with fewer values than the row limit; and to
  multi-shard DMLs that vtgate runs in an implicit transaction (BeginExecute + Commit per shard even when each shard
  statement affects ≤ 1 row).
- Fix the quoted-integer SET quirk (validate the value on the target at SET time).
- vtgate still sends a separate Commit RPC per shard; a "commit with last statement" RPC is impossible without knowing
  the last statement, but `BeginExecute`+`Commit` for single-statement implicit transactions could be one RPC
  (`ExecuteAutocommit`-style) for multi-shard DML with `autocommit`.
- Re-measure deferred BEGIN and parallel commit on a host with dedicated cores / TCP between tablet and MySQL.
- gRPC Session payload for gRPC-API clients with many shard sessions (12 KB at 256 shards per call, both ways).
