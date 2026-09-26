# H1-hop-rootcause: where the per-hop cost of a point select goes, and what moves it

Investigator H1, BASE 30000, base commit aa9ccf9, Go 1.27.1, 4 vCPU KVM guest (Firecracker, `pv_native_safe_halt`
idle, no cpuidle driver, 1 thread per core as seen by the guest). Workload: sysbench `oltp_point_select`, text
protocol, 2 shards, 4 × 100k rows, 1 / 8 / 32 client threads. Patch: `findings/H1-hop-rootcause.patch` (harness,
floor programs, trace analyzer, Go runtime experiment diff; **no Vitess production code change**). Every measurement
ran under `flock -o /home/vt/perf/bench.lock`; load average is in every table row.

## TL;DR

At 1 client thread a point select through vtgate costs ~0.80 ms and ~1.25 ms of CPU summed over all processes
(vtgate ~565 µs, the serving vttablet ~495 µs, mysqld ~180 µs). Direct to mysqld it is 0.07 ms and ~50 µs. The cost is
**not** Vitess query logic (~110–150 µs of CPU in total). It is the number of **thread wake-ups per query** multiplied
by what a wake-up costs on this VM:

- A wake-up of a thread on another, halted vCPU costs ≈ 20 µs of CPU and ≈ 28 µs of latency here (C ping-pong), versus
  ≈ 1 µs on the same CPU. Direct MySQL needs ~1.5 inter-processor interrupts (IPIs) per query; Vitess needs **26 per query**.
- Where the 26 come from: 6 are the unavoidable process-to-process messages (client → vtgate → vttablet → mysqld and
  back). The rest are Go-internal:
  - gRPC's reader and writer goroutines: ~7.5 goroutine hand-offs per query in each Go process.
  - Idle Ps: ~4 "futile" spinning-thread wake-ups per query per process, and ~5 goroutines per query resumed on a
    different OS thread.
  - gRPC BDP flow-control pings: 2 extra frames per RPC in each direction.
- The same halting also explains the mysqld "2–3x CPU" when driven by vttablet. mysqld's CPU per query depends only
  on the idle gap between queries: 45 µs at 15k qps, 150 µs at 1.2k qps, 175 µs at 600 qps. That is direct sysbench
  with the rate limited and the identical statement text. With the vCPUs kept out of HLT it drops back to 51 µs.
- **The vtgate→vttablet hop (gRPC plus a second Go process) is ~70% of the CPU and ~55% of the latency at 1 thread,
  and ~60% of the CPU at 8 threads.** vtcombo runs the same Vitess code without that hop. It does 0.30 ms / 357 µs CPU
  at 1 thread and 13.3k QPS at 8 threads, against 0.78 ms / 1.25 ms and 5.1k QPS for vtgate+vttablet.

Measured fixes (A/B, 2–3 alternating rounds, 10 s each):

| fix | kind | 1 thread latency / CPU (vtgate, tablet) | 8 threads QPS / CPU | 32 threads QPS / CPU |
|---|---|---|---|---|
| static gRPC windows (turns off BDP pings) | flags | −1..−7% / −17%, −27% | +2..+11% / −1..−5%, −3..−8% | +2% / ≈0 |
| + GOMAXPROCS=1 (vtgate, vttablet) | env | **−32% (0.80→0.54 ms)** / **−54%, −55%** | +8..+13% / −38%, −29% | −3% / −35%, −19% |
| + GOMAXPROCS=2 | env | −3..−9% / −20%, −31% | +6..+11% / −20%, −11% | −1..−4% / −15%, −2% |
| + GOMAXPROCS=2 + Go runtime "no wakep on hand-off" (prototype) | Go runtime | −16% / −36%, −42% | **+11..+14%** / −23%, −16% | ≈0 / −15%, −4% |
| window + GOMAXPROCS=1 + P1's stream-worker patch (vs base) | env + P1 | **−33% (0.52 ms)** / −54%, −61% | **+18%** / −42%, −39% | **+9%** / −40%, −34% |
| in-process tablets (vtcombo, upper bound) | design (L) | −62% (0.30 ms) / 357 µs total | **+160% (13.3k)** | +89% (15.5k) |

## 1. Per-query accounting at 1 client thread (base binaries, defaults)

### 1a. Counts per query (`bench.py perf=1`: `perf stat` tracepoints per process; `traceana` on 1 s Go execution traces)

| per query | vtgate | vttablet (serving) | mysqld | system |
|---|---|---|---|---|
| CPU (µs, /proc) | 560–600 (u 355 / s 240) | 490–525 (u 280 / s 240) | 180 | ~1.3 ms total |
| syscalls (raw_syscalls:sys_enter) | 45.8 | 46.1 | 5.6 | |
| … of which (perf trace -s) | futex 9.1, nanosleep 6.9 (6.0 = sysmon), epoll_pwait 6.1, read 6.0 (2.6 EAGAIN), write 3.4 | futex 11.6, epoll 7.9, nanosleep 7.5, read 6.6 (3.0 EAGAIN), write 3.7 | recvfrom 2.5, futex 1.0, sendto 0.8, ppoll 0.8 | |
| voluntary / involuntary context switches | 18.6 / 0.7 | 18.3 / 0.7 | 1.3 / 0.2 | |
| sched_switch / sched_waking issued | 19.4 / 13.4 | 19.0 / 14.1 | 1.5 / 1.9 | |
| goroutine unblocks | 7.7 | 7.4 | | |
| goroutine starts (on a different OS thread than last time) | 11.9 (4.9) | 10.1 (4.6) | | |
| P wake-ups (ProcStart) / futile (P started and stopped without running a goroutine) | 7.9 / 3.9 | 8.8 / 4.4 | | |
| IPIs (reschedule + call-function-single, system-wide) | | | | **26.4** (direct MySQL: 1.5) |

Hand-offs per query (from the traces; `waker -> wakee`):
- vtgate: netpoll → gRPC reader 2.85; reader → loopy writer 1.7 (ping acks, window updates); reader → MySQL conn
  goroutine 1.05; conn goroutine → loopy writer 0.93 (the request); conn goroutine → `processQueryInfo` 1.0 (removed by
  P1's patch); netpoll → conn goroutine 0.54.
- vttablet: netpoll → gRPC reader 2.9; reader → loopy writer 1.9; reader → stream worker 1.07; worker → loopy writer
  1.04; netpoll → worker (MySQL reply) 0.93; smartconnpool expire ticker 0.42 (idle cost, P6's patch).
- The loopy writer calls `runtime.Gosched()` before every small flush (grpc-go `minBatchSize`), which adds another
  scheduler round.
- One vtgate query in time order: query arrives, then ~150 µs of vtgate work (traced), then the request is written.
  About 200 µs later the reader wakes for the tablet's BDP ping and the loopy writer answers it. About 300 µs later
  the response arrives. The reader wakes the conn goroutine and the loopy writer (client BDP ping), then the result is
  written to the client. Loopback `write(2)` calls take 10–150 µs because they include waking the peer on another vCPU.

The vtgate MySQL server writes each result set with exactly one `write(2)` (one flush). The vttablet MySQL client
does one write and one read (plus one EAGAIN) per query. There is nothing left to batch on the MySQL sides.

### 1b. Where the CPU goes, by mechanism (system-wide `perf record -g`, cpu-clock 1999 Hz, samples classified by call chain)

`perf` works here with software events only (no PMU in the guest); tracepoints and kernel symbols are available.
Samples are attributed to the syscall at the user/kernel boundary (futex, epoll, nanosleep, socket read/write). User
samples are attributed to the leaf-most non-runtime frame, or to the Go scheduler, malloc or GC
(`harness/H1-scripts/classify.py`, `groups.py`).

| µs/query (perf-sampled) | scheduler: futex + Go sched + epoll + nanosleep | socket syscalls | gRPC + proto (user) | malloc + GC | Vitess logic (parser, planner, executor, tablet path, MySQL protocol) | other | total |
|---|---|---|---|---|---|---|---|
| vtgate, base | **163** (32%) | 98 (19%) | 70 (14%) | 50 (10%) | 105 (21%) | 26 | 512 |
| vttablet, base | **179** (40%) | 91 (20%) | 73 (16%) | 36 (8%) | 50 (11%) | 20 | 449 |
| mysqld, base | 3 | 28 | – | – | – | 158 (mysqld) | 189 |
| vtgate, window + GOMAXPROCS=1 | 43 | 51 | 31 | 27 | 70 | 16 | 238 |
| vttablet, window + GOMAXPROCS=1 | 44 | 43 | 26 | 20 | 27 | 35 (copystack, see §5) | 195 |
| vtcombo (vtgate + tablets, one process) | 56 | 49 | 2 | 23 | 82 | 17 | 230 |

Kernel detail (vtgate, base):
- Of the 67 µs/q in `futex`, 47 µs (70%) is `try_to_wake_up`. That is the IPI to a halted vCPU, whose cost lands where
  interrupts are re-enabled.
- Of the 68 µs/q in `write(2)`, 27.5 µs is waking the receiving thread (`__wake_up` → `ttwu`). The rest is TCP
  transmit plus the loopback receive softirq, which runs in the sender's context.

### 1c. The latency (0.80 ms), additively, from the floor programs and vtcombo (§2)

| step | adds | how measured |
|---|---|---|
| mysqld + client + 2 wake-ups | 0.07 ms | direct sysbench |
| a Go MySQL-protocol hop (go/mysql server + client, no logic) | +0.06–0.08 ms | `proxy` floor − direct |
| Vitess logic in-process (parse, normalize, plan, route, tablet execute path) | +0.17–0.22 ms | vtcombo − `proxy` |
| **vtgate → vttablet hop** (gRPC unary + second Go runtime) | **+0.42–0.48 ms** | Vitess − vtcombo (bare gRPC floor: +0.31) |
| … of which spare-P scheduling (futile wake-ups, goroutines resumed on other threads) | ≈ −0.21 ms with GOMAXPROCS=1 | A/B 2 |
| … of which BDP ping frames | ≈ −0.01..−0.06 ms (mostly off the critical path; CPU −250 µs/q) | A/B 2/3 |

## 2. The floor for this machine

Same box, same libraries as Vitess (go/mysql, grpc-go 1.83.2 with Vitess's vtprotobuf codec, `queryservice.Execute`
as the RPC). Program: `perf-findings/harness/H1-floor` (modes `proxy`, `grpcsrv`, `grpcproxy`, `tcpsrv`/`tcpclient`),
C wake-up benchmark `harness/H1-c/pingpong.c`. The floor backends query mysqld_100 over its unix socket, like vttablet.
Direct and floor numbers use one shard, so half the ids miss; that doesn't change the per-query cost.

| path (1 hop = 1 process boundary) | 1 thread: latency | 1 thread: CPU µs/q (per process) | 8 threads: QPS | 8 threads: CPU µs/q |
|---|---|---|---|---|
| (d) sysbench → mysqld (C/C++) | 0.07 ms | mysqld 49 | 121,000 | mysqld 23 |
| (a) sysbench → Go MySQL proxy → mysqld | 0.13–0.16 ms | proxy 80–101, mysqld 55–72 | 27,200 | proxy 41, mysqld 52 |
| (b) sysbench → Go MySQL proxy → gRPC unary → Go server → mysqld | 0.44–0.49 ms | 306 + 298, mysqld 134 | 8,340 | 103 + 104, mysqld 102 |
| (b) with a canned result (no mysqld) | 0.24 ms | 243 + 185 | 17,600 | 81 + 48 |
| (b) + static window + GOMAXPROCS=1 | 0.32 ms | 136 + 135, mysqld 104 | 10,200 | 58 + 59, mysqld 83 |
| vtcombo: full Vitess logic, tablets in-process | 0.30–0.38 ms | 262–320, mysqld 95–124 | 12,100–13,300 | 143–148, mysqld 74–82 |
| vtcombo, GOMAXPROCS=1 | 0.21 ms | 154, mysqld 71 | 12,200 | 82, mysqld 49 |
| **vtgate → vttablet → mysqld (Vitess)** | **0.78–0.82 ms** | **560 + 495, mysqld 180** | **4,700–5,100** | **217 + 221, mysqld 145** |
| Vitess, static window + GOMAXPROCS=1 | 0.53–0.55 ms | 262 + 222, mysqld 145 | 5,300–5,500 | 133 + 159, mysqld 126 |

(c) Wake-up cost on this VM (C, two processes ping-pong 1 byte, `pingpong.c`):

| | round trip | CPU per round trip | per wake-up |
|---|---|---|---|
| pipe, both on CPU 0 | 1.9 µs | 1.9 µs | ~1 µs |
| pipe, CPU 0 ↔ 1, back to back | 22.6 µs | 18.4 µs | ~9 µs CPU, ~11 µs latency |
| pipe, CPU 0 ↔ 1, 500 µs idle gap before each ping | 56.6 µs | 39.8 µs | **~20 µs CPU, ~28 µs latency** |
| loopback TCP, CPU 0 ↔ 1, back to back / 500 µs gap | 28.9 / 77.5 µs | 26.8 / 60.6 µs | TCP adds ~3–10 µs per message |
| unix socketpair, CPU 0 ↔ 1, back to back / gap | 22.7 / 69.2 µs | 22.3 / 58.0 µs | |

Reading:
- Go networking itself is cheap. The Go MySQL hop adds ~0.07 ms and 41 µs/q at 8 threads.
- **A gRPC unary hop adds ~0.3 ms and ~500 µs of CPU at 1 thread, and 166 µs/q at 8 threads.** That is 3x the cost of
  the MySQL hop on the same box. The reason is that each RPC is spread over 4 goroutines per side: caller, loopy
  writer, reader, and stream worker.
- The Vitess logic on top of that is ~100–150 µs of CPU.

## 3. Why mysqld costs 2–3x when driven by vttablet

| test | mysqld µs/query |
|---|---|
| direct, sysbench statement, back to back (13–15k qps) | 45–51 |
| direct, **vttablet's exact text** `select c from sbtest1 where id = N limit 10001 /* INT64 */` (custom Lua) | 51 (same) |
| direct, rate-limited to 5,000 / 2,000 / 1,200 / 600 qps | 79 / 120 / **150** / 175 |
| direct, vttablet text, 1,200 qps | 140 |
| direct, 1,200 qps, SCHED_IDLE spinners on every vCPU (vCPUs never halt) | **51** (without spinners, same period: 129) |
| through vttablet (Vitess, ~1,250 qps) | 177–185 |
| through vttablet, mysqld pinned to its own CPU (`taskset`, Go on CPUs 0–2) | 189 (no change) |

- **Statement text and session settings: no effect.** vttablet sends exactly one statement per query (general log).
- **Hyperthread siblings: can't be the cause as seen from the guest.** 1 thread per core, and pinning mysqld away from
  the Go processes changes nothing.
- **Cause: the idle gap.** Between two queries mysqld's vCPU halts. Each query then starts cold: caches, TLB and
  branch predictors were possibly taken by the host, plus the halt-exit path. User time goes 31 → 112 µs per query
  from back-to-back to 1.2k qps. At the same arrival rate direct gets 150 µs vs 180 µs via vttablet; the rest is
  pollution by the Go processes sharing the CPUs.
- The same effect inflates the Go processes at 1 thread; the sqlparser costs 37 µs/q at 1.2k qps vs 24 µs/q in vtcombo
  at 3k qps. Consequence: 1-thread CPU/query numbers on this VM overstate the steady-state cost of Vitess logic. At
  8 threads mysqld is 145 µs and at 32 threads 113 µs.

## 4. Levers tested

### A/B 2: gRPC windows and GOMAXPROCS (base binaries, 3 rounds, 10 s each, restart per config)

`window` = `--grpc-initial-window-size=4194304 --grpc-initial-conn-window-size=8388608` (vtgate) and
`--grpc-server-initial-window-size=4194304 --grpc-server-initial-conn-window-size=8388608` (vttablet). Setting any of
them disables grpc-go's BDP estimator.

| config | thr | n | QPS | avg ms | p99 ms | vtgate µs/q | tablets µs/q | mysqld µs/q | load |
|---|---|---|---|---|---|---|---|---|---|
| base | 1 | 3 | 1219 | 0.82 | 1.73 | 568 [565–573] | 503 [498–510] | 182 [181–184] | 7.2 |
| window | 1 | 3 | 1308 (+7%) | 0.76 (-7%) | 1.22 (-30%) | 463 (-18%) [454–472] | 361 (-28%) [357–365] | 174 (-4%) | 6.2 |
| procs1 | 1 | 3 | 1632 (+34%) | 0.61 (-26%) | 1.24 (-29%) | 371 (-35%) [360–386] | 358 (-29%) [348–372] | 151 (-17%) | 6.4 |
| winprocs1 | 1 | 3 | 1812 (+49%) | 0.55 (-33%) | 1.15 (-34%) | 265 (-53%) [257–271] | 229 (-54%) [222–234] | 145 (-21%) | 5.6 |
| winprocs2 | 1 | 3 | 1335 (+10%) | 0.75 (-9%) | 1.33 (-23%) | 440 (-23%) [439–440] | 331 (-34%) [329–333] | 170 (-7%) | 4.1 |
| procs2 | 1 | 3 | 1304 (+7%) | 0.76 (-7%) | 1.36 (-22%) | 529 (-7%) [519–539] | 460 (-9%) [450–471] | 182 (+0%) | 4.7 |
| base | 8 | 3 | 4669 | 1.72 | 5.49 | 217 [213–219] | 225 [217–234] | 146 | 8.2 |
| window | 8 | 3 | 5188 (+11%) | 1.54 (-10%) | 4.28 (-22%) | 207 (-5%) [203–211] | 207 (-8%) [206–207] | 142 (-3%) | 6.6 |
| procs1 | 8 | 3 | 5109 (+9%) | 1.56 (-9%) | 4.33 (-21%) | 140 (-36%) [136–143] | 169 (-25%) [164–173] | 129 (-12%) | 6.4 |
| winprocs1 | 8 | 3 | 5288 (+13%) | 1.51 (-12%) | 4.26 (-22%) | 135 (-38%) [132–139] | 161 (-29%) [158–166] | 128 (-12%) | 5.4 |
| winprocs2 | 8 | 3 | 5198 (+11%) | 1.54 (-11%) | 4.16 (-24%) | 174 (-20%) [172–176] | 198 (-12%) [196–201] | 139 (-5%) | 5.2 |
| procs2 | 8 | 3 | 4918 (+5%) | 1.62 (-6%) | 4.36 (-21%) | 185 (-15%) [180–190] | 212 (-6%) [206–219] | 142 (-3%) | 5.4 |

(The base rounds ran at a higher load average than the others, so the latency/QPS gains at 8 threads are partly load;
the CPU columns are robust, ±2%.)

### A/B 3: + Go runtime prototype, 32 threads (2 rounds)

`nowakewin` = window + patched runtime with `GODEBUG=nowakepnext=1`; `nowakewin2` = the same + GOMAXPROCS=2.

| config | thr | n | QPS | avg ms | p99 ms | vtgate µs/q | tablets µs/q | mysqld µs/q | load |
|---|---|---|---|---|---|---|---|---|---|
| base | 1 | 2 | 1286 | 0.78 | 1.33 | 560 [550–570] | 490 [483–496] | 179 | 1.5 |
| window | 1 | 2 | 1304 (+1%) | 0.77 (-1%) | 1.21 (-9%) | 464 (-17%) | 362 (-26%) | 174 (-3%) | 3.1 |
| winprocs1 | 1 | 2 | 1880 (+46%) | 0.53 (-32%) | 1.13 (-15%) | 260 (-54%) | 218 (-56%) | 144 (-19%) | 2.9 |
| winprocs2 | 1 | 2 | 1323 (+3%) | 0.76 (-3%) | 1.23 (-8%) | 448 (-20%) | 336 (-31%) | 172 (-4%) | 1.9 |
| nowakewin | 1 | 2 | 1482 (+15%) | 0.68 (-13%) | 1.09 (-18%) | 406 (-28%) | 305 (-38%) | 157 (-12%) | 2.4 |
| nowakewin2 | 1 | 2 | 1547 (+20%) | 0.65 (-16%) | 1.11 (-17%) | 360 (-36%) | 286 (-42%) | 158 (-12%) | 3.0 |
| base | 8 | 2 | 5060 | 1.58 | 4.33 | 217 | 221 | 144 | 3.4 |
| window | 8 | 2 | 5180 (+2%) | 1.54 (-2%) | 4.10 (-5%) | 214 (-1%) | 214 (-3%) | 145 (+0%) | 4.2 |
| winprocs1 | 8 | 2 | 5478 (+8%) | 1.46 (-8%) | 3.86 (-11%) | 132 (-39%) | 158 (-29%) | 126 (-12%) | 3.2 |
| winprocs2 | 8 | 2 | 5384 (+6%) | 1.48 (-6%) | 4.03 (-7%) | 172 (-21%) | 196 (-11%) | 138 (-5%) | 3.4 |
| nowakewin | 8 | 2 | 5400 (+7%) | 1.48 (-6%) | 3.96 (-9%) | 204 (-6%) | 196 (-11%) | 141 (-2%) | 3.6 |
| nowakewin2 | 8 | 2 | 5600 (+11%) | 1.42 (-10%) | 3.69 (-15%) | 168 (-23%) | 186 (-16%) | 134 (-7%) | 3.6 |
| base | 32 | 2 | 8173 | 3.92 | 10.38 | 140 | 138 | 113 | 6.1 |
| window | 32 | 2 | 8307 (+2%) | 3.85 (-2%) | 9.82 (-5%) | 142 (+2%) | 138 (-1%) | 114 (+0%) | 6.3 |
| winprocs1 | 32 | 2 | 7904 (-3%) | 4.04 (+3%) | 9.31 (-10%) | 90 (-35%) | 112 (-19%) | 106 (-6%) | 3.7 |
| winprocs2 | 32 | 2 | 7867 (-4%) | 4.06 (+4%) | 10.09 (-3%) | 118 (-15%) | 136 (-2%) | 116 (+3%) | 4.9 |
| nowakewin | 32 | 2 | 8322 (+2%) | 3.84 (-2%) | 10.00 (-4%) | 140 (+0%) | 134 (-4%) | 114 (+1%) | 6.4 |
| nowakewin2 | 32 | 2 | 8120 (-1%) | 3.94 (+1%) | 9.56 (-8%) | 118 (-15%) | 132 (-4%) | 114 (+1%) | 4.3 |

### A/B 1: runtime knobs alone (2 rounds)

| config | thr | QPS | avg ms | vtgate µs/q | tablets µs/q | mysqld µs/q | IPIs/q |
|---|---|---|---|---|---|---|---|
| base | 1 | 1243 | 0.81 | 594 | 526 | 183 | 26.6 |
| rt (patched runtime, knobs off) | 1 | 1236 (−1%) | 0.81 | 608 (+2%) | 528 (0%) | 185 | 26.9 |
| `nowakepnext=1` | 1 | 1348 (+8%) | 0.74 (−8%) | 572 (−4%) | 466 (−11%) | 171 (−7%) | 23.7 |
| `sysmonmin=1000` (sysmon min sleep 1 ms instead of 20 µs) | 1 | 1229 (−1%) | 0.81 | 598 (+1%) | 528 (0%) | 186 | 25.7 |
| window | 1 | 1231 (−1%) | 0.81 | 501 (−16%) | 392 (−26%) | 182 | 21.7 |
| base | 8 | 4816 | 1.66 | 221 | 231 | 146 | 2.5 |
| `nowakepnext=1` | 8 | 4518 (−6%) | 1.77 | 224 | 228 | 150 | 2.5 |
| `sysmonmin=1000` | 8 | 4815 | 1.66 | 218 | 224 | 150 | 2.5 |
| window | 8 | 4356 (−10%) | 1.86 | 216 | 224 | 148 | 2.8 |

The −10% for `window` at 8 threads came from one round at 3862 QPS (the other 4851). Over 12 rounds in A/B 1–3,
`window` at 8 threads ranged from −20% to +11% QPS with a median of +2%, and its CPU/query was always ≤ base. P1's
earlier −23% could not be reproduced consistently. I treat it as noise from neighbour load, not a regression, but a
dedicated check on a quiet machine would settle it.

### Counts per config (1 thread; `counts.sh`)

| per query | base | window | procs1 | window + procs1 |
|---|---|---|---|---|
| IPIs (system-wide) | 26.4 | 22.0 | 16.6 | 11.7 |
| vtgate / tablet syscalls | 45.8 / 46.1 | 29.3 / 29.1 | 27.4 / 30.5 | 16.1 / 18.2 |
| vtgate / tablet voluntary context switches | 18.6 / 18.3 | 13.5 / 12.6 | 9.3 / 11.4 | 6.4 / 7.4 |
| vtgate / tablet goroutine unblocks | 7.7 / 7.4 | 5.6 / 5.1 | 8.7 / 7.0 | 5.8 / 4.1 |
| vtgate / tablet P wake-ups (futile) | 7.9 (3.9) / 8.8 (4.4) | 5.6 (2.9) / 6.5 (3.6) | 2.6 (0) / 3.6 (0.1) | 1.9 (0) / 2.4 (0.4) |
| vtgate / tablet goroutines resumed on another thread | 4.9 / 4.6 | 3.8 / 2.9 | 0.1 / 0.1 | 0 / 0 |
| vtgate / tablet CPU µs | 609 / 572 | 504 / 396 | 402 / 390 | 291 / 257 |

- `window` removes ~2 goroutine hand-offs, ~17 syscalls and ~5 context switches per query in each process: the BDP
  ping, its ack, and the loopy-writer/reader wake-ups they cause.
- GOMAXPROCS=1 removes the futile P wake-ups and the cross-thread resumes. The goroutine hand-offs stay, but they
  become same-thread switches (~1 µs instead of ~20 µs).

### Placement and other negative results

| experiment | result |
|---|---|
| everything (vtgate, both tablets, both mysqld) on CPU 0 (`taskset`, window + GOMAXPROCS=1) | **worse**: 0.53 → 0.77 ms, vtgate 262 → 311 µs. Serializing four processes on one CPU costs more than cross-CPU wakes. |
| same with base config (GOMAXPROCS follows affinity since Go 1.25) | CPU 555/508 → 327/256 µs, latency 0.92 → 0.80 ms (load 16); cheaper but not faster |
| mysqld on its own CPU, Go processes on 0–2 | no change (189 µs) |
| sysmon minimum sleep 20 µs → 1 ms (patched runtime) | no change. sysmon's ~4000 nanosleeps/s show up as ~6 syscalls/q but cost ~14 µs/q |
| `LockOSThread` per MySQL connection (floor `-lockthread`) | worse: gRPC floor 0.44 → 0.50 ms, +20% CPU in the proxy |
| gRPC over a unix socket instead of loopback TCP (floor `-unix`) | no gain (0.49 → 0.52 ms, within noise). TCP adds only ~3 µs per message over a unix socket (C ping-pong) |
| SCHED_IDLE spinners keeping every vCPU busy | mysqld back to 51 µs/q, but Vitess latency got worse under the spinners (0.83 → 1.11 ms). An experiment only, not a fix |
| vttablet's exact statement text direct to mysqld | identical cost (51 vs 51 µs) |
| GOMAXPROCS=2 alone | 1 thread only −3..−7%; the second P still triggers a futile wake per hand-off |

## 5. Root causes, ranked by share of the gap

1. **The vtgate→vttablet process hop (gRPC unary + second Go runtime): ~70% of CPU, ~55% of latency at 1 thread, ~60%
   of CPU at 8 threads.** vtcombo vs vtgate+vttablet: 357 vs 1,250 µs and 0.30 vs 0.78 ms at 1 thread; 13.3k vs 5.1k
   QPS at 8 threads. A bare gRPC hop with no logic already costs 0.3 ms and ~500 µs at 1 thread (floor). Mechanism:
   per RPC and per side, 4 goroutines (caller, loopy writer, reader, stream worker) and ~7.5 hand-offs, each a
   potential cross-CPU wake. The fixes below shrink this; eliminating it is H2's work (bidi/raw streams), or an
   in-process path for co-located tablets.
2. **Cross-CPU wake-ups are expensive on this VM: ~20 µs CPU and ~28 µs latency each when the target vCPU halted.**
   26 IPIs per query vs 1.5 direct, so roughly 400–500 µs of the ~1.3 ms total CPU. It is also why mysqld costs
   150–180 µs instead of 45–50 µs at low rates (§3). This is a property of the platform, not of Vitess. Guest
   `cpuidle-haltpoll` (here built in but not enabled, `force=N`) or host halt-polling would reduce it; I couldn't
   change the kernel command line on this shared VM.
3. **Go scheduler with spare Ps: ~370 µs/q summed over processes at 1 thread** (GOMAXPROCS=1: vtgate −197, tablets
   −145, mysqld −31 µs). Every `goready` with an idle P calls `wakep()`: ~4 futile M wake-ups per query per process,
   plus ~5 goroutines per query resumed on another thread.
4. **gRPC BDP estimator pings: ~250 µs/q summed at 1 thread** (−105 vtgate, −142 tablets). ~17 syscalls and 2
   hand-offs per query per process. grpc-go sends a BDP PING on DATA arrival whenever none is outstanding. With
   ~300-byte messages the estimate never reaches its cap, so this never stops. It is mostly off the critical path
   (latency −1..−7%), and small at ≥ 8 threads (−1..−8%).
5. **Vitess logic proper: ~110–150 µs/q in total** (vtcombo profile: sqlparser 24, vtgate executor 22, other Vitess
   22, vttablet path 9, MySQL protocol 6, malloc + GC 23). Round 1, P1 and P6 already cover it; no big single item.

## 6. Recommendations (ranked by user-visible impact)

1. **Operators: disable gRPC BDP with static windows on vtgate and vttablet in LAN deployments** (S, config). −17/−27%
   CPU at 1 thread, never worse in CPU at 8/32 threads.
   - Trade-offs: a static window caps a single stream at window/RTT. 4 MiB at 50 ms is 80 MB/s, so size it for the
     worst cross-cell stream.
   - A slow consumer can make the receiver buffer up to the stream window per stream, bounded by the connection window.
2. **Operators: size GOMAXPROCS for vtgate and vttablet**, together with (1) (S, env/config; P1 recommended the same
   independently).
   - GOMAXPROCS=1 plus windows: −32% latency and −54% CPU at 1 thread, +8..+13% QPS at 8 threads, −3% QPS at 32 threads.
   - GOMAXPROCS=2 plus windows is the safer middle: −20..−31% CPU, +6..+11% QPS at 8 threads, −1..−4% at 32 threads.
   - In containers, a CPU limit makes Go pick this automatically. On VMs or bare metal shared with mysqld, set it
     explicitly.
   - With a small GOMAXPROCS, P1's `--grpc-server-num-stream-workers` fix matters more. With 1 P the only worker is
     pinned by StreamHealth, every Execute RPC spawns a goroutine (0.97 creates/q), and `copystack` costs ~20–24 µs/q
     in vttablet (A/B 4).
3. **Go upstream: don't `wakep()` in `ready()` for runnext hand-offs** (prototype; M for the proposal, S for the
   diff). −16% latency and −36/−42% CPU at 1 thread, +11..+14% QPS at 8 threads with GOMAXPROCS=2 plus windows,
   neutral at 32 threads.
   - Diff: `perf-findings/harness/H1-go-runtime-experiment.diff`. It adds `GODEBUG=nowakepnext=1` and
     `sysmonmin=<µs>`.
   - Risk: when the readying goroutine keeps running, the readied one waits until it blocks, a spinning M steals it
     (runnext steal after ~3 µs), or sysmon preempts it (10 ms).
   - This is a scheduler-policy change for the Go team, e.g. only skip the wake when the caller is about to park,
     which gRPC's loopy writer and the MySQL conn goroutine always do. It is not a Vitess change.
4. **grpc-go upstream: start a BDP measurement only for bulk data** (e.g. when a DATA frame, or the bytes received
   since the last sample, exceeds a threshold). That would give recommendation 1's savings without its trade-offs.
   Not prototyped.
5. **Design: collapse the hop for co-located tablets** (L). vtcombo already runs the tablet query service in-process
   (`internal` tablet connection, synchronous call) and shows the ceiling:
   - 1 thread: 0.30 ms (0.21 ms with GOMAXPROCS=1)
   - 8 threads: 13.3k QPS (vs 5.1k)
   - 32 threads: 15.5k QPS (vs 8.2k)

   H2's raw/bidi-stream work attacks the same hand-offs over the network. The floor (§2) says a well-built network hop
   can't get below ~0.07 ms and ~40 µs/q per side here, which is what the Go MySQL hop costs.

## 7. A/B 4: P1's patch on top of the recommended configs (2 rounds)

`p1…` = `/home/vt/bin-P1r1` (P1's point-read patch, including `--grpc-server-num-stream-workers` defaulting to
max(GOMAXPROCS, 64), plus round-1 F10/F11/F15/F19/F25). Percentages are vs `winprocs1`.

| config | thr | n | QPS | avg ms | p99 ms | vtgate µs/q | tablets µs/q | mysqld µs/q | load |
|---|---|---|---|---|---|---|---|---|---|
| winprocs1 | 1 | 2 | 1905 | 0.52 | 1.13 | 258 | 212 | 144 | 1.8 |
| p1winprocs1 | 1 | 2 | 1914 (+0%) | 0.52 (+0%) | 1.04 (-8%) | 260 (+1%) | 191 (-10%) | 151 (+4%) | 2.1 |
| winprocs2 | 1 | 2 | 1353 | 0.73 | 1.17 | 437 | 326 | 171 | 2.0 |
| p1winprocs2 | 1 | 2 | 1364 | 0.73 | 1.19 | 416 | 332 | 170 | 2.5 |
| winprocs1 | 8 | 2 | 5526 | 1.45 | 3.56 | 132 | 160 | 128 | 2.6 |
| p1winprocs1 | 8 | 2 | 5960 (+8%) | 1.34 (-8%) | 3.59 (+1%) | 126 (-5%) | 134 (-16%) | 126 (-1%) | 2.7 |
| winprocs2 | 8 | 2 | 5422 | 1.48 | 3.72 | 173 | 198 | 138 | 3.6 |
| p1winprocs2 | 8 | 2 | 5680 | 1.40 | 3.71 | 168 | 176 | 138 | 3.2 |
| winprocs1 | 32 | 2 | 7850 | 4.08 | 9.39 | 92 | 116 | 108 | 4.1 |
| p1winprocs1 | 32 | 2 | 8885 (+13%) | 3.60 (-12%) | 8.66 (-8%) | 84 (-9%) | 91 (-22%) | 102 (-6%) | 3.7 |
| winprocs2 | 32 | 2 | 7930 | 4.04 | 9.82 | 118 | 137 | 116 | 5.1 |
| p1winprocs2 | 32 | 2 | 8527 | 3.75 | 9.64 | 112 | 114 | 112 | 4.2 |

With a small GOMAXPROCS the stream-worker fix matters most: vttablet −10% (1 thread), −16% (8), −22% (32). It turns
GOMAXPROCS=1's 32-thread QPS loss into a gain.

**Best combination measured: P1 patch + static windows + GOMAXPROCS=1, vs base defaults (A/B 3 base):**
- 1 thread: 0.52 vs 0.78 ms (−33%); CPU/q vtgate 260 vs 560 (−54%), tablet 191 vs 490 (−61%), mysqld 151 vs 179.
- 8 threads: 5,960 vs 5,060 QPS (+18%); CPU/q 126/134/126 vs 217/221/144.
- 32 threads: 8,885 vs 8,173 QPS (+9%); CPU/q 84/91/102 vs 140/138/113.

(Cross-A/B comparison, same day, load 1.5–6: treat the QPS numbers as ±10%.)

## Method notes

- Scripts (all in `perf-findings/harness/H1-scripts/`):
  - `bench.py`: one sysbench run with per-process CPU from `/proc/<pid>/stat` and context switches summed over
    `/proc/<pid>/task/*/status`. With `perf=1` it also runs `perf stat` tracepoint counts (`raw_syscalls:sys_enter`,
    `sched:sched_switch`, `sched:sched_waking`) per process, and system-wide `irq_vectors:reschedule_entry` +
    `irq_vectors:call_function_single_entry` for IPIs.
  - `ab.sh`: alternating configs, restart outside the lock, each run under `flock -o`.
  - `agg.py`: means with [min–max].
  - `counts.sh` / `tracepair.sh`: Go execution traces. `H1-traceana` counts unblocks, goroutine starts, P
    starts/stops and futile P starts from `golang.org/x/exp/trace`.
  - `perfrec.sh`, `classify.py`, `groups.py`, `kbreak.py`: system-wide `perf record -g` and classification.
  - `place.sh`: `taskset` placement.
  - `ratescan.sh`: mysqld cost vs arrival rate.
  - `combo.sh` / `combobench.sh`: vtcombo via vttestserver on port 30600.
- `perf` from `linux-tools-6.8.0-142` works on this 6.18 guest kernel with software events and tracepoints (mount
  tracefs). There are no hardware counters, so no cycles, IPC or vmexit counts.
- Noise: the machine was shared with H2's cluster, and at times with Go builds (load 1–17). CPU/query was stable to
  ±2–3% within a config. QPS and latency at 8/32 threads moved ±10% between rounds.
- `perf sched timehist` run times were inconsistent with `/proc` (lost events at ~50k switches/s), so I didn't use
  them. Wake-up delays from it: avg ~8 µs, p90 18 µs for the Go threads.
