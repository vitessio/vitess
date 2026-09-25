# Brief for end-to-end performance investigators (round 2)

## Goal

Find changes that end users would actually notice in a running Vitess cluster: lower query latency (avg and p99), higher throughput per CPU, faster operations (VReplication, backup/restore, failover), fewer errors or stalls.

- Micro-optimisations that don't move an end-to-end number are out of scope.
- Round 1 is already done. It covered byte loops, per-row allocations and formatting; see `perf-findings/SUMMARY.md`. Don't redo those items. You may apply round-1 patches (`perf-findings/F*.patch`) if they are relevant to your workload and you want to measure them end to end.

## Environment

- You run as root in your own git worktree of vitessio/vitess. Go is 1.27.1. The machine has 4 vCPUs and 15 GB RAM, and it is SHARED: one other investigator runs a cluster at the same time.
- MySQL 8.0.46, sysbench and etcd are installed. Vitess binaries built from the base commit are in `/home/vt/bin`.
- Clusters must run as the unprivileged user `vt`. Use the harness at `/home/vt/perf/cluster.sh`, which is a copy of `perf-findings/harness/cluster.sh`. Read its header first.
  ```
  cd /home/vt/perf
  C="runuser -u vt -- env BASE=<your BASE> HOME=/home/vt BIN=/home/vt/bin ./cluster.sh"
  $C up && $C prepare      # ~1 min
  $C sb oltp_point_select --threads=8 --time=20 run
  $C cpu                   # CPU seconds per process; diff before/after a run for CPU/query
  $C down
  ```
- Use ONLY your assigned BASE port range (BASE .. BASE+999). Never touch another cluster's processes or data dirs.
- Always `down` your cluster before you finish.
- You may extend the harness: copy it to `/home/vt/perf/<ID>/` and edit it there, e.g. to add a second keyspace or replicas. Put the changed copy in your patch under `perf-findings/harness/` if it is generally useful.
- `pprof`: `curl -o x.pprof "http://localhost:<web port>/debug/pprof/profile?seconds=15"` during a run, then `go tool pprof -top <binary> x.pprof`. Also useful: `/debug/pprof/allocs`, `mutex`, `block` (mutex and block profiling may need enabling via flags; check the servenv pprof flags), and `/debug/vars` for stats.
- Build patched binaries from your worktree:
  ```
  export GOFLAGS=-trimpath
  go build -o /home/vt/bin-<ID>/ ./go/cmd/vtgate ./go/cmd/vttablet ./go/cmd/vtctld ./go/cmd/vtctldclient ./go/cmd/mysqlctl ./go/cmd/vtorc ./go/cmd/vtctl
  chown -R vt:vt /home/vt/bin-<ID>
  ```
  Then run the cluster with `BIN=/home/vt/bin-<ID>`.
- Baseline numbers are in `perf-findings/BASELINE.md`. Point select through vtgate is ~1.5 ms avg against 0.07 ms direct to MySQL. The CPU cost per query is ~200 µs in vtgate, ~215 µs in the vttablet that serves it, and ~145 µs in mysqld. That is a lot of headroom.

## Method

1. Measure the baseline for your workload: QPS, avg, p95 and p99 latency, and CPU µs per query for each process.
2. Profile: CPU, allocations, mutex and block profiles, goroutine counts.
3. Form hypotheses and test them. Many questions are answered by a flag or config change before you touch any code, e.g. pool sizes, gRPC settings or GC settings.
4. Prototype the most promising fixes.
5. Measure A/B end to end:
   - Alternate baseline and patched runs, at least 3 rounds each.
   - CPU µs per query from `cluster.sh cpu` is more robust than latency on this noisy shared machine; report both.
   - Note in your report how loaded the machine was (`uptime`).
6. Be honest about noise. If an improvement isn't clearly above run-to-run variance, say so.

## Rules

- Do NOT commit, push, or open PRs. Leave changes uncommitted in your worktree.
- Repo conventions from CLAUDE.md apply to code you write: testify, `t.Context()`, vterrors, release compatibility (N-1/N+1), and `scripts/fmt`.
- Keep total CPU hogging reasonable; the machine is shared.

## Deliverables

- `git diff > /tmp/claude-0/-home-user-vitess/d4e874e9-36db-544e-abf5-6cb4fd4871ca/scratchpad/findings/<ID>.patch` (run `git add -N .` first so new files are included).
- A detailed report at `.../scratchpad/findings/<ID>.md`. Include:
  - baseline numbers and profiles (top entries);
  - every hypothesis tested and its outcome, including negative results;
  - the A/B tables;
  - the recommended changes, ranked by user-visible impact.
- Your final reply must be at most 500 words:
  - TOP FINDINGS, ranked. For each:
    - the change (file, flag or config);
    - the measured end-to-end effect (QPS, latency, CPU/query);
    - difficulty (S/M/L);
    - risk and gotchas.
  - NEGATIVE RESULTS: what you tried that didn't help.
  - FOLLOW-UPS: what you'd investigate next.
