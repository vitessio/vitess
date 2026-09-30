# Brief for bug validators (VAL-A…D)

Goal: **validate or refute** bugs in `perf-findings/BUGS.md` against unpatched code (base commit aa9ccf9, i.e. your worktree HEAD minus `perf-findings/`). **Do not fix anything.** The user explicitly asked for no implementation yet. Deliver:
- a failing test, or a scripted reproduction, that shows the bug on base; or
- evidence that the bug does not exist as described (refuted), or can't be triggered in practice (downgrade).

## Rules

- Work in your own git worktree. Don't commit, push or open PRs.
- Tests follow CLAUDE.md conventions: testify `assert`/`require`, `t.Context()`, `t.Cleanup()`. A validation test should FAIL on base and describe the expected (correct) behaviour, so it can later become the fix's regression test. Name it after the bug.
- Run `scripts/fmt` on the Go files you add.
- If a patch in `perf-findings/` already contains a fix, you may apply it in a scratch copy to confirm your test turns green. Report that as a bonus; don't leave the fix in your deliverable.

### MySQL-backed Go tests (vreplication, vstreamer, vdiff, ...)

1. Compile as root: `export GOFLAGS=-trimpath; go test -c -o /home/vt/<ID>/x.test ./go/vt/...`, then `chown -R vt:vt /home/vt/<ID>`.
2. Run as `vt` from the package directory: `runuser -u vt -- env HOME=/home/vt VTROOT=/home/vt VT_MYSQL_ROOT=/usr VTDATAROOT=/home/vt/<ID>/data PATH=/home/vt/bin:/usr/sbin:/usr/bin:/bin /home/vt/<ID>/x.test -test.run '<Name>' -test.v`.
3. Create the data dir first (vt-owned). `/home/vt/bin` holds base binaries including mysqlctl; `/home/vt/config` holds the repo's config dir.
4. Afterwards, kill any orphaned test mysqlds you started (`ps -u vt`) and delete your data dirs.

### Clusters

- Harness: `/home/vt/perf/cluster.sh` (single keyspace, `SHARDS`, `REPLICAS`) and `/home/vt/perf/P4-scripts/` (multi-keyspace VReplication).
- Run as `vt` with your assigned BASE. The base binaries are in `/home/vt/bin`.
- Never start a cluster inside `flock`. Use `flock -o /home/vt/perf/bench.lock` only for timing-sensitive measurements.
- Run `down` your cluster and delete data when finished.
- Disk: ~18 GB free, shared.

## Deliverables

In `/tmp/claude-0/-home-user-vitess/d4e874e9-36db-544e-abf5-6cb4fd4871ca/scratchpad/findings/`:
- `<ID>.md`: per bug, the verdict (**REPRODUCED** / **TEST FAILS ON BASE** / **REFUTED** / **CANNOT REPRODUCE** / **DOWNGRADE**), the evidence (test name and failure output, or repro steps and output), the conditions needed to trigger it, and a suggested priority change if any.
- `<ID>.patch`: `git add -N . && git diff`, with tests and repro scripts only.

Final reply: at most 400 words, one line per bug with verdict and evidence.
