# VTOrc chaos harness

This package runs a real cluster on one host and injects faults into it, then checks its invariants: no acknowledged write lost, no two writable primaries, convergence. The cluster has 3 cells, a tablet per cell, etcd, vtgate and one or more VTOrcs. The faults are process kills and hangs, mysqld restarts, and network partitions. The results are in `doc/failover-audit/SemiSyncFailover.md`.

Every simulated node runs in its own cgroup v2 leaf, and iptables drops traffic between nodes by cgroup. All the processes share 127.0.0.1, so a partition is a set of rules on connection marks and ports, not on addresses (see `netfault.go`).

## Files

| File | What it does |
|---|---|
| `scenarios_test.go`, `s11_test.go`, `s12_test.go`, `s13_test.go`, `vtorc_single_test.go`, `model_scenarios_test.go`, `prs_test.go` | The scenarios. The comment above each test describes its fault and what it checks. `prs_test.go` holds the PlannedReparentShard scenarios (R1, R2, R2b, R3, R5). |
| `profile.go` | The deployment profiles: `audit`, `semisync-3vtorc`, `semisync-1vtorc`. |
| `cluster.go`, `faults.go`, `netfault.go` | The cluster and the fault injection. |
| `workload.go`, `observer.go`, `invariants.go` | The write workload, the observer that samples every tablet, and the checks. |
| `chaos_run.sh` | Runs the test binary as root, with the cgroups and capabilities the harness needs. |
| `chaos_matrix.sh` | Runs a list of scenarios one by one under a profile. |
| `chaos_plan.sh` | The scenario plan behind the report, in phases (below). |
| `chaos_summary.py` | Turns the results of the matrix into the report's tables. |

## Prerequisites

The runs in the report used this host. Other versions should work, but these are the ones verified.

| | Used for the report |
|---|---|
| OS | Ubuntu 24.04, Linux 6.18, linux/amd64 |
| Go | 1.27.1 (whatever `go.mod` requires) |
| MySQL | 8.4.6, `mysql-8.4.6-linux-glibc2.17-x86_64-minimal.tar.xz` from dev.mysql.com, unpacked to `VT_MYSQL_ROOT` |
| etcd | 3.5.17, `etcd-v3.5.17-linux-amd64.tar.gz` from the etcd GitHub releases; the binary goes in `ETCD_DIR` |
| iptables | 1.8.10 (nf_tables), with the `cgroup` and `connmark` matches |
| Other | `setpriv` (util-linux); an unprivileged user to run the cluster as (`RUN_USER`, default `ubuntu`) |

The scripts must run as root. `chaos_run.sh` creates the cgroups, moves itself into the harness cgroup and runs the test binary as `RUN_USER` with `CAP_NET_ADMIN` and `CAP_NET_RAW`, so that it can change iptables without running MySQL as root.

The harness needs a cgroup v2 hierarchy. `chaos_run.sh` uses `/sys/fs/cgroup` on a cgroup v2 host and `/sys/fs/cgroup/unified` on a hybrid v1/v2 host (the report's runs). Set `CHAOS_CGROUP2_MOUNT` to use another mount point.

`chaos_run.sh` documents the other environment variables. These are the ones you are likely to need:

```
export VT_MYSQL_ROOT=/opt/mysql-8.4.6     # default /home/user/mysql84
export ETCD_DIR=/opt/etcd-v3.5.17         # default /home/user/vtlab/bin
export RUN_USER=ubuntu
```

## Vitess binaries

The test binary always comes from this branch: `chaos_matrix.sh` builds it, unless `CHAOS_SKIP_BUILD=1`. The cluster runs the Vitess binaries in `bin/` of the checkout: `vttablet`, `vtctld`, `vtctldclient`, `vtgate`, `vtorc`, `mysqlctl`, `mysqlctld` and `vtctl`.

The report compares two sets of binaries:
- **main.** The base of this branch, `fb653f2`.
- **fixed.** This branch. Its Vitess code has not changed since the report's runs.

```
# Binaries of main, built from a separate worktree into this checkout's bin/.
git worktree add ../vitess-main fb653f2
(cd ../vitess-main && source build.env && for b in vttablet vtctld vtctldclient vtgate vtorc mysqlctl mysqlctld vtctl; do
  go build -o "$OLDPWD/bin/$b" ./go/cmd/$b; done)

# Binaries of this branch.
source build.env && for b in vttablet vtctld vtctldclient vtgate vtorc mysqlctl mysqlctld vtctl; do
  go build -o bin/$b ./go/cmd/$b; done
```

## Running the report's plan

```
# 1. With the binaries of main in bin/:
go/test/endtoend/vtorc/chaos/chaos_plan.sh main     # several hours
go/test/endtoend/vtorc/chaos/chaos_plan.sh probe
go/test/endtoend/vtorc/chaos/chaos_plan.sh prs main
# 2. With the binaries of this branch in bin/:
go/test/endtoend/vtorc/chaos/chaos_plan.sh fixed
go/test/endtoend/vtorc/chaos/chaos_plan.sh prs fixed
go/test/endtoend/vtorc/chaos/chaos_plan.sh relaylog
# 3. The tables:
go/test/endtoend/vtorc/chaos/chaos_summary.py /home/ubuntu/chaos-results/main-semisync-3vtorc \
  /home/ubuntu/chaos-results/main-semisync-1vtorc-colo /home/ubuntu/chaos-results/main-semisync-1vtorc-remote
```

Each scenario writes `report.txt` (outcome, timings, violations, notes) and copies of the component logs to `$CHAOS_PLAN_RESULTS/<run>/<scenario>/`. The default is `/home/$RUN_USER/chaos-results`.

Expect timing variation between runs. Most of the report's failover times and outage lengths come from a single run, and P2's failover time ranged from 25s to 103s over its eight runs.

The `main` phase ran with a harness build from shortly before this branch's first commit. The one later change that affects its reports: DRAINED tablets are no longer checked for catching up with the primary, so a rerun does not report the "did not catch up" violations that the report lists as artifacts. The `probe` and `fixed` phases ran with the harness as it is here, apart from the profile names. S11b still reports a harness artifact (T23). The S13 scenarios always run with `relay_log_recovery=0` and `sync_relay_log=1`; the `relaylog` phase sets `CHAOS_RELAY_LOG_SAFE=1` so that S11, S11k and S11ka do too.

## One scenario

```
CHAOS_PROFILE=semisync-3vtorc go/test/endtoend/vtorc/chaos/chaos_matrix.sh TestS3IsolatePrimary
```

The tests skip unless `CHAOS_E2E=1`, which `chaos_run.sh` sets, so `go test ./go/test/endtoend/vtorc/chaos` on its own only runs the unit tests.
