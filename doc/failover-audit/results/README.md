# Chaos run reports

One `report.txt` per scenario and run, as `go/test/endtoend/vtorc/chaos` writes them: outcome, timings, violations and notes. They are the data behind the tables in `../SemiSyncFailover.md`. The component logs are not included.

| Directory | `chaos_plan.sh` phase | Profile | Binaries |
|---|---|---|---|
| `main-semisync-3vtorc` | `main` | `semisync-3vtorc` | `main` (`fb653f2`) |
| `main-semisync-1vtorc-colo` | `main` | `semisync-1vtorc`, primary in the VTOrc's cell (`zone1`) | `main` |
| `main-semisync-1vtorc-remote` | `main` | `semisync-1vtorc`, primary in `zone2` | `main` |
| `main-probe-semisync-3vtorc-{1,2}` | `probe` | `semisync-3vtorc` | `main` |
| `main-probe-semisync-1vtorc-{1,2}` | `probe` | `semisync-1vtorc`, primary in `zone1` | `main` |
| `fixed-semisync-3vtorc-{1,2}` | `fixed` | `semisync-3vtorc` | this branch |
| `fixed-semisync-1vtorc-{1,2}` | `fixed` | `semisync-1vtorc`, primary in `zone1` | this branch |
| `main-prs-semisync-3vtorc` | `prs main` | `semisync-3vtorc` | `main` |
| `main-prs-semisync-1vtorc` | `prs main` | `semisync-1vtorc`, primary in `zone1` | `main` |
| `fixed-prs-semisync-3vtorc` | `prs fixed` | `semisync-3vtorc` | this branch |
| `fixed-prs-semisync-1vtorc` | `prs fixed` | `semisync-1vtorc`, primary in `zone1` | this branch |
| `fixed-relaylog-safe-semisync-3vtorc` | S11, S11k, S11ka, S13-T* with `CHAOS_RELAY_LOG_SAFE=1` (`relay_log_recovery=0`, `sync_relay_log=1`) | `semisync-3vtorc` | this branch |
| `fixed-t27-relaylog-safe-semisync-3vtorc` | S11, S11k, S11ka, S13-T1, S13-T3, S13-T3b with `CHAOS_RELAY_LOG_SAFE=1` | `semisync-3vtorc` | this branch, with the T27 fix |

Paths in the reports (`/home/ubuntu/...`, `/home/user/...`) are those of the test host.

`go/test/endtoend/vtorc/chaos/chaos_summary.py <directory>...` rebuilds the report's tables from them; `go/test/endtoend/vtorc/chaos/README.md` describes how to rerun them.
