#!/bin/bash
# Runs the chaos scenarios. Must be run as root (it prepares cgroups and then drops to the
# unprivileged $RUN_USER, keeping CAP_NET_ADMIN for iptables), e.g.:
#
#   go/test/endtoend/vtorc/chaos/chaos_run.sh -test.run '^TestS1KillPrimaryMysqld$' -test.v -test.timeout 30m
#
# Arguments are passed verbatim to the compiled test binary (use -test.* flag names).
# Reports and log copies go to $CHAOS_RESULTS_DIR (default /home/$RUN_USER/chaos-results).
#
# Environment:
#   VTROOT          Vitess checkout whose bin/ holds vttablet, vtctld, vtctldclient, vtgate, vtorc,
#                   mysqlctl and mysqlctld (default: the checkout containing this script).
#   VT_MYSQL_ROOT   MySQL installation (default /home/user/mysql84).
#   ETCD_DIR        directory containing the etcd binary (default /home/user/vtlab/bin).
#   RUN_USER        unprivileged user to run as (default ubuntu).
#   CHAOS_DURABILITY=group_replication_cross_cell converts the shard to Group Replication after
#                   the semi-sync setup (see cluster.go).
#   CHAOS_TEST_BIN, CHAOS_SKIP_BUILD=1 run an already compiled test binary.
#   CHAOS_TABLET_EXTRA_ARGS extra vttablet flags (space separated) for every tablet.
# The per-run VTDATAROOT is deleted afterwards, unless CHAOS_KEEP_DATA=1.
set -uo pipefail

VTROOT=${VTROOT:-$(cd "$(dirname "$0")/../../../../.." && pwd)}
cd "$VTROOT"
RUN_USER=${RUN_USER:-ubuntu}
export VT_MYSQL_ROOT=${VT_MYSQL_ROOT:-/home/user/mysql84}
export VTDATAROOT=${VTDATAROOT:-/home/$RUN_USER/chaos-vtdataroot}
source build.env >/dev/null
# Our own binaries first; ETCD_DIR may contain other (older) Vitess binaries.
export PATH="$VTROOT/bin:$VT_MYSQL_ROOT/bin:${ETCD_DIR:-/home/user/vtlab/bin}:/usr/local/go/bin:/usr/local/bin:$PATH"

CG=/sys/fs/cgroup/unified/chaos
RESULTS=${CHAOS_RESULTS_DIR:-/home/$RUN_USER/chaos-results}
BIN=${CHAOS_TEST_BIN:-/home/$RUN_USER/e2e-bins/chaos.test}

mkdir -p "$CG"
for g in harness infra vtgate tablet1 tablet2 tablet3 tablet4 orc1 orc2 orc3 etcd1 etcd2 etcd3; do
  mkdir -p "$CG/$g"
  chown "$RUN_USER" "$CG/$g" "$CG/$g/cgroup.procs" "$CG/$g/cgroup.kill" 2>/dev/null || true
done
chown "$RUN_USER" "$CG" "$CG/cgroup.procs"

mkdir -p "$(dirname "$BIN")" "$RESULTS" "$VTDATAROOT" /home/$RUN_USER/tmp
if [ "${CHAOS_SKIP_BUILD:-0}" != 1 ]; then
  go test -c -o "$BIN" ./go/test/endtoend/vtorc/chaos || exit 1
fi
chown "$RUN_USER" "$(dirname "$BIN")" "$BIN" "$RESULTS" "$VTDATAROOT" /home/$RUN_USER/tmp

# Move this shell into the harness cgroup; the test process inherits it.
echo $$ > "$CG/harness/cgroup.procs"

cd go/test/endtoend/vtorc/chaos
env HOME=/home/$RUN_USER USER=$RUN_USER TMPDIR=/home/$RUN_USER/tmp CHAOS_E2E=1 CHAOS_RESULTS_DIR="$RESULTS" \
  setpriv --reuid="$RUN_USER" --regid="$RUN_USER" --init-groups \
  --inh-caps=+net_admin,+net_raw --ambient-caps=+net_admin,+net_raw \
  "$BIN" "$@"
rc=$?

# Leftovers of a crashed run, then the data directories (disk is limited).
for g in infra vtgate tablet1 tablet2 tablet3 tablet4 orc1 orc2 orc3 etcd1 etcd2 etcd3; do
  echo 1 > "$CG/$g/cgroup.kill" 2>/dev/null || true
done
if [ "${CHAOS_KEEP_DATA:-0}" != 1 ]; then
  sleep 1
  rm -rf "$VTDATAROOT"/vtroot_* /home/$RUN_USER/tmp/chaos-wrap-* 2>/dev/null
fi
exit $rc
