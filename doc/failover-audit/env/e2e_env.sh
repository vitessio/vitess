# Source this file (bash, as root) to run the Vitess end-to-end tests and the chaos harness:
#
#   E2E_SETUP=1 source doc/failover-audit/env/e2e_env.sh   # first time: installs and builds
#   source doc/failover-audit/env/e2e_env.sh               # later: only sets the environment
#
# The one-time setup (E2E_SETUP=1), tested on Ubuntu 24.04 as root:
#   1. Installs MySQL 8.0 from apt and disables the system mysqld (Vitess runs its own mysqld).
#   2. Builds etcd $ETCD_VER (from build.env) from the Go module proxy and installs it to
#      /usr/local/bin/etcd. A wrapper module is used because `go install` of
#      go.etcd.io/etcd/server/v3 fails on its replace directives.
#   3. Builds the Vitess binaries into $VTROOT/bin (NOVTADMINBUILD=1 make build).
#   4. Creates the unprivileged user $RUN_USER (default "vitess"): Vitess servers refuse to run
#      as root, so tests are compiled as root and run as that user (see e2e_run and
#      go/test/endtoend/vtorc/chaos/chaos_run.sh), with CAP_NET_ADMIN/CAP_NET_RAW kept as
#      ambient capabilities so that iptables works from the test process.
#
# Environment: VTROOT (default: the checkout this file is in), RUN_USER (default: vitess),
# E2E_WORK (scratch directory for the etcd build, default: $HOME/e2e-work).

VTROOT=${VTROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)}
RUN_USER=${RUN_USER:-vitess}
RUN_HOME=/home/$RUN_USER
E2E_WORK=${E2E_WORK:-$HOME/e2e-work}

cd "$VTROOT" || return 1
source build.env >/dev/null # exports VTROOT, VTDATAROOT, ETCD_VER, PATH

_e2e_setup() {
  set -e
  export DEBIAN_FRONTEND=noninteractive
  if ! command -v mysqld >/dev/null 2>&1; then
    apt-get update -qq
    apt-get install -y -qq mysql-server-8.0 mysql-client
  fi
  service mysql stop >/dev/null 2>&1 || true
  systemctl disable mysql >/dev/null 2>&1 || true

  if ! command -v etcd >/dev/null 2>&1; then
    local d="$E2E_WORK/etcdbuild"
    mkdir -p "$d"
    cat > "$d/main.go" <<'GO'
package main

import (
	"os"

	"go.etcd.io/etcd/server/v3/etcdmain"
)

func main() {
	etcdmain.Main(os.Args)
}
GO
    (
      cd "$d"
      rm -f go.mod go.sum
      go mod init etcdbuild >/dev/null 2>&1
      go get "go.etcd.io/etcd/server/v3@$ETCD_VER" "go.etcd.io/etcd/api/v3@$ETCD_VER" \
        "go.etcd.io/etcd/client/v3@$ETCD_VER" "go.etcd.io/etcd/client/pkg/v3@$ETCD_VER" "go.etcd.io/etcd/pkg/v3@$ETCD_VER"
      CGO_ENABLED=0 go build -trimpath -o etcd .
      cp etcd /usr/local/bin/etcd
    )
  fi

  (cd "$VTROOT" && NOVTADMINBUILD=1 make build)

  id "$RUN_USER" >/dev/null 2>&1 || useradd -m -s /bin/bash "$RUN_USER"
  mkdir -p "$VTDATAROOT" "$RUN_HOME/e2e-bins" "$RUN_HOME/tmp"
  chown -R "$RUN_USER:$RUN_USER" "$VTDATAROOT" "$RUN_HOME/e2e-bins" "$RUN_HOME/tmp"
  set +e
}

if [ "${E2E_SETUP:-0}" = "1" ]; then
  _e2e_setup
fi

export PATH="$VTROOT/bin:/usr/local/bin:$PATH"
export VTROOT VTDATAROOT RUN_USER

# e2e_clean stops every process left over from a previous run, removes the chaos harness's
# iptables chains and the per-cluster data, so the next run starts clean.
e2e_clean() {
  pkill -9 -u "$RUN_USER" -f 'vttablet|vtorc|vtgate|vtctld|mysqlctl|mysqld|etcd|chaos.test' 2>/dev/null || true
  sleep 1
  local c
  for c in CHAOS_MARK CHAOS_DROP; do
    while iptables -w 5 -D OUTPUT -j "$c" 2>/dev/null; do :; done
    iptables -w 5 -F "$c" 2>/dev/null
    iptables -w 5 -X "$c" 2>/dev/null
  done
  rm -rf "$VTDATAROOT"/vtroot_* "$RUN_HOME"/tmp/chaos-wrap-*
  mkdir -p "$VTDATAROOT"
  chown "$RUN_USER:$RUN_USER" "$VTDATAROOT"
  rm -f /tmp/endtoend.port # root-owned leftover from a plain `go test` run as root
}

# e2e_run <package-dir> [go test flags]: compiles an e2e test package as root and runs it as
# $RUN_USER. Usual go test flags (-run, -timeout, -v, -count) are translated to -test.* flags.
e2e_run() {
  local pkg="$1"
  shift
  local dir
  dir="$(cd "$VTROOT" && cd "$pkg" && pwd)" || return 1
  local bin
  bin="$RUN_HOME/e2e-bins/$(echo "${dir#"$VTROOT"/}" | tr '/' '_').test"
  mkdir -p "$RUN_HOME/e2e-bins" "$RUN_HOME/tmp"
  (cd "$VTROOT" && go test -c -o "$bin" "./${dir#"$VTROOT"/}") || return 1
  chown "$RUN_USER:$RUN_USER" "$bin" "$RUN_HOME/tmp"
  local args=() a
  for a in "$@"; do
    case "$a" in
      -test.*) args+=("$a") ;;
      -*) args+=("-test.${a#-}") ;;
      *) args+=("$a") ;;
    esac
  done
  (cd "$dir" && HOME="$RUN_HOME" USER="$RUN_USER" TMPDIR="$RUN_HOME/tmp" \
    setpriv --reuid="$RUN_USER" --regid="$RUN_USER" --init-groups \
    --inh-caps=+net_admin,+net_raw --ambient-caps=+net_admin,+net_raw \
    "$bin" "${args[@]}")
}
