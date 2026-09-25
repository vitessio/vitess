#!/bin/bash
# vtorc.sh start [extra flags] | stop
BIN=${VTORC_BIN:-/home/vt/bin}
LOG=/home/vt/perf/c40000/logs/vtorc.log
case "$1" in
  start)
    shift
    setpriv --reuid=vt --regid=vt --init-groups env HOME=/home/vt nohup $BIN/vtorc \
      --topo-implementation etcd2 --topo-global-server-address localhost:40010 --topo-global-root /vitess/global \
      --cell zone1 --port 40030 --log-format text --config-file-not-found-handling=ignore \
      --sqlite-data-file /home/vt/perf/c40000/vtorc.db "$@" > $LOG 2>&1 < /dev/null &
    echo $! > /home/vt/perf/c40000/vtorc.pid
    for i in $(seq 1 100); do curl -sf http://localhost:40030/debug/status >/dev/null && break; sleep 0.2; done
    echo "vtorc started pid $(cat /home/vt/perf/c40000/vtorc.pid)"
    ;;
  stop)
    kill $(cat /home/vt/perf/c40000/vtorc.pid) 2>/dev/null
    rm -f /home/vt/perf/c40000/vtorc.db*
    ;;
esac
