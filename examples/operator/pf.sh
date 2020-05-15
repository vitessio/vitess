#!/bin/bash

killall () {
    PID=$$
    kill -9 -$(ps -o pgid= $PID | grep -o '[0-9]*')
}

trap killall EXIT

vtctld(){
 kubectl port-forward --address localhost service/"$(kubectl get service --selector="planetscale.com/component=vtctld" -o=jsonpath="{.items..metadata.name}")" 15000 15999 >> pf.log 2>&1 &
 process_id1=$!
}

vtgate() {
 kubectl port-forward --address localhost service/"$(kubectl get service --selector="planetscale.com/component=vtgate" -o=jsonpath="{.items[0].metadata.name}")" 15306:3306 >> pf.log 2>&1 &
 process_id2=$!
}

restart_vtctld_pf() {
 echo "ERROR: vtctld port forward failed. restarting..."
 kill $process_id1 > /dev/null 2&>1 # might have terminated
 vtctld
}

restart_vtgate_pf() {
 echo "ERROR: vtgate port forward failed. restarting..."
 kill $process_id2 > /dev/null 2&>1 # might have terminated
 vtgate
}

healthcheck(){
 while true; do
  vtctlclient -server=localhost:15999 Validate > /dev/null 2>&1 || restart_vtctld_pf
  mysql -h 127.0.0.1 -P 15306 -u user -e 'select 1' > /dev/null 2>&1 || restart_vtgate_pf
  sleep 3
 done
}

echo "Starting port-forwards.."

vtctld
vtgate
sleep 2

echo "You may point your browser to http://localhost:15000, use the following aliases as shortcuts:"
echo 'alias vtctlclient="vtctlclient -server=localhost:15999"'
echo 'alias mysql="mysql -h 127.0.0.1 -P 15306 -u user"'
echo "Hit Ctrl-C to stop the port forwards"

# The current behavior of kubectl port-forward is very annoying.
# If the pod that it is routing to fails, it won't auto-reconnect,
# but it won't hard fail either. Instead it just logs an error saying
# the pod it was routing to has disappeared, The solution for now is to
# healthcheck both of the port-forwards, and re-establish them if they
# appear to be broken.

healthcheck
