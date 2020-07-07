#!/bin/sh

kubectl port-forward service/vtctld 15000 15999 &
process_id1=$!
kubectl port-forward service/vtgate-zone1 15306:3306 15001 &
process_id2=$!
sleep 2
echo "You may point your browser to http://localhost:15000 for vtctld."
echo "You may point your browser to http://localhost:15001 for vtgate, use the following aliases as shortcuts:"
echo 'alias vtctlclient="vtctlclient -server=localhost:15999"'
echo 'alias mysql="mysql -h 127.0.0.1 -P 15306"'
echo "Hit Ctrl-C to stop the port forwards"
wait $process_id1
wait $process_id2
