host=$(minikube service vtgate-zone1 --format "{{.IP}}" | tail -n 1)
port=$(minikube service vtgate-zone1 --format "{{.Port}}" | tail -n 1)

mysql -h $host -P $port $*
