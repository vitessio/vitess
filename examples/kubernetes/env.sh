# This is an include file used by the other scripts in this directory.

# Most clusters will just be accessed with 'kubectl' on $PATH.
# However, some might require a different command. For example, GKE required
# KUBECTL='gcloud container kubectl' for a while. Now that most of our
# use cases just need KUBECTL=kubectl, we'll make that the default.
KUBECTL=${KUBECTL:-kubectl}

# This should match the nodePort in vtctld-service.yaml
VTCTLD_PORT=${VTCTLD_PORT:-30001}

# Get the ExternalIP of any node.
get_node_ip() {
  $KUBECTL get -o template -t '{{range (index .items 0).status.addresses}}{{if eq .type "ExternalIP" "LegacyHostIP"}}{{.address}}{{end}}{{end}}' nodes
}

# Try to find vtctld address if not provided.
get_vtctld_addr() {
  if [ -z "$VTCTLD_ADDR" ]; then
    VTCTLD_HOST=$(get_node_ip)
    if [ -n "$VTCTLD_HOST" ]; then
      VTCTLD_ADDR="$VTCTLD_HOST:$VTCTLD_PORT"
    fi
  fi
}

# Find the name of a vtctld pod.
get_vtctld_pod() {
  $KUBECTL get -o template -t "{{if ge (len .items) 1 }}{{(index .items 0).metadata.name}}{{end}}" -l 'app=vitess,component=vtctld' pods
}

start_vtctld_forward() {
  pod=`get_vtctld_pod`
  if [ -z "$pod" ]; then
    >&2 echo "ERROR: Can't get vtctld pod name. Is vtctld running?"
    return 1
  fi

  tmpfile=`mktemp`
  $KUBECTL port-forward -p $pod 0:15999 &> $tmpfile &
  vtctld_forward_pid=$!

  until [[ `cat $tmpfile` =~ :([0-9]+)\ -\> ]]; do :; done
  vtctld_forward_port=${BASH_REMATCH[1]}
  rm $tmpfile
}

stop_vtctld_forward() {
  kill $vtctld_forward_pid
}

config_file=`dirname "${BASH_SOURCE}"`/config.sh
if [ ! -f $config_file ]; then
  echo "Please run ./configure.sh first to generate config.sh file."
  exit 1
fi

source $config_file

