# This is an include file used by the other scripts in this directory.

# Most clusters will just be accessed with 'kubectl' on $PATH.
# However, some might require a different command. For example, GKE required
# KUBECTL='gcloud container kubectl' for a while. Now that most of our
# use cases just need KUBECTL=kubectl, we'll make that the default.
KUBECTL=${KUBECTL:-kubectl}

# This should match the nodePort in vtctld-service.yaml
VTCTLD_PORT=${VTCTLD_PORT:-30000}

# Get the ExternalIP of any node.
get_node_ip() {
  $KUBECTL get -o template -t '{{range (index .items 0).status.addresses}}{{if eq .type "ExternalIP" "LegacyHostIP"}}{{.address}}{{end}}{{end}}' nodes
}

# Try to find vtctld address if not provided.
get_vtctld_addr() {
  if [ -z "$VTCTLD_ADDR" ]; then
    node_ip=$(get_node_ip)
    if [ -n "$node_ip" ]; then
      VTCTLD_ADDR="$node_ip:$VTCTLD_PORT"
    fi
  fi
  echo "$VTCTLD_ADDR"
}

