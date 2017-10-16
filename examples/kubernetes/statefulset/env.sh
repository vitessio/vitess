# This is an include file used by the other scripts in this directory.

# Most clusters will just be accessed with 'kubectl' on $PATH.
# However, some might require a different command. For example, GKE required
# KUBECTL='gcloud container kubectl' for a while. Now that most of our
# use cases just need KUBECTL=kubectl, we'll make that the default.
KUBECTL=${KUBECTL:-kubectl}

# Kuberentes namespace for Vitess and components.
VITESS_NAME=${VITESS_NAME:-'default'}

# Find the name of a vtctld pod.
get_vtctld_pod() {
  $KUBECTL get -o template --template "{{if ge (len .items) 1 }}{{(index .items 0).metadata.name}}{{end}}" -l 'app=vitess,component=vtctld' pods --namespace=$VITESS_NAME
}

start_vtctld_forward() {
  pod=`get_vtctld_pod`
  if [ -z "$pod" ]; then
    >&2 echo "ERROR: Can't get vtctld pod name. Is vtctld running?"
    return 1
  fi

  tmpfile=`mktemp`
  $KUBECTL port-forward -p $pod 0:15999 --namespace=$VITESS_NAME &> $tmpfile &
  vtctld_forward_pid=$!

  until [[ `cat $tmpfile` =~ :([0-9]+)\ -\> ]]; do :; done
  vtctld_forward_port=${BASH_REMATCH[1]}
  rm $tmpfile
}

stop_vtctld_forward() {
  kill $vtctld_forward_pid
}

