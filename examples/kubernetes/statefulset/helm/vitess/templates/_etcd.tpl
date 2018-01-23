{{- define "etcd" -}}
{{- $ := index . 0 -}}
{{- $cell := index . 1 -}}
{{- with (index . 2) -}}
{{- $0 := $.Values.etcd -}}
{{- $replicas := .replicas | default $0.replicas -}}
# etcd
# Regular service for load balancing client connections.
kind: Service
apiVersion: v1
metadata:
  name: "etcd-{{$cell.name}}"
  labels:
    component: etcd
    cell: {{$cell.name | quote}}
    app: vitess
spec:
  ports:
    - port: 4001
  selector:
    component: etcd
    cell: {{$cell.name | quote}}
    app: vitess
---
# Headless service for etcd cluster bootstrap.
kind: Service
apiVersion: v1
metadata:
  name: "etcd-{{$cell.name}}-srv"
  labels:
    component: etcd
    cell: {{$cell.name | quote}}
    app: vitess
spec:
  clusterIP: None
  ports:
    - name: etcd-server
      port: 7001
  selector:
    component: etcd
    cell: {{$cell.name | quote}}
    app: vitess
---
apiVersion: extensions/v1beta1
kind: ReplicaSet
metadata:
  name: "etcd-{{$cell.name}}"
spec:
  replicas: {{$replicas}}
  template:
    metadata:
      labels:
        component: etcd
        cell: {{$cell.name | quote}}
        app: vitess
    spec:
      volumes:
        - name: certs
          hostPath: { path: {{$.Values.certsPath | quote}} }
      containers:
        - name: etcd
          image: {{.image | default $0.image | quote}}
          volumeMounts:
            - name: certs
              readOnly: true
              # Mount root certs from the host OS into the location
              # expected for our container OS (Debian):
              mountPath: /etc/ssl/certs/ca-certificates.crt
          resources:
{{ toYaml (.resources | default $0.resources) | indent 12 }}
          command:
            - bash
            - "-c"
            - |
              set -ex

              ipaddr=$(hostname -i)
              peer_url="http://$ipaddr:7001"
              client_url="http://$ipaddr:4001"

              export ETCD_NAME=$HOSTNAME
              export ETCD_DATA_DIR=/vt/vtdataroot/etcd-$ETCD_NAME
              export ETCD_STRICT_RECONFIG_CHECK=true
              export ETCD_ADVERTISE_CLIENT_URLS=$client_url
              export ETCD_INITIAL_ADVERTISE_PEER_URLS=$peer_url
              export ETCD_LISTEN_CLIENT_URLS=$client_url
              export ETCD_LISTEN_PEER_URLS=$peer_url

              if [ -d $ETCD_DATA_DIR ]; then
                # We've been restarted with an intact datadir.
                # Just run without trying to do any bootstrapping.
                echo "Resuming with existing data dir: $ETCD_DATA_DIR"
              else
                # This is the first run for this member.

                # If there's already a functioning cluster, join it.
                echo "Checking for existing cluster by trying to join..."
                if result=$(etcdctl -C http://etcd-{{$cell.name}}:4001 member add $ETCD_NAME $peer_url); then
                  [[ "$result" =~ ETCD_INITIAL_CLUSTER=\"([^\"]*)\" ]] && \
                  export ETCD_INITIAL_CLUSTER="${BASH_REMATCH[1]}"
                  export ETCD_INITIAL_CLUSTER_STATE=existing
                  echo "Joining existing cluster: $ETCD_INITIAL_CLUSTER"
                else
                  # Join failed. Assume we're trying to bootstrap.

                  # First register with global topo, if we aren't global.
                  if [ "{{$cell.name}}" != "global" ]; then
                    echo "Registering cell "{{$cell.name}}" with global etcd..."
                    until etcdctl -C "http://etcd-global:4001" \
                        set "/vt/cells/{{$cell.name}}" "http://etcd-{{$cell.name}}:4001"; do
                      echo "[$(date)] waiting for global etcd to register cell '{{$cell.name}}'"
                      sleep 1
                    done
                  fi

                  # Use DNS to bootstrap.

                  # First wait for the desired number of replicas to show up.
                  echo "Waiting for {{$replicas}} replicas in SRV record for etcd-{{$cell.name}}-srv..."
                  until [ $(getsrv etcd-server tcp etcd-{{$cell.name}}-srv | wc -l) -eq {{$replicas}} ]; do
                    echo "[$(date)] waiting for {{$replicas}} entries in SRV record for etcd-{{$cell.name}}-srv"
                    sleep 1
                  done

                  export ETCD_DISCOVERY_SRV=etcd-{{$cell.name}}-srv
                  echo "Bootstrapping with DNS discovery:"
                  getsrv etcd-server tcp etcd-{{$cell.name}}-srv
                fi
              fi

              # We've set up the env as we want it. Now run.
              exec etcd
          lifecycle:
            preStop:
              exec:
                command:
                  - bash
                  - "-c"
                  - |
                    # Find our member ID.
                    members=$(etcdctl -C http://etcd-{{$cell.name}}:4001 member list)
                    if [[ "$members" =~ ^([0-9a-f]+):\ name=$HOSTNAME ]]; then
                      member_id=${BASH_REMATCH[1]}
                      echo "Removing $HOSTNAME ($member_id) from etcd-{{$cell.name}} cluster..."
                      etcdctl -C http://etcd-{{$cell.name}}:4001 member remove $member_id
                    fi
{{- end -}}
{{- end -}}

