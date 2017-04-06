# vttablet pod spec (template for both StatefulSet and naked pod)
{{- define "vttablet-pod-spec" -}}
{{- $ := index . 0 -}}
{{- $cell := index . 1 -}}
{{- $keyspace := index . 2 -}}
{{- $shard := index . 3 -}}
{{- $tablet := index . 4 -}}
{{- $uid := index . 5 -}}
{{- with index . 6 -}}
{{- $0 := $.Values.vttablet -}}
{{- $controllerType := .controllerType | default $0.controllerType -}}
containers:
  - name: vttablet
    image: {{.image | default $0.image | quote}}
    livenessProbe:
      httpGet:
        path: /debug/vars
        port: 15002
      initialDelaySeconds: 60
      timeoutSeconds: 10
    volumeMounts:
      - name: syslog
        mountPath: /dev/log
      - name: vtdataroot
        mountPath: /vt/vtdataroot
      - name: certs
        readOnly: true
        # Mount root certs from the host OS into the location
        # expected for our container OS (Debian):
        mountPath: /etc/ssl/certs/ca-certificates.crt
    resources:
{{ toYaml (.resources | default $0.resources) | indent 6 }}
    ports:
      - name: web
        containerPort: 15002
      - name: grpc
        containerPort: 16002
    securityContext:
      runAsUser: 999
    command:
      - bash
      - "-c"
      - |
        set -ex
        eval exec /vt/bin/vttablet $(cat <<END_OF_COMMAND
          -topo_implementation "etcd"
          -etcd_global_addrs "http://etcd-global:4001"
          -log_dir "$VTDATAROOT/tmp"
          -alsologtostderr
          -port 15002
          -grpc_port 16002
          -service_map "grpc-queryservice,grpc-tabletmanager,grpc-updatestream"
          -tablet-path "{{$cell.name}}-{{$uid}}"
{{ if eq $controllerType "None" }}
          -tablet_hostname "$(hostname -i)"
{{ else }}
          -tablet_hostname "$(hostname).vttablet"
{{ end }}
          -init_keyspace {{$keyspace.name | quote}}
          -init_shard {{$shard.name | quote}}
          -init_tablet_type {{$tablet.type | quote}}
          -health_check_interval "5s"
          -mysqlctl_socket "$VTDATAROOT/mysqlctl.sock"
          -db-config-app-uname "vt_app"
          -db-config-app-dbname "vt_{{$keyspace.name}}"
          -db-config-app-charset "utf8"
          -db-config-dba-uname "vt_dba"
          -db-config-dba-dbname "vt_{{$keyspace.name}}"
          -db-config-dba-charset "utf8"
          -db-config-repl-uname "vt_repl"
          -db-config-repl-dbname "vt_{{$keyspace.name}}"
          -db-config-repl-charset "utf8"
          -db-config-filtered-uname "vt_filtered"
          -db-config-filtered-dbname "vt_{{$keyspace.name}}"
          -db-config-filtered-charset "utf8"
          -enable_semi_sync
          -enable_replication_reporter
          -orc_api_url "http://orchestrator/api"
          -orc_discover_interval "5m"
          -restore_from_backup
{{ include "format-flags-all" (tuple $.Values.backupFlags $0.extraFlags .extraFlags) | indent 10 }}
        END_OF_COMMAND
        )
  - name: mysql
    image: {{.image | default $0.image | quote}}
    volumeMounts:
      - name: syslog
        mountPath: /dev/log
      - name: vtdataroot
        mountPath: /vt/vtdataroot
    resources:
{{ toYaml (.mysqlResources | default $0.mysqlResources) | indent 6 }}
    securityContext:
      runAsUser: 999
    command:
      - bash
      - "-c"
      - |
        set -ex
        eval exec /vt/bin/mysqlctld $(cat <<END_OF_COMMAND
          -log_dir "$VTDATAROOT/tmp"
          -alsologtostderr
          -tablet_uid "{{$uid}}"
          -socket_file "$VTDATAROOT/mysqlctl.sock"
          -db-config-dba-uname "vt_dba"
          -db-config-dba-charset "utf8"
          -init_db_sql_file "$VTROOT/config/init_db.sql"
{{ include "format-flags-all" (tuple $0.mysqlctlExtraFlags .mysqlctlExtraFlags) | indent 10 }}
        END_OF_COMMAND
        )
    env:
      - name: EXTRA_MY_CNF
{{ if eq $controllerType "None" }}
        value: {{.extraMyCnf | default $0.extraMyCnf | quote}}
{{ else }}
        value: "/vt/vtdataroot/init/report-host.cnf:{{.extraMyCnf | default $0.extraMyCnf}}"
{{ end }}
volumes:
  - name: syslog
    hostPath: {path: /dev/log}
{{ if eq (.dataVolumeType | default $0.dataVolumeType) "EmptyDir" }}
  - name: vtdataroot
    emptyDir: {}
{{ end }}
  - name: certs
    hostPath: { path: {{$.Values.certsPath | quote}} }
{{- end -}}
{{- end -}}

# init-container to compute tablet UID.
# This converts the unique identity assigned by StatefulSet (pod name)
# into a 31-bit unsigned integer for use as a Vitess tablet UID.
{{- define "init-tablet-uid" -}}
{{- $image := index . 0 -}}
{{- $cell := index . 1 -}}
{
  "name": "init-tablet-uid",
  "image": {{$image | quote}},
  "securityContext": {"runAsUser": 999},
  "command": ["bash", "-c", "
    set -ex\n
    # Split pod name (via hostname) into prefix and ordinal index.\n
    hostname=$(hostname -s)\n
    [[ $hostname =~ ^(.+)-([0-9]+)$ ]] || exit 1\n
    pod_prefix=${BASH_REMATCH[1]}\n
    pod_index=${BASH_REMATCH[2]}\n
    # Prepend cell name since tablet UIDs must be globally unique.\n
    uid_name={{$cell.name | replace "_" "-" | lower}}-$pod_prefix\n
    # Take MD5 hash of cellname-podprefix.\n
    uid_hash=$(echo -n $uid_name | md5sum | awk \"{print \\$1}\")\n
    # Take first 24 bits of hash, convert to decimal.\n
    # Shift left 2 decimal digits, add in index.\n
    tablet_uid=$((16#${uid_hash:0:6} * 100 + $pod_index))\n
    # Save UID for other containers to read.\n
    mkdir -p $VTDATAROOT/init\n
    echo $tablet_uid > $VTDATAROOT/init/tablet-uid\n
    # Tell MySQL what hostname to report in SHOW SLAVE HOSTS.\n
    # Orchestrator looks there, so it should match -tablet_hostname above.\n
    echo report-host=$hostname.vttablet > $VTDATAROOT/init/report-host.cnf\n
  "],
  "volumeMounts": [
    {
      "name": "vtdataroot",
      "mountPath": "/vt/vtdataroot"
    }
  ]
}
{{- end -}}

# vttablet StatefulSet
{{- define "vttablet-stateful-set" -}}
{{- $ := index . 0 -}}
{{- $cell := index . 1 -}}
{{- $keyspace := index . 2 -}}
{{- $shard := index . 3 -}}
{{- $tablet := index . 4 -}}
{{- with $tablet.vttablet -}}
{{- $0 := $.Values.vttablet -}}
{{- $cellClean := include "clean-label" $cell.name -}}
{{- $keyspaceClean := include "clean-label" $keyspace.name -}}
{{- $shardClean := include "clean-label" $shard.name -}}
{{- $setName := printf "%s-%s-%s-%s" $cellClean $keyspaceClean $shardClean $tablet.type | lower -}}
{{- $uid := "$(cat $VTDATAROOT/init/tablet-uid)" }}
# vttablet StatefulSet
apiVersion: apps/v1beta1
kind: StatefulSet
metadata:
  name: {{$setName | quote}}
spec:
  serviceName: vttablet
  replicas: {{.replicas | default $0.replicas}}
  template:
    metadata:
      labels:
        app: vitess
        component: vttablet
        cell: {{$cellClean | quote}}
        keyspace: {{$keyspace.name | quote}}
        shard: {{$shardClean | quote}}
        type: {{$tablet.type | quote}}
      annotations:
        pod.alpha.kubernetes.io/initialized: "true"
        pod.beta.kubernetes.io/init-containers: '[
{{ include "init-vtdataroot" (.image | default $0.image) | indent 10 }},
{{ include "init-tablet-uid" (tuple (.image | default $0.image) $cell) | indent 10 }}
        ]'
    spec:
{{ include "vttablet-pod-spec" (tuple $ $cell $keyspace $shard $tablet $uid .) | indent 6 }}
{{ if eq (.dataVolumeType | default $0.dataVolumeType) "PersistentVolume" }}
  volumeClaimTemplates:
    - metadata:
        name: vtdataroot
        annotations:
{{ toYaml (.dataVolumeClaimAnnotations | default $0.dataVolumeClaimAnnotations) | indent 10 }}
      spec:
{{ toYaml (.dataVolumeClaimSpec | default $0.dataVolumeClaimSpec) | indent 8 }}
{{ end }}
{{- end -}}
{{- end -}}

# vttablet naked pod (not using StatefulSet)
{{- define "vttablet-pod" -}}
{{- $ := index . 0 -}}
{{- $cell := index . 1 -}}
{{- $keyspace := index . 2 -}}
{{- $shard := index . 3 -}}
{{- $shardClean := include "clean-label" $shard.name -}}
{{- $tablet := index . 4 -}}
{{- $uid := index . 5 -}}
{{- with $tablet.vttablet -}}
{{- $0 := $.Values.vttablet -}}
# vttablet
kind: Pod
apiVersion: v1
metadata:
  name: "vttablet-{{$uid}}"
  labels:
    app: vitess
    component: vttablet
    keyspace: {{$keyspace.name | quote}}
    shard: {{$shardClean | quote}}
    type: {{$tablet.type | quote}}
  annotations:
    pod.beta.kubernetes.io/init-containers: '[
{{ include "init-vtdataroot" (.image | default $0.image) | indent 6 }}
    ]'
spec:
{{ include "vttablet-pod-spec" (tuple $ $cell $keyspace $shard $tablet $uid .) | indent 2 }}
{{- end -}}
{{- end -}}

