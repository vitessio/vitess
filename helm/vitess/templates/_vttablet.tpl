###################################
# vttablet Service
###################################
{{ define "vttablet-service" -}}
# set tuple values to more recognizable variables
{{- $pmm := index . 0 }}
apiVersion: v1
kind: Service
metadata:
  name: vttablet
  labels:
    app: vitess
  annotations:
    service.alpha.kubernetes.io/tolerate-unready-endpoints: "true"
spec:
  publishNotReadyAddresses: true
  ports:
    - port: 15002
      name: web
    - port: 16002
      name: grpc
{{ if $pmm.enabled }}
    - port: 42001
      name: query-data
    - port: 42002
      name: mysql-metrics
{{ end }}
  clusterIP: None
  selector:
    app: vitess
    component: vttablet

{{- end -}}

###################################
# vttablet
###################################
{{ define "vttablet" -}}
# set tuple values to more recognizable variables
{{- $topology := index . 0 -}}
{{- $cell := index . 1 -}}
{{- $keyspace := index . 2 -}}
{{- $shard := index . 3 -}}
{{- $tablet := index . 4 -}}
{{- $defaultVttablet := index . 5 -}}
{{- $defaultVtctlclient := index . 6 -}}
{{- $namespace := index . 7 -}}
{{- $config := index . 8 -}}
{{- $pmm := index . 9 -}}
{{- $orc := index . 10 -}}

# sanitize inputs for labels
{{- $cellClean := include "clean-label" $cell.name -}}
{{- $keyspaceClean := include "clean-label" $keyspace.name -}}
{{- $shardClean := include "clean-label" $shard.name -}}
{{- $uid := "$(cat /vtdataroot/tabletdata/tablet-uid)" }}
{{- $setName := printf "%s-%s-%s-%s" $cellClean $keyspaceClean $shardClean $tablet.type | lower -}}
{{- $shardName := printf "%s-%s-%s" $cellClean $keyspaceClean $shardClean | lower -}}

{{- with $tablet.vttablet -}}

# define images to use
{{- $vitessTag := .vitessTag | default $defaultVttablet.vitessTag -}}
{{- $image := .image | default $defaultVttablet.image -}}
{{- $mysqlImage := .mysqlImage | default $defaultVttablet.mysqlImage -}}
{{- $mysqlImage := .mysqlImage | default $defaultVttablet.mysqlImage }}
---
###################################
# vttablet StatefulSet
###################################
apiVersion: apps/v1beta1
kind: StatefulSet
metadata:
  name: {{ $setName | quote }}
spec:
  serviceName: vttablet
  replicas: {{ .replicas | default $defaultVttablet.replicas }}
  podManagementPolicy: Parallel
  updateStrategy:
    type: RollingUpdate
  selector:
    matchLabels:
      app: vitess
      component: vttablet
      cell: {{ $cellClean | quote }}
      keyspace: {{ $keyspaceClean | quote }}
      shard: {{ $shardClean | quote }}
      type: {{ $tablet.type | quote }}
  template:
    metadata:
      labels:
        app: vitess
        component: vttablet
        cell: {{ $cellClean | quote }}
        keyspace: {{ $keyspaceClean | quote }}
        shard: {{ $shardClean | quote }}
        type: {{ $tablet.type | quote }}
    spec:
      terminationGracePeriodSeconds: 60000000
{{ include "pod-security" . | indent 6 }}
{{ include "vttablet-affinity" (tuple $cellClean $keyspaceClean $shardClean $cell.region) | indent 6 }}

      initContainers:
{{ include "init-mysql" (tuple $vitessTag $cellClean) | indent 8 }}
{{ include "init-vttablet" (tuple $vitessTag $cell $cellClean $namespace) | indent 8 }}

      containers:
{{ include "cont-mysql" (tuple $topology $cell $keyspace $shard $tablet $defaultVttablet $uid) | indent 8 }}
{{ include "cont-vttablet" (tuple $topology $cell $keyspace $shard $tablet $defaultVttablet $defaultVtctlclient $vitessTag $uid $namespace $config $orc) | indent 8 }}
{{ include "cont-logrotate" . | indent 8 }}
{{ include "cont-mysql-generallog" . | indent 8 }}
{{ include "cont-mysql-errorlog" . | indent 8 }}
{{ include "cont-mysql-slowlog" . | indent 8 }}
{{ if $pmm.enabled }}{{ include "cont-pmm-client" (tuple $pmm $namespace $keyspace) | indent 8 }}{{ end }}

      volumes:
        - name: vt
          emptyDir: {}
{{ include "backup-volume" $config.backup | indent 8 }}
{{ include "user-config-volume" (.extraMyCnf | default $defaultVttablet.extraMyCnf) | indent 8 }}
{{ include "user-secret-volumes" (.secrets | default $defaultVttablet.secrets) | indent 8 }}
{{ if $keyspace.pmm }}{{if $keyspace.pmm.config }}
        - name: config
          configMap:
            name: {{ $keyspace.pmm.config }}
{{ end }}{{ end }}

  volumeClaimTemplates:
    - metadata:
        name: vtdataroot
        annotations:
{{ toYaml (.dataVolumeClaimAnnotations | default $defaultVttablet.dataVolumeClaimAnnotations) | indent 10 }}
      spec:
{{ toYaml (.dataVolumeClaimSpec | default $defaultVttablet.dataVolumeClaimSpec) | indent 8 }}

---
###################################
# vttablet PodDisruptionBudget
###################################
apiVersion: policy/v1beta1
kind: PodDisruptionBudget
metadata:
  name: {{ $setName | quote }}
spec:
  maxUnavailable: 1
  selector:
    matchLabels:
      app: vitess
      component: vttablet
      cell: {{ $cellClean | quote }}
      keyspace: {{ $keyspaceClean | quote }}
      shard: {{ $shardClean | quote }}
      type: {{ $tablet.type | quote }}

# conditionally add cron job
{{ include "vttablet-backup-cron" (tuple $cellClean $keyspaceClean $shardClean $shardName $keyspace $shard $vitessTag $config.backup $namespace $defaultVtctlclient) }}

{{- end -}}
{{- end -}}

###################################
# init-container to copy binaries for mysql
###################################
{{ define "init-mysql" -}}
{{- $vitessTag := index . 0 -}}
{{- $cellClean := index . 1 }}

- name: "init-mysql"
  image: "vitess/mysqlctld:{{$vitessTag}}"
  imagePullPolicy: IfNotPresent
  volumeMounts:
    - name: vtdataroot
      mountPath: "/vtdataroot"
    - name: vt
      mountPath: "/vttmp"

  command: ["bash"]
  args:
    - "-c"
    - |
      set -ex
      # set up the directories vitess needs
      mkdir -p /vttmp/bin
      mkdir -p /vtdataroot/tabletdata

      # copy necessary assets to the volumeMounts
      cp /vt/bin/mysqlctld /vttmp/bin/
      cp /bin/busybox /vttmp/bin/
      cp -R /vt/config /vttmp/

      # make sure the log files exist
      touch /vtdataroot/tabletdata/error.log
      touch /vtdataroot/tabletdata/slow-query.log
      touch /vtdataroot/tabletdata/general.log

      # remove the old socket file if it is still around
      rm -f /vtdataroot/tabletdata/mysql.sock
      rm -f /vtdataroot/tabletdata/mysql.sock.lock

{{- end -}}

###################################
# init-container to set tablet uid + register tablet with global topo
# This converts the unique identity assigned by StatefulSet (pod name)
# into a 31-bit unsigned integer for use as a Vitess tablet UID.
###################################
{{ define "init-vttablet" -}}
{{- $vitessTag := index . 0 -}}
{{- $cell := index . 1 -}}
{{- $cellClean := index . 2 -}}
{{- $namespace := index . 3 }}

- name: init-vttablet
  image: "vitess/vtctl:{{$vitessTag}}"
  imagePullPolicy: IfNotPresent
  volumeMounts:
    - name: vtdataroot
      mountPath: "/vtdataroot"
  command: ["bash"]
  args:
    - "-c"
    - |
      set -ex
      # Split pod name (via hostname) into prefix and ordinal index.
      hostname=$(hostname -s)
      [[ $hostname =~ ^(.+)-([0-9]+)$ ]] || exit 1
      pod_prefix=${BASH_REMATCH[1]}
      pod_index=${BASH_REMATCH[2]}
      # Prepend cell name since tablet UIDs must be globally unique.
      uid_name={{$cell.name | replace "_" "-" | lower}}-$pod_prefix
      # Take MD5 hash of cellname-podprefix.
      uid_hash=$(echo -n $uid_name | md5sum | awk "{print \$1}")
      # Take first 24 bits of hash, convert to decimal.
      # Shift left 2 decimal digits, add in index.
      tablet_uid=$((16#${uid_hash:0:6} * 100 + $pod_index))
      # Save UID for other containers to read.
      echo $tablet_uid > /vtdataroot/tabletdata/tablet-uid
      # Tell MySQL what hostname to report in SHOW SLAVE HOSTS.
      echo report-host=$hostname.vttablet > /vtdataroot/tabletdata/report-host.cnf
      # Orchestrator looks there, so it should match -tablet_hostname above.

      # make sure that etcd is initialized
      eval exec /vt/bin/vtctl $(cat <<END_OF_COMMAND
        -topo_implementation="etcd2"
        -topo_global_root=/vitess/global
        -topo_global_server_address="etcd-global-client.{{ $namespace }}:2379"
        -logtostderr=true
        -stderrthreshold=0
        UpdateCellInfo
        -server_address="etcd-global-client.{{ $namespace }}:2379"
        {{ $cellClean | quote}}
      END_OF_COMMAND
      )

{{- end -}}

##########################
# main vttablet container
##########################
{{ define "cont-vttablet" -}}

{{- $topology := index . 0 -}}
{{- $cell := index . 1 -}}
{{- $keyspace := index . 2 -}}
{{- $shard := index . 3 -}}
{{- $tablet := index . 4 -}}
{{- $defaultVttablet := index . 5 -}}
{{- $defaultVtctlclient := index . 6 -}}
{{- $vitessTag := index . 7 -}}
{{- $uid := index . 8 -}}
{{- $namespace := index . 9 -}}
{{- $config := index . 10 -}}
{{- $orc := index . 11 -}}

{{- $cellClean := include "clean-label" $cell.name -}}
{{- with $tablet.vttablet }}

- name: vttablet
  image: "vitess/vttablet:{{$vitessTag}}"
  imagePullPolicy: IfNotPresent
  readinessProbe:
    httpGet:
      path: /debug/health
      port: 15002
    initialDelaySeconds: 60
    timeoutSeconds: 10
  livenessProbe:
    httpGet:
      path: /debug/status
      port: 15002
    initialDelaySeconds: 60
    timeoutSeconds: 10
  volumeMounts:
    - name: vtdataroot
      mountPath: "/vtdataroot"
{{ include "backup-volumeMount" $config.backup | indent 4 }}
{{ include "user-config-volumeMount" (.extraMyCnf | default $defaultVttablet.extraMyCnf) | indent 4 }}
{{ include "user-secret-volumeMounts" (.secrets | default $defaultVttablet.secrets) | indent 4 }}

  resources:
{{ toYaml (.resources | default $defaultVttablet.resources) | indent 6 }}
  ports:
    - name: web
      containerPort: 15002
    - name: grpc
      containerPort: 16002
  env:
{{ include "vitess-env" . | indent 4 }}
{{ include "backup-env" $config.backup | indent 4 }}

    - name: VT_DB_FLAVOR
      valueFrom:
        configMapKeyRef:
          name: vitess-cm
          key: db.flavor

  lifecycle:
    preStop:
      exec:
        command:
          - "bash"
          - "-c"
          - |
            set -x

            VTCTLD_SVC=vtctld.{{ $namespace }}:15999
            VTCTL_EXTRA_FLAGS=({{ include "format-flags-inline" $defaultVtctlclient.extraFlags }})

            master_alias_json=$(/vt/bin/vtctlclient ${VTCTL_EXTRA_FLAGS[@]} -server $VTCTLD_SVC GetShard {{ $keyspace.name }}/{{ $shard.name }})
            master_cell=$(jq -r '.master_alias.cell' <<< "$master_alias_json")
            master_uid=$(jq -r '.master_alias.uid' <<< "$master_alias_json")
            master_alias=$master_cell-$master_uid

            current_uid=$(cat /vtdataroot/tabletdata/tablet-uid)
            current_alias={{ $cell.name }}-$current_uid

            if [ $master_alias != $current_alias ]; then
                # since this isn't the master, there's no reason to reparent
                exit
            fi

            # TODO: add more robust health checks to make sure that we don't initiate a reparent
            # if there isn't a healthy enough replica to take over
            # - seconds behind master
            # - use GTID_SUBTRACT

            RETRY_COUNT=0
            MAX_RETRY_COUNT=100000
            hostname=$(hostname -s)

            # retry reparenting
            until [ $DONE_REPARENTING ]; do

{{ if $orc.enabled }}
              # tell orchestrator to not attempt a recovery for 10 seconds while we are in the middle of reparenting
              wget -q -S -O - "http://orchestrator.{{ $namespace }}/api/begin-downtime/$hostname.vttablet/3306/preStopHook/VitessPlannedReparent/10s"
{{ end }}

              # reparent before shutting down
              /vt/bin/vtctlclient ${VTCTL_EXTRA_FLAGS[@]} -server $VTCTLD_SVC PlannedReparentShard -keyspace_shard={{ $keyspace.name }}/{{ $shard.name }} -avoid_master=$current_alias

{{ if $orc.enabled }}
              # tell orchestrator to refresh its view of this tablet
              wget -q -S -O - "http://orchestrator.{{ $namespace }}/api/refresh/$hostname.vttablet/3306"

              # let orchestrator attempt recoveries now
              wget -q -S -O - "http://orchestrator.{{ $namespace }}/api/end-downtime/$hostname.vttablet/3306"
{{ end }}

              # if PlannedReparentShard succeeded, then don't retry
              if [ $? -eq 0 ]; then
                DONE_REPARENTING=true

              # if we've reached the max retry count, exit unsuccessfully
              elif [ $RETRY_COUNT -eq $MAX_RETRY_COUNT ]; then
                exit 1

              # otherwise, increment the retry count and sleep for 10 seconds
              else
                let RETRY_COUNT=RETRY_COUNT+1
                sleep 10
              fi

            done

            # delete the current tablet from topology. Not strictly necessary, but helps to prevent
            # edge cases where there are two masters
            /vt/bin/vtctlclient ${VTCTL_EXTRA_FLAGS[@]} -server $VTCTLD_SVC DeleteTablet $current_alias


{{ if $orc.enabled }}
            # tell orchestrator to forget the tablet, to prevent confusion / race conditions while the tablet restarts
            wget -q -S -O - "http://orchestrator.{{ $namespace }}/api/forget/$hostname.vttablet/3306"
{{ end }}

  command: ["bash"]
  args:
    - "-c"
    - |
      set -ex

{{ include "mycnf-exec" (.extraMyCnf | default $defaultVttablet.extraMyCnf) | indent 6 }}
{{ include "backup-exec" $config.backup | indent 6 }}

      eval exec /vt/bin/vttablet $(cat <<END_OF_COMMAND
        -topo_implementation="etcd2"
        -topo_global_server_address="etcd-global-client.{{ $namespace }}:2379"
        -topo_global_root=/vitess/global
        -logtostderr
        -port 15002
        -grpc_port 16002
        -service_map "grpc-queryservice,grpc-tabletmanager,grpc-updatestream"
        -tablet_dir "tabletdata"
        -tablet-path "{{ $cell.name }}-$(cat /vtdataroot/tabletdata/tablet-uid)"
        -tablet_hostname "$(hostname).vttablet"
        -init_keyspace {{ $keyspace.name | quote }}
        -init_shard {{ $shard.name | quote }}
        -init_tablet_type {{ $tablet.type | quote }}
        -health_check_interval "5s"
        -mysqlctl_socket "/vtdataroot/mysqlctl.sock"
        -enable_replication_reporter

{{ if $defaultVttablet.useKeyspaceNameAsDbName }}
        -init_db_name_override {{ $keyspace.name | quote }}
{{ end }}
{{ if $defaultVttablet.enableSemisync }}
        -enable_semi_sync
{{ end }}
{{ if $defaultVttablet.enableHeartbeat }}
        -heartbeat_enable
{{ end }}
{{ if $orc.enabled }}
        -orc_api_url "http://orchestrator.{{ $namespace }}/api"
        -orc_discover_interval "5m"
{{ end }}
{{ include "backup-flags" (tuple $config.backup "vttablet") | indent 8 }}
{{ include "format-flags-all" (tuple $defaultVttablet.extraFlags .extraFlags) | indent 8 }}
      END_OF_COMMAND
      )
{{- end -}}
{{- end -}}

##########################
# main mysql container
##########################
{{ define "cont-mysql" -}}

{{- $topology := index . 0 -}}
{{- $cell := index . 1 -}}
{{- $keyspace := index . 2 -}}
{{- $shard := index . 3 -}}
{{- $tablet := index . 4 -}}
{{- $defaultVttablet := index . 5 -}}
{{- $uid := index . 6 -}}

{{- with $tablet.vttablet }}

- name: mysql
  image: {{.mysqlImage | default $defaultVttablet.mysqlImage | quote}}
  imagePullPolicy: IfNotPresent
  readinessProbe:
    exec:
      command: ["mysqladmin", "ping", "-uroot", "--socket=/vtdataroot/tabletdata/mysql.sock"]
    initialDelaySeconds: 60
    timeoutSeconds: 10

  volumeMounts:
    - name: vtdataroot
      mountPath: /vtdataroot
    - name: vt
      mountPath: /vt
{{ include "user-config-volumeMount" (.extraMyCnf | default $defaultVttablet.extraMyCnf) | indent 4 }}
{{ include "user-secret-volumeMounts" (.secrets | default $defaultVttablet.secrets) | indent 4 }}
  resources:
{{ toYaml (.mysqlResources | default $defaultVttablet.mysqlResources) | indent 6 }}
  env:
{{ include "vitess-env" . | indent 4 }}

    - name: VT_DB_FLAVOR
      valueFrom:
        configMapKeyRef:
          name: vitess-cm
          key: db.flavor

  lifecycle:
    preStop:
      exec:
        command:
          - "bash"
          - "-c"
          - |
            set -x

            # block shutting down mysqlctld until vttablet shuts down first
            until [ $VTTABLET_GONE ]; do

              # poll every 5 seconds to see if vttablet is still running
              /vt/bin/busybox wget --spider localhost:15002/debug/vars

              if [ $? -ne 0 ]; then
                VTTABLET_GONE=true
              fi

              sleep 5
            done

  command: ["bash"]
  args:
    - "-c"
    - |
      set -ex
{{ include "mycnf-exec" (.extraMyCnf | default $defaultVttablet.extraMyCnf) | indent 6 }}
{{- if eq (.mysqlSize | default $defaultVttablet.mysqlSize) "test" }}
      export EXTRA_MY_CNF="$EXTRA_MY_CNF:/vt/config/mycnf/default-fast.cnf"
{{- end }}

      eval exec /vt/bin/mysqlctld $(cat <<END_OF_COMMAND
        -logtostderr=true
        -stderrthreshold=0
        -tablet_dir "tabletdata"
        -tablet_uid "{{$uid}}"
        -socket_file "/vtdataroot/mysqlctl.sock"
        -init_db_sql_file "/vt/config/init_db.sql"

      END_OF_COMMAND
      )

{{- end -}}
{{- end -}}

##########################
# run logrotate for all log files in /vtdataroot/tabletdata
##########################
{{ define "cont-logrotate" }}

- name: logrotate
  image: vitess/logrotate:helm-1.0.6
  imagePullPolicy: IfNotPresent
  volumeMounts:
    - name: vtdataroot
      mountPath: /vtdataroot

{{- end -}}

##########################
# redirect the error log file to stdout
##########################
{{ define "cont-mysql-errorlog" }}

- name: error-log
  image: vitess/logtail:helm-1.0.6
  imagePullPolicy: IfNotPresent

  env:
  - name: TAIL_FILEPATH
    value: /vtdataroot/tabletdata/error.log

  volumeMounts:
    - name: vtdataroot
      mountPath: /vtdataroot
{{- end -}}

##########################
# redirect the slow log file to stdout
##########################
{{ define "cont-mysql-slowlog" }}

- name: slow-log
  image: vitess/logtail:helm-1.0.6
  imagePullPolicy: IfNotPresent

  env:
  - name: TAIL_FILEPATH
    value: /vtdataroot/tabletdata/slow-query.log

  volumeMounts:
    - name: vtdataroot
      mountPath: /vtdataroot
{{- end -}}

##########################
# redirect the general log file to stdout
##########################
{{ define "cont-mysql-generallog" }}

- name: general-log
  image: vitess/logtail:helm-1.0.6
  imagePullPolicy: IfNotPresent

  env:
  - name: TAIL_FILEPATH
    value: /vtdataroot/tabletdata/general.log

  volumeMounts:
    - name: vtdataroot
      mountPath: /vtdataroot
{{- end -}}

###################################
# vttablet-affinity sets node/pod affinities
###################################
{{ define "vttablet-affinity" -}}
# set tuple values to more recognizable variables
{{- $cellClean := index . 0 -}}
{{- $keyspaceClean := index . 1 -}}
{{- $shardClean := index . 2 -}}
{{- $region := index . 3 }}

# affinity pod spec
affinity:
{{ include "node-affinity" $region | indent 2 }}

  podAffinity:
    preferredDuringSchedulingIgnoredDuringExecution:
    # prefer to be scheduled with same-cell vtgates
    - weight: 10
      podAffinityTerm:
        topologyKey: kubernetes.io/hostname
        labelSelector:
          matchLabels:
            app: "vitess"
            component: "vtgate"
            cell: {{ $cellClean | quote }}

  podAntiAffinity:
    preferredDuringSchedulingIgnoredDuringExecution:
    # strongly prefer to stay away from same shard vttablets
    - weight: 100
      podAffinityTerm:
        topologyKey: kubernetes.io/hostname
        labelSelector:
          matchLabels:
            app: "vitess"
            component: "vttablet"
            cell: {{ $cellClean | quote }}
            keyspace: {{ $keyspaceClean | quote }}
            shard: {{ $shardClean | quote }}

    # prefer to stay away from any vttablets
    - weight: 10
      podAffinityTerm:
        topologyKey: kubernetes.io/hostname
        labelSelector:
          matchLabels:
            app: "vitess"
            component: "vttablet"

{{- end -}}
