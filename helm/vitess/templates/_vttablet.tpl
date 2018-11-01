###################################
# vttablet Service
###################################
{{- define "vttablet-service" -}}
# set tuple values to more recognizable variables
{{- $pmm := index . 0 -}}
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
{{- define "vttablet" -}}
# set tuple values to more recognizable variables
{{- $topology := index . 0 -}}
{{- $cell := index . 1 -}}
{{- $keyspace := index . 2 -}}
{{- $shard := index . 3 -}}
{{- $tablet := index . 4 -}}
{{- $defaultVttablet := index . 5 -}}
{{- $namespace := index . 6 -}}
{{- $config := index . 7 -}}
{{- $pmm := index . 8 -}}
{{- $orc := index . 9 -}}
{{- $totalTabletCount := index . 10 -}}

# sanitize inputs to create tablet name
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
      terminationGracePeriodSeconds: 600
{{ include "pod-security" . | indent 6 }}
{{ include "vttablet-affinity" (tuple $cellClean $keyspaceClean $shardClean $cell.region) | indent 6 }}

      initContainers:
{{ include "init-mysql" (tuple $vitessTag $cellClean) | indent 8 }}
{{ include "init-vttablet" (tuple $vitessTag $cell $cellClean $namespace) | indent 8 }}

      containers:
{{ include "cont-mysql" (tuple $topology $cell $keyspace $shard $tablet $defaultVttablet $uid) | indent 8 }}
{{ include "cont-vttablet" (tuple $topology $cell $keyspace $shard $tablet $defaultVttablet $vitessTag $uid $namespace $config $orc $totalTabletCount) | indent 8 }}
{{ include "cont-mysql-errorlog" . | indent 8 }}
{{ include "cont-mysql-slowlog" . | indent 8 }}
{{ if $pmm.enabled }}{{ include "cont-pmm-client" (tuple $pmm $namespace) | indent 8 }}{{ end }}

      volumes:
        - name: vt
          emptyDir: {}
{{ include "backup-volume" $config.backup | indent 8 }}
{{ include "user-config-volume" $defaultVttablet.extraMyCnf | indent 8 }}

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

{{ if eq $tablet.type "replica" }}
---
###################################
# vttablet InitShardMaster Job
###################################
apiVersion: batch/v1
kind: Job
metadata:
  name: {{ $shardName }}-init-shard-master
spec:
  backoffLimit: 1
  template:
    spec:
      restartPolicy: OnFailure
      containers:
      - name: init-shard-master
        image: "vitess/vtctlclient:{{$vitessTag}}"

        command: ["bash"]
        args:
          - "-c"
          - |
            set -ex

            VTCTLD_SVC=vtctld.{{ $namespace }}:15999
            SECONDS=0
            TIMEOUT_SECONDS=600

            # poll every 5 seconds to see if vtctld is ready
            until vtctlclient -server $VTCTLD_SVC ListAllTablets {{ $cellClean }} > /dev/null 2>&1; do 
              if (( $SECONDS > $TIMEOUT_SECONDS )); then
                echo "timed out waiting for vtctlclient to be ready"
                exit 1
              fi
              sleep 5
            done

            until [ $TABLETS_READY ]; do
              # get all the tablets in the current cell
              cellTablets="$(vtctlclient -server $VTCTLD_SVC ListAllTablets {{ $cellClean }})"
              
              # filter to only the tablets in our current shard
              shardTablets=$( echo "$cellTablets" | awk 'substr( $5,1,{{ len $shardName }} ) == "{{ $shardName }}" {print $0}')

              # check for a master tablet from the ListAllTablets call
              masterTablet=$( echo "$shardTablets" | awk '$4 == "master" {print $1}')
              if [ $masterTablet ]; then
                  echo "'$masterTablet' is already the master tablet, exiting without running InitShardMaster"
                  exit
              fi

              # check for a master tablet from the GetShard call
              master_alias=$(vtctlclient -server $VTCTLD_SVC GetShard {{ $keyspace.name }}/{{ $shard.name }} | jq '.master_alias.uid')
              if [ $master_alias != "null" ]; then
                  echo "'$master_alias' is already the master tablet, exiting without running InitShardMaster"
                  exit
              fi

              # count the number of newlines for the given shard to get the tablet count
              tabletCount=$( echo "$shardTablets" | wc | awk '{print $1}')
              
              # check to see if the tablet count equals the expected tablet count
              if [ $tabletCount == {{ $totalTabletCount }} ]; then
                TABLETS_READY=true
              else
                if (( $SECONDS > $TIMEOUT_SECONDS )); then
                  echo "timed out waiting for tablets to be ready"
                  exit 1
                fi
                
                # wait 5 seconds for vttablets to continue getting ready
                sleep 5
              fi

            done

            # find the tablet id for the "-replica-0" stateful set for a given cell, keyspace and shard
            tablet_id=$( echo "$shardTablets" | awk 'substr( $5,1,{{ add (len $shardName) 10 }} ) == "{{ $shardName }}-replica-0" {print $1}')
            
            # initialize the shard master
            until vtctlclient -server $VTCTLD_SVC InitShardMaster -force {{ $keyspace.name }}/{{ $shard.name }} $tablet_id; do 
              if (( $SECONDS > $TIMEOUT_SECONDS )); then
                echo "timed out waiting for InitShardMaster to succeed"
                exit 1
              fi
              sleep 5
            done
            
{{- end -}}

{{- end -}}
{{- end -}}

###################################
# init-container to copy binaries for mysql
###################################
{{- define "init-mysql" -}}
{{- $vitessTag := index . 0 -}}
{{- $cellClean := index . 1 -}}

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

{{- end -}}

###################################
# init-container to set tablet uid + register tablet with global topo
# This converts the unique identity assigned by StatefulSet (pod name)
# into a 31-bit unsigned integer for use as a Vitess tablet UID.
###################################
{{- define "init-vttablet" -}}
{{- $vitessTag := index . 0 -}}
{{- $cell := index . 1 -}}
{{- $cellClean := index . 2 -}}
{{- $namespace := index . 3 -}}

- name: init-vttablet
  image: "vitess/vtctl:{{$vitessTag}}"
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
{{- define "cont-vttablet" -}}

{{- $topology := index . 0 -}}
{{- $cell := index . 1 -}}
{{- $keyspace := index . 2 -}}
{{- $shard := index . 3 -}}
{{- $tablet := index . 4 -}}
{{- $defaultVttablet := index . 5 -}}
{{- $vitessTag := index . 6 -}}
{{- $uid := index . 7 -}}
{{- $namespace := index . 8 -}}
{{- $config := index . 9 -}}
{{- $orc := index . 10 -}}
{{- $totalTabletCount := index . 11 -}}

{{- $cellClean := include "clean-label" $cell.name -}}
{{- with $tablet.vttablet -}}

- name: vttablet
  image: "vitess/vttablet:{{$vitessTag}}"
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
{{ include "user-config-volumeMount" $defaultVttablet.extraMyCnf | indent 4 }}

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

            master_alias_json=$(/vt/bin/vtctlclient -server $VTCTLD_SVC GetShard {{ $keyspace.name }}/{{ $shard.name }})
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
            MAX_RETRY_COUNT=5

            # retry reparenting
            until [ $DONE_REPARENTING ]; do

              # reparent before shutting down
              /vt/bin/vtctlclient -server $VTCTLD_SVC PlannedReparentShard -keyspace_shard={{ $keyspace.name }}/{{ $shard.name }} -avoid_master=$current_alias

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
            /vt/bin/vtctlclient -server $VTCTLD_SVC DeleteTablet $current_alias

  command: ["bash"]
  args:
    - "-c"
    - |
      set -ex

{{ include "mycnf-exec" $defaultVttablet.extraMyCnf | indent 6 }}
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
      END_OF_COMMAND
      )
{{- end -}}
{{- end -}}

##########################
# main mysql container
##########################
{{- define "cont-mysql" -}}

{{- $topology := index . 0 -}}
{{- $cell := index . 1 -}}
{{- $keyspace := index . 2 -}}
{{- $shard := index . 3 -}}
{{- $tablet := index . 4 -}}
{{- $defaultVttablet := index . 5 -}}
{{- $uid := index . 6 -}}

{{- with $tablet.vttablet -}}

- name: mysql
  image: {{.mysqlImage | default $defaultVttablet.mysqlImage | quote}}
  imagePullPolicy: Always
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
{{ include "user-config-volumeMount" $defaultVttablet.extraMyCnf | indent 4 }}
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

{{ include "mycnf-exec" $defaultVttablet.extraMyCnf | indent 6 }}

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
# redirect the error log file to stdout
##########################
{{- define "cont-mysql-errorlog" -}}

- name: error-log
  image: busybox
  command: ["/bin/sh"]
  args: ["-c", "tail -n+1 -F /vtdataroot/tabletdata/error.log"]
  volumeMounts:
    - name: vtdataroot
      mountPath: /vtdataroot
{{- end -}}

##########################
# redirect the slow log file to stdout
##########################
{{- define "cont-mysql-slowlog" -}}

- name: slow-log
  image: busybox
  command: ["/bin/sh"]
  args: ["-c", "tail -n+1 -F /vtdataroot/tabletdata/slow.log"]
  volumeMounts:
    - name: vtdataroot
      mountPath: /vtdataroot
{{- end -}}

###################################
# vttablet-affinity sets node/pod affinities
###################################
{{- define "vttablet-affinity" -}}
# set tuple values to more recognizable variables
{{- $cellClean := index . 0 -}}
{{- $keyspaceClean := index . 1 -}}
{{- $shardClean := index . 2 -}}
{{- $region := index . 3 -}}

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
