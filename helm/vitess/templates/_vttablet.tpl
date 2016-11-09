{{- define "vttablet" -}}
{{- $ := index . 0 -}}
{{- $cell := index . 1 -}}
{{- $keyspace := index . 2 -}}
{{- $shard := index . 3 -}}
{{- $tablet := index . 4 -}}
{{- $index := index . 5 -}}
{{- with index . 6 -}}
{{- $0 := $.Values.vttablet -}}
{{- $uid := add $tablet.uidBase $index -}}
{{- $alias := printf "%s-%d" $cell.name $uid -}}
# vttablet
kind: Pod
apiVersion: v1
metadata:
  name: "vttablet-{{$uid}}"
  labels:
    component: vttablet
    keyspace: {{$keyspace.name | quote}}
    shard: {{$shard.name | quote}}
    tablet: {{$alias | quote}}
    app: vitess
spec:
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
{{ toYaml (.resources | default $0.resources) | indent 8 }}
      ports:
        - name: web
          containerPort: 15002
        - name: grpc
          containerPort: 16002
      command:
        - bash
        - "-c"
        - |
          set -ex
          mkdir -p $VTDATAROOT/tmp
          chown -R vitess /vt

          exec su -p -c "$(tr '\n' ' ' <<END_OF_COMMAND
            exec /vt/bin/vttablet
              -topo_implementation "etcd"
              -etcd_global_addrs "http://etcd-global:4001"
              -log_dir "$VTDATAROOT/tmp"
              -alsologtostderr
              -port 15002
              -grpc_port 16002
              -service_map "grpc-queryservice,grpc-tabletmanager,grpc-updatestream"
              -tablet-path {{$alias | quote}}
              -tablet_hostname "$(hostname -i)"
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
{{ include "format-flags-all" (tuple $.Values.backupFlags $0.extraFlags .extraFlags) | indent 14 }}
          END_OF_COMMAND
          )" vitess
    - name: mysql
      image: {{.image | default $0.image | quote}}
      volumeMounts:
        - name: syslog
          mountPath: /dev/log
        - name: vtdataroot
          mountPath: /vt/vtdataroot
      resources:
{{ toYaml (.mysqlResources | default $0.mysqlResources) | indent 8 }}
      command:
        - sh
        - "-c"
        - |
          set -ex
          mkdir -p $VTDATAROOT/tmp
          chown -R vitess /vt

          exec su -p -c "$(tr '\n' ' ' <<END_OF_COMMAND
            exec /vt/bin/mysqlctld
              -log_dir "$VTDATAROOT/tmp"
              -alsologtostderr
              -tablet_uid {{$uid}}
              -socket_file "$VTDATAROOT/mysqlctl.sock"
              -db-config-app-uname "vt_app"
              -db-config-app-dbname "vt_{{$keyspace.name}}"
              -db-config-app-charset "utf8"
              -db-config-allprivs-uname "vt_allprivs"
              -db-config-allprivs-dbname "vt_{{$keyspace.name}}"
              -db-config-allprivs-charset "utf8"
              -db-config-dba-uname "vt_dba"
              -db-config-dba-dbname "vt_{{$keyspace.name}}"
              -db-config-dba-charset "utf8"
              -db-config-repl-uname "vt_repl"
              -db-config-repl-dbname "vt_{{$keyspace.name}}"
              -db-config-repl-charset "utf8"
              -db-config-filtered-uname "vt_filtered"
              -db-config-filtered-dbname "vt_{{$keyspace.name}}"
              -db-config-filtered-charset "utf8"
              -init_db_sql_file "$VTROOT/config/init_db.sql"
{{ include "format-flags-all" (tuple $0.mysqlctlExtraFlags .mysqlctlExtraFlags) | indent 14 }}
          END_OF_COMMAND
          )" vitess
      env:
        - name: EXTRA_MY_CNF
          value: {{.extraMyCnf | default $0.extraMyCnf | quote}}
  volumes:
    - name: syslog
      hostPath: {path: /dev/log}
    - name: vtdataroot
{{ toYaml (.dataVolume | default $0.dataVolume) | indent 6 }}
    - name: certs
      hostPath: { path: {{$.Values.certsPath | quote}} }
{{- end -}}
{{- end -}}

