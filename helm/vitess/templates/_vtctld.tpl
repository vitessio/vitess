###################################
# vtctld Service + Deployment
###################################
{{- define "vtctld" -}}
# set tuple values to more recognizable variables
{{- $topology := index . 0 -}}
{{- $cell := index . 1 -}}
{{- $defaultVtctld := index . 2 -}}
{{- $namespace := index . 3 -}}
{{- $config := index . 4 -}}

{{- with $cell.vtctld -}}

# define image to use
{{- $vitessTag := .vitessTag | default $defaultVtctld.vitessTag -}}
{{- $cellClean := include "clean-label" $cell.name -}}

###################################
# vtctld Service
###################################
kind: Service
apiVersion: v1
metadata:
  name: vtctld
  labels:
    component: vtctld
    app: vitess
spec:
  ports:
    - name: web
      port: 15000
    - name: grpc
      port: 15999
  selector:
    component: vtctld
    app: vitess
  type: {{.serviceType | default $defaultVtctld.serviceType}}
---
###################################
# vtctld Service + Deployment
###################################
apiVersion: apps/v1beta1
kind: Deployment
metadata:
  name: vtctld
spec:
  replicas: {{.replicas | default $defaultVtctld.replicas}}
  selector:
    matchLabels:
      app: vitess
      component: vtctld
  template:
    metadata:
      labels:
        app: vitess
        component: vtctld
    spec:
{{ include "pod-security" . | indent 6 }}
{{ include "vtctld-affinity" (tuple $cellClean $cell.region) | indent 6 }}
      containers:
        - name: vtctld
          image: vitess/vtctld:{{$vitessTag}}
          readinessProbe:
            httpGet:
              path: /debug/health
              port: 15000
            initialDelaySeconds: 30
            timeoutSeconds: 5
          livenessProbe:
            httpGet:
              path: /debug/status
              port: 15000
            initialDelaySeconds: 30
            timeoutSeconds: 5
          env:
{{ include "backup-env" $config.backup | indent 12 }}
          volumeMounts:
{{ include "backup-volumeMount" $config.backup | indent 12 }}
{{ include "user-secret-volumeMounts" $defaultVtctld.secrets | indent 12 }}
{{ include "user-secret-volumeMounts" .secrets | indent 12 }}
          resources:
{{ toYaml (.resources | default $defaultVtctld.resources) | indent 12 }}
          command:
            - bash
            - "-c"
            - |
              set -ex;

{{ include "backup-exec" $config.backup | indent 14 }}

              eval exec /vt/bin/vtctld $(cat <<END_OF_COMMAND
                -cell={{$cellClean | quote}}
                -web_dir="/vt/web/vtctld"
                -web_dir2="/vt/web/vtctld2/app"
                -workflow_manager_init
                -workflow_manager_use_election
                -logtostderr=true
                -stderrthreshold=0
                -port=15000
                -grpc_port=15999
                -service_map="grpc-vtctl"
                -topo_implementation="etcd2"
                -topo_global_server_address="etcd-global-client.{{ $namespace }}:2379"
                -topo_global_root=/vitess/global
{{ include "backup-flags" (tuple $config.backup "vtctld") | indent 16 }}
{{ include "format-flags-all" (tuple $defaultVtctld.extraFlags .extraFlags) | indent 16 }}
              END_OF_COMMAND
              )

      volumes:
{{ include "backup-volume" $config.backup | indent 8 }}
{{ include "user-secret-volumes" $defaultVtctld.secrets | indent 8 }}
{{ include "user-secret-volumes" .secrets | indent 8 }}

{{- end -}}
{{- end -}}

###################################
# vtctld-affinity sets node/pod affinities
###################################
{{- define "vtctld-affinity" -}}
# set tuple values to more recognizable variables
{{- $cellClean := index . 0 -}}
{{- $region := index . 1 -}}

{{ with $region }}
# affinity pod spec
affinity:
{{ include "node-affinity" $region | indent 2 }}
{{- end -}}

{{- end -}}
