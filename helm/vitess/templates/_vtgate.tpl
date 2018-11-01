###################################
# vtgate Service + Deployment
###################################
{{- define "vtgate" -}}
# set tuple values to more recognizable variables
{{- $topology := index . 0 -}}
{{- $cell := index . 1 -}}
{{- $defaultVtgate := index . 2 -}}
{{- $namespace := index . 3 -}}

{{- with $cell.vtgate -}}

# define image to use
{{- $vitessTag := .vitessTag | default $defaultVtgate.vitessTag -}}
{{- $cellClean := include "clean-label" $cell.name -}}

###################################
# vtgate Service
###################################
kind: Service
apiVersion: v1
metadata:
  name: vtgate-{{ $cellClean }}
  labels:
    component: vtgate
    cell: {{ $cellClean }}
    app: vitess
spec:
  ports:
    - name: web
      port: 15001
    - name: grpc
      port: 15991
{{ if $cell.mysqlProtocol.enabled }}
    - name: mysql
      port: 3306
{{ end }}
  selector:
    component: vtgate
    cell: {{ $cellClean }}
    app: vitess
  type: {{.serviceType | default $defaultVtgate.serviceType}}
---
###################################
# vtgate Deployment
###################################
apiVersion: apps/v1beta1
kind: Deployment
metadata:
  name: vtgate-{{ $cellClean }}
spec:
  replicas: {{.replicas | default $defaultVtgate.replicas}}
  selector:
    matchLabels:
      app: vitess
      component: vtgate
      cell: {{ $cellClean }}
  template:
    metadata:
      labels:
        app: vitess
        component: vtgate
        cell: {{ $cellClean }}
    spec:
{{ include "pod-security" . | indent 6 }}
{{ include "vtgate-affinity" (tuple $cellClean $cell.region) | indent 6 }}

{{ if $cell.mysqlProtocol.enabled }}
      initContainers:
{{ include "init-mysql-creds" (tuple $vitessTag $cell) | indent 8 }}
{{ end }}

      containers:
        - name: vtgate
          image: vitess/vtgate:{{$vitessTag}}
          readinessProbe:
            httpGet:
              path: /debug/health
              port: 15001
            initialDelaySeconds: 30
            timeoutSeconds: 5
          livenessProbe:
            httpGet:
              path: /debug/status
              port: 15001
            initialDelaySeconds: 30
            timeoutSeconds: 5
          volumeMounts:
            - name: creds
              mountPath: "/mysqlcreds"
{{ include "user-secret-volumeMounts" $defaultVtgate.secrets | indent 12 }}
{{ include "user-secret-volumeMounts" .secrets | indent 12 }}
          resources:
{{ toYaml (.resources | default $defaultVtgate.resources) | indent 12 }}

          command:
            - bash
            - "-c"
            - |
              set -ex

              eval exec /vt/bin/vtgate $(cat <<END_OF_COMMAND
                -topo_implementation=etcd2
                -topo_global_server_address="etcd-global-client.{{ $namespace }}:2379"
                -topo_global_root=/vitess/global
                -logtostderr=true
                -stderrthreshold=0
                -port=15001
                -grpc_port=15991
{{ if $cell.mysqlProtocol.enabled }}
                -mysql_server_port=3306
                -mysql_auth_server_static_file="/mysqlcreds/creds.json"
{{ end }}
                -service_map="grpc-vtgateservice"
                -cells_to_watch={{$cell.name | quote}}
                -tablet_types_to_wait="MASTER,REPLICA"
                -gateway_implementation="discoverygateway"
                -cell={{$cell.name | quote}}
{{ include "format-flags-all" (tuple $defaultVtgate.extraFlags .extraFlags) | indent 16 }}
              END_OF_COMMAND
              )
      volumes:
        - name: creds
          emptyDir: {}
{{ include "user-secret-volumes" $defaultVtgate.Secrets | indent 8 }}
{{ include "user-secret-volumes" .secrets | indent 8 }}
---
###################################
# vtgate PodDisruptionBudget
###################################
apiVersion: policy/v1beta1
kind: PodDisruptionBudget
metadata:
  name: vtgate-{{ $cellClean }}
spec:
  maxUnavailable: 1
  selector:
    matchLabels:
      app: vitess
      component: vtgate
      cell: {{ $cellClean }}

{{ $maxReplicas := .maxReplicas | default .replicas }}
{{ if gt $maxReplicas .replicas }}
###################################
# optional HPA for vtgate
###################################
---
apiVersion: autoscaling/v2beta1
kind: HorizontalPodAutoscaler
metadata:
  name: vtgate-{{ $cellClean }}
spec:
  scaleTargetRef:
    apiVersion: apps/v1beta1
    kind: Deployment
    name: vtgate-{{ $cellClean }}
  minReplicas: {{ .replicas }}
  maxReplicas: {{ $maxReplicas }}
  metrics:
  - type: Resource
    resource:
      name: cpu
      targetAverageUtilization: 70
{{- end -}}

{{- end -}}
{{- end -}}

###################################
# vtgate-affinity sets node/pod affinities
###################################
{{- define "vtgate-affinity" -}}
# set tuple values to more recognizable variables
{{- $cellClean := index . 0 -}}
{{- $region := index . 1 -}}

# affinity pod spec
affinity:
{{ include "node-affinity" $region | indent 2 }}

  podAffinity:
    preferredDuringSchedulingIgnoredDuringExecution:
    # prefer to be scheduled with same-cell vttablets
    - weight: 10
      podAffinityTerm:
        topologyKey: kubernetes.io/hostname
        labelSelector:
          matchLabels:
            app: "vitess"
            component: "vttablet"
            cell: {{ $cellClean | quote }}

  podAntiAffinity:
    preferredDuringSchedulingIgnoredDuringExecution:
    # prefer to stay away from other same-cell vtgates
    - weight: 10
      podAffinityTerm:
        topologyKey: kubernetes.io/hostname
        labelSelector:
          matchLabels:
            app: "vitess"
            component: "vtgate"
            cell: {{ $cellClean | quote }}

{{- end -}}

###################################
# init-container to set mysql credentials file
# it loops through the users and pulls out their
# respective passwords from mounted secrets
###################################
{{- define "init-mysql-creds" -}}
{{- $vitessTag := index . 0 -}}
{{- $cell := index . 1 -}}

{{- with $cell.mysqlProtocol -}}

- name: init-mysql-creds
  image: "vitess/vtgate:{{$vitessTag}}"
  volumeMounts:
    - name: creds
      mountPath: "/mysqlcreds"
  env:
    - name: MYSQL_PASSWORD
      valueFrom:
        secretKeyRef:
          name: {{ .passwordSecret }}
          key: password

  command: ["bash"]
  args:
    - "-c"
    - |
      set -ex
      creds=$(cat <<END_OF_COMMAND
      {
        "{{ .username }}": [
          {
            "UserData": "{{ .username }}",
            "Password": "$MYSQL_PASSWORD"
          }
        ],
        "vt_appdebug": []
      }
      END_OF_COMMAND
      )
      echo $creds > /mysqlcreds/creds.json

{{- end -}}
{{- end -}}
