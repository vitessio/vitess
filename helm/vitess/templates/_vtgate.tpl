{{- define "vtgate" -}}
{{- $ := index . 0 -}}
{{- $cell := index . 1 -}}
{{- with index . 2 -}}
{{- $0 := $.Values.vtgate -}}
# vtgate
kind: Service
apiVersion: v1
metadata:
  name: vtgate-{{$cell.name}}
  labels:
    component: vtgate
    cell: {{$cell.name}}
    app: vitess
spec:
  ports:
    - name: web
      port: 15001
    - name: grpc
      port: 15991
  selector:
    component: vtgate
    cell: {{$cell.name}}
    app: vitess
  type: {{.serviceType | default $0.serviceType}}
---
apiVersion: extensions/v1beta1
kind: Deployment
metadata:
  name: vtgate-{{$cell.name}}
spec:
  replicas: {{.replicas | default $0.replicas}}
  template:
    metadata:
      labels:
        component: vtgate
        cell: {{$cell.name}}
        app: vitess
      annotations:
        pod.beta.kubernetes.io/init-containers: '[
{{ include "init-vtdataroot" (.image | default $0.image) | indent 10 }}
        ]'
    spec:
      containers:
        - name: vtgate
          image: {{.image | default $0.image | quote}}
          livenessProbe:
            httpGet:
              path: /debug/vars
              port: 15001
            initialDelaySeconds: 30
            timeoutSeconds: 5
          volumeMounts:
            - name: syslog
              mountPath: /dev/log
            - name: vtdataroot
              mountPath: /vt/vtdataroot
          resources:
{{ toYaml (.resources | default $0.resources) | indent 12 }}
          securityContext:
            runAsUser: 999
          command:
            - bash
            - "-c"
            - |
              set -ex
              eval exec /vt/bin/vtgate $(cat <<END_OF_COMMAND
                -topo_implementation="etcd"
                -etcd_global_addrs="http://etcd-global:4001"
                -log_dir="$VTDATAROOT/tmp"
                -alsologtostderr
                -port=15001
                -grpc_port=15991
                -service_map="grpc-vtgateservice"
                -cells_to_watch={{$cell.name | quote}}
                -tablet_types_to_wait="MASTER,REPLICA"
                -gateway_implementation="discoverygateway"
                -cell={{$cell.name | quote}}
{{ include "format-flags-all" (tuple $0.extraFlags .extraFlags) | indent 16 }}
              END_OF_COMMAND
              )
      volumes:
        - name: syslog
          hostPath: {path: /dev/log}
        - name: vtdataroot
          emptyDir: {}
{{- end -}}
{{- end -}}

