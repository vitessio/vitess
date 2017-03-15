{{- define "vtworker" -}}
{{- $ := index . 0 -}}
{{- $cell := index . 1 -}}
{{- with index . 2 -}}
{{- $0 := $.Values.vtworker -}}
# vtworker
kind: Service
apiVersion: v1
metadata:
  name: vtworker
  labels:
    component: vtworker
    app: vitess
spec:
  ports:
    - name: web
      port: 17032
    - name: grpc
      port 17033
  selector:
    component: vtworker
    app: vitess
  type: {{.serviceType | default $0.serviceType}}
---
apiVersion: extensions/v1beta1
kind: Deployment
metadata:
  name: vtworker
spec:
  replicas: {{.replicas | default $0.replicas}}
  template:
    metadata:
      labels:
        component: vtworker
        app: vitess
      annotations:
        pod.beta.kubernetes.io/init-containers: '[
{{ include "init-vtdataroot" (.image | default $0.image) | indent 10 }},
          {
            "name": "init-vtworker",
            "image": {{.image | default $0.image | quote}},
            "imagePullPolicy": "IfNotPresent"
          }
        ]'
    spec:
      containers:
        - name: vtworker
          image: {{.image | default $0.image | quote}}
          livenessProbe:
            httpGet:
              path: /debug/vars
              port: 17032
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
              eval exec /vt/bin/vtworker $(cat <<END_OF_COMMAND
                -cell={{$cell.name | quote}}
                -log_dir="$VTDATAROOT/tmp"
                -alsologtostderr
                -port=17032
                -grpc_port=17033
                -service_map="grpc-vtworker"
                -topo_implementation etcd
                -etcd_global_addrs="http://etcd-global:4001"
                -use_v3_resharding_mode
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
