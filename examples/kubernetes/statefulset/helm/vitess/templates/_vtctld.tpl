{{- define "vtctld" -}}
{{- $ := index . 0 -}}
{{- $cell := index . 1 -}}
{{- with index . 2 -}}
{{- $0 := $.Values.vtctld -}}
# vtctld
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
  type: {{.serviceType | default $0.serviceType}}
---
apiVersion: extensions/v1beta1
kind: Deployment
metadata:
  name: vtctld
spec:
  replicas: {{.replicas | default $0.replicas}}
  template:
    metadata:
      labels:
        component: vtctld
        app: vitess
      annotations:
        pod.beta.kubernetes.io/init-containers: '[
{{ include "init-vtdataroot" (.image | default $0.image) | indent 10 }},
          {
            "name": "init-vtctld",
            "image": {{.image | default $0.image | quote}},
            "imagePullPolicy": "IfNotPresent",
            "command": ["bash", "-c", "
              set -ex\n
              rm -rf /vt/web/*\n
              cp -R $VTTOP/web/* /vt/web/\n
              cp /mnt/config/config.js /vt/web/vtctld/\n
            "],
            "volumeMounts": [
              {
                "name": "config",
                "mountPath": "/mnt/config"
              },
              {
                "name": "web",
                "mountPath": "/vt/web"
              }
            ]
          }
        ]'
    spec:
      containers:
        - name: vtctld
          image: {{.image | default $0.image | quote}}
          livenessProbe:
            httpGet:
              path: /debug/vars
              port: 15000
            initialDelaySeconds: 30
            timeoutSeconds: 5
          volumeMounts:
            - name: syslog
              mountPath: /dev/log
            - name: vtdataroot
              mountPath: /vt/vtdataroot
            - name: web
              mountPath: /vt/web
            - name: certs
              readOnly: true
              # Mount root certs from the host OS into the location
              # expected for our container OS (Debian):
              mountPath: /etc/ssl/certs/ca-certificates.crt
          resources:
{{ toYaml (.resources | default $0.resources) | indent 12 }}
          securityContext:
            runAsUser: 999
          command:
            - bash
            - "-c"
            - |
              set -ex
              eval exec /vt/bin/vtctld $(cat <<END_OF_COMMAND
                -cell={{$cell.name | quote}}
                -web_dir="/vt/web/vtctld"
                -web_dir2="/vt/web/vtctld2/app"
                -workflow_manager_init
                -workflow_manager_use_election
                -log_dir="$VTDATAROOT/tmp"
                -alsologtostderr
                -port=15000
                -grpc_port=15999
                -service_map="grpc-vtctl"
                -topo_implementation="etcd"
                -etcd_global_addrs="http://etcd-global:4001"
{{ include "format-flags-all" (tuple $.Values.backupFlags $0.extraFlags .extraFlags) | indent 16 }}
              END_OF_COMMAND
              )
      volumes:
        - name: syslog
          hostPath: {path: /dev/log}
        - name: vtdataroot
          emptyDir: {}
        - name: certs
          hostPath: { path: {{$.Values.certsPath | quote}} }
        - name: web
          emptyDir: {}
        - name: config
          configMap:
            name: vtctld
---
# vtctld ConfigMap
apiVersion: v1
kind: ConfigMap
metadata:
  name: vtctld
data:
  config.js: |
{{ $.Files.Get "vtctld-config.js" | indent 4 }}
{{- end -}}
{{- end -}}

