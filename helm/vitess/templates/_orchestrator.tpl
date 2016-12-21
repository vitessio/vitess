{{- define "orchestrator" -}}
{{- $ := index . 0 -}}
{{- $cell := index . 1 -}}
{{- with index . 2 -}}
{{- $0 := $.Values.orchestrator -}}
{{- $dataVolumeType := .dataVolumeType | default $0.dataVolumeType -}}
# Orchestrator service
apiVersion: v1
kind: Service
metadata:
  name: orchestrator
  labels:
    component: orchestrator
    app: vitess
spec:
  ports:
    - port: 80
      targetPort: 3000
  selector:
    component: orchestrator
    app: vitess
{{ if eq $dataVolumeType "PersistentVolume" }}
---
# Orchestrator persistent volume claim
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: orchestrator-data
  annotations:
{{ toYaml (.dataVolumeClaimAnnotations | default $0.dataVolumeClaimAnnotations) | indent 4 }}
spec:
{{ toYaml (.dataVolumeClaimSpec | default $0.dataVolumeClaimSpec) | indent 2 }}
{{ end }}
---
# Orchestrator replication controller
apiVersion: extensions/v1beta1
kind: Deployment
metadata:
  name: orchestrator
spec:
  replicas: {{.replicas | default $0.replicas}}
  template:
    metadata:
      labels:
        component: orchestrator
        app: vitess
      annotations:
        pod.beta.kubernetes.io/init-containers: '[
          {
            "name": "init-mysql",
            "image": {{.image | default $0.image | quote}},
            "imagePullPolicy": "IfNotPresent",
            "command": ["bash", "-c", "
              set -ex\n
              rm -rf /mnt/data/lost+found\n
              if [[ ! -d /mnt/data/mysql ]]; then\n
                cp -R /var/lib/mysql/* /mnt/data/\n
              fi\n
              chown -R mysql:mysql /mnt/data\n
            "],
            "volumeMounts": [
              {"name": "data", "mountPath": "/mnt/data"}
            ]
          }
        ]'
    spec:
      containers:
        - name: orchestrator
          image: {{.image | default $0.image | quote}}
          command:
            - bash
            - "-c"
            - |
              set -x
              until mysqladmin -h 127.0.0.1 ping; do sleep 1; done
              exec orchestrator http
          livenessProbe:
            httpGet:
              path: "/"
              port: 3000
            initialDelaySeconds: 300
            timeoutSeconds: 30
          readinessProbe:
            httpGet:
              path: "/"
              port: 3000
            timeoutSeconds: 10
          volumeMounts:
            - mountPath: /orc/conf
              name: config
          resources:
{{ toYaml (.resources | default $0.resources) | indent 12 }}
        - name: mysql
          image: {{.image | default $0.image | quote}}
          volumeMounts:
            - mountPath: /var/lib/mysql
              name: data
          livenessProbe:
            exec:
              command: ["mysqladmin", "ping"]
            initialDelaySeconds: 60
            timeoutSeconds: 10
          resources:
{{ toYaml (.mysqlResources | default $0.mysqlResources) | indent 12 }}
          command: ["mysqld"]
      volumes:
        - name: config
          configMap:
            name: orchestrator
        - name: data
{{ if eq $dataVolumeType "PersistentVolume" }}
          persistentVolumeClaim:
            claimName: orchestrator-data
{{ else }}
          emptyDir: {}
{{ end }}
---
# Orchestrator ConfigMap
apiVersion: v1
kind: ConfigMap
metadata:
  name: orchestrator
data:
  orchestrator.conf.json: |
{{ $.Files.Get "orchestrator.conf.json" | indent 4 }}
{{- end -}}
{{- end -}}

