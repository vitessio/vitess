{{- define "orchestrator" -}}
{{- $ := index . 0 -}}
{{- $cell := index . 1 -}}
{{- with index . 2 -}}
{{- $0 := $.Values.orchestrator -}}
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
    spec:
      containers:
        - name: orchestrator
          image: {{.image | default $0.image | quote}}
          livenessProbe:
            httpGet:
              path: "/"
              port: 3000
            initialDelaySeconds: 300
            timeoutSeconds: 30
          volumeMounts:
            - mountPath: /orc/conf
              name: config
          resources:
{{ toYaml (.resources | default $0.resources) | indent 12 }}
        - name: mysql
          image: {{.image | default $0.image | quote}}
          resources:
{{ toYaml (.mysqlResources | default $0.mysqlResources) | indent 12 }}
          command: ["mysqld"]
      volumes:
        - name: config
          configMap:
            name: orchestrator
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

