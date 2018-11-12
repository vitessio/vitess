###################################
# Master Orchestrator Service
###################################
{{- define "orchestrator" -}}
# set tuple values to more recognizable variables
{{- $orc := index . 0 -}}
{{- $defaultVtctlclient := index . 1 }}

apiVersion: v1
kind: Service
metadata:
  name: orchestrator
  labels:
    app: vitess
    component: orchestrator
spec:
  ports:
    - name: web
      port: 80
      targetPort: 3000
  selector:
    app: vitess
    component: orchestrator
  type: ClusterIP

---
###################################
# Headless Orchestrator Service
###################################
apiVersion: v1
kind: Service
metadata:
  name: orchestrator-headless
  annotations:
    service.alpha.kubernetes.io/tolerate-unready-endpoints: "true"
  labels:
    app: vitess
    component: orchestrator
spec:
  clusterIP: None
  ports:
    - name: web
      port: 80
      targetPort: 3000
  selector:
    component: orchestrator
    app: vitess

---

###################################
# Orchestrator StatefulSet
###################################
apiVersion: apps/v1beta1
kind: StatefulSet
metadata:
  name: orchestrator
spec:
  serviceName: orchestrator-headless
  replicas: {{ $orc.replicas }}
  podManagementPolicy: Parallel
  updateStrategy:
    type: RollingUpdate
  selector:
    matchLabels:
      app: vitess
      component: orchestrator
  template:
    metadata:
      labels:
        app: vitess
        component: orchestrator
    spec:
      affinity:
        podAntiAffinity:
          preferredDuringSchedulingIgnoredDuringExecution:
          # strongly prefer to stay away from other orchestrators
          - weight: 100
            podAffinityTerm:
              topologyKey: kubernetes.io/hostname
              labelSelector:
                matchLabels:
                  app: "vitess"
                  component: "orchestrator"

      initContainers:
{{ include "init-orchestrator" $orc | indent 8 }}

      containers:
        - name: orchestrator
          image: {{ $orc.image | quote }}
          imagePullPolicy: Always
          ports:
            - containerPort: 3000
              name: web
              protocol: TCP
            - containerPort: 10008
              name: raft
              protocol: TCP
          livenessProbe:
            httpGet:
              path: /api/lb-check
              port: 3000
            initialDelaySeconds: 300
            timeoutSeconds: 10
          readinessProbe:
            httpGet:
              path: "/api/raft-health"
              port: 3000
            timeoutSeconds: 10

          resources:
{{ toYaml ($orc.resources) | indent 12 }}

          volumeMounts:
            - name: config-shared
              mountPath: /conf/
            - name: tmplogs
              mountPath: /tmp
{{ include "user-secret-volumeMounts" $defaultVtctlclient.secrets | indent 12 }}
          env:
            - name: VTCTLD_SERVER_PORT
              value: "15999"

        - name: recovery-log
          image: busybox
          command: ["/bin/sh"]
          args: ["-c", "tail -n+1 -F /tmp/recovery.log"]
          volumeMounts:
            - name: tmplogs
              mountPath: /tmp

        - name: audit-log
          image: busybox
          command: ["/bin/sh"]
          args: ["-c", "tail -n+1 -F /tmp/orchestrator-audit.log"]
          volumeMounts:
            - name: tmplogs
              mountPath: /tmp

      volumes:
        - name: config-map
          configMap:
            name: orchestrator-cm
        - name: config-shared
          emptyDir: {}
        - name: tmplogs
          emptyDir: {}
{{ include "user-secret-volumes" $defaultVtctlclient.secrets | indent 8 }}

{{- end -}}

###################################
# Per StatefulSet Orchestrator Service
###################################
{{- define "orchestrator-statefulset-service" -}}
# set tuple values to more recognizable variables
{{- $orc := index . 0 -}}
{{- $i := index . 1 }}

apiVersion: v1
kind: Service
metadata:
  name: orchestrator-{{ $i }}
  annotations:
    service.alpha.kubernetes.io/tolerate-unready-endpoints: "true"
  labels:
    app: vitess
    component: orchestrator
spec:
  ports:
    - name: web
      port: 80
      targetPort: 3000
    - name: raft
      port: 10008
      targetPort: 10008
  selector:
    component: orchestrator
    app: vitess
    # this should be auto-filled by kubernetes
    statefulset.kubernetes.io/pod-name: "orchestrator-{{ $i }}"

{{- end -}}

###################################
# init-container to copy and sed
# Orchestrator config from ConfigMap
###################################
{{- define "init-orchestrator" -}}
{{- $orc := . -}}

- name: init-orchestrator
  image: {{ $orc.image | quote }}
  volumeMounts:
    - name: config-map
      mountPath: /conftmp/
    - name: config-shared
      mountPath: /conf/
  env:
    - name: MY_POD_NAME
      valueFrom:
        fieldRef:
          fieldPath: metadata.name

  command: ["bash"]
  args:
    - "-c"
    - |
      set -ex

      # make a copy of the config map file before editing it locally
      cp /conftmp/orchestrator.conf.json /conf/orchestrator.conf.json

      # set the local config to advertise/bind its own service IP
      sed -i -e "s/POD_NAME/$MY_POD_NAME/g" /conf/orchestrator.conf.json

{{- end -}}
