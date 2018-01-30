###################################
# Master Orchestrator Service
###################################
{{- define "orchestrator" -}}
# set tuple values to more recognizable variables
{{- $orc := index . 0 -}}

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
    - name: raft
      port: 10008
      targetPort: 10008
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
      containers:
        - name: orchestrator
          image: {{ $orc.image }}
          ports:
            - containerPort: 3000
              name: web
              protocol: TCP
            - containerPort: 10008
              name: raft
              protocol: TCP
          livenessProbe:
            httpGet:
              path: /
              port: 3000
            initialDelaySeconds: 300
            timeoutSeconds: 10
          readinessProbe:
            httpGet:
              path: "/api/leader-check"
              port: 3000
            timeoutSeconds: 10
          volumeMounts:
            - name: config-volume
              mountPath: /conf/
          resources:
{{ toYaml ($orc.resources ) | indent 12 }}
          env:
            - name: MY_POD_NAME
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
            - name: VTCTLD_SERVER_PORT
              value: "15999"
          
          command: ["bash"]
          args:
            - "-c"
            - |
              set -ex

              # set the local config to advertise/bind its own service IP
              sed -i -e "s/POD_NAME/$MY_POD_NAME/g" /conf/orchestrator.conf.json

              cd /usr/local/orchestrator && ./orchestrator --config=/conf/orchestrator.conf.json http

        - name: recovery-log
          image: busybox
          command: ["/bin/sh"]
          args: ["-c", "tail -n+1 -F /tmp/recovery.log"]

        - name: audit-log
          image: busybox
          command: ["/bin/sh"]
          args: ["-c", "tail -n+1 -F /tmp/orchestrator-audit.log"]

      volumes:
        - name: config-volume
          configMap:
            name: orchestrator-cm
{{- end -}}

###################################
# Per StatefulSet Orchestrator Service
###################################
{{- define "orchestrator-statefulset-services" -}}
# set tuple values to more recognizable variables
{{- $orc := index . 0 -}}
{{- $i := index . 1 -}}

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