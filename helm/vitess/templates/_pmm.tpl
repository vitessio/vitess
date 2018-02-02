###################################
# pmm Service + Deployment
###################################
{{- define "pmm" -}}
# set tuple values to more recognizable variables
{{- $pmm := index . 0 -}}
{{- $namespace := index . 1 -}}

###################################
# pmm Service
###################################
kind: Service
apiVersion: v1
metadata:
  name: pmm
  labels:
    component: pmm
    app: vitess
spec:
  ports:
    - name: web
      port: 80

  selector:
    component: pmm
    app: vitess
  type: ClusterIP
---
###################################
# pmm StatefulSet
###################################
apiVersion: apps/v1beta1
kind: StatefulSet
metadata:
  name: pmm
spec:
  serviceName: pmm
  replicas: 1
  updateStrategy: 
    type: RollingUpdate
  selector:
    matchLabels:
      app: vitess
      component: pmm
  template:
    metadata:
      labels:
        app: vitess
        component: pmm
    spec:
      containers:
        - name: pmm
          image: "percona/pmm-server:{{ $pmm.pmmTag }}"

          ports:
            - name: web
              containerPort: 80

          volumeMounts:
            - name: pmmdata
              mountPath: /pmmdata

          resources:
{{ toYaml $pmm.server.resources | indent 12 }}

          env:
            - name: DISABLE_UPDATES
              value: "true"

            - name: DISABLE_TELEMETRY
              value: {{ $pmm.server.env.disableTelemetry | quote }}

            - name: METRICS_RESOLUTION
              value: {{ $pmm.server.env.metricsResolution | quote }}

            - name: METRICS_RETENTION
              value: {{ $pmm.server.env.metricsRetention | quote }}

            - name: QUERIES_RETENTION
              value: {{ $pmm.server.env.queriesRetention | quote }}

            - name: METRICS_MEMORY
              value: {{ $pmm.server.env.metricsMemory | quote }}
        
          command: ["bash"]
          args:
            - "-c"
            - |
              set -ex


              if [ ! -f /pmmdata/vitess-init ]; then
                  # the PV hasn't been initialized, so copy over default
                  # pmm-server directories before symlinking
                  mkdir -p /pmmdata

                  mv /opt/prometheus/data /pmmdata/data
                  mv /opt/consul-data /pmmdata
                  mv /var/lib/mysql /pmmdata
                  mv /var/lib/grafana /pmmdata

                  # initialize the PV and then mark it complete
                  touch /pmmdata/vitess-init
              else
                  # remove the default directories so we can symlink the
                  # existing PV directories
                  rm -Rf /opt/prometheus/data
                  rm -Rf /opt/consul-data
                  rm -Rf /var/lib/mysql
                  rm -Rf /var/lib/grafana
              fi

              # symlink pmm-server paths to point to our PV
              ln -s /pmmdata/data /opt/prometheus/
              ln -s /pmmdata/consul-data /opt/
              ln -s /pmmdata/mysql /var/lib/
              ln -s /pmmdata/grafana /var/lib/
              
              /opt/entrypoint.sh

  volumeClaimTemplates:
    - metadata:
        name: pmmdata
        annotations:
{{ toYaml $pmm.server.dataVolumeClaimAnnotations | indent 10 }}
      spec:
{{ toYaml $pmm.server.dataVolumeClaimSpec | indent 8 }}

{{- end -}}

###################################
# sidecar container running pmm-client
###################################
{{- define "cont-pmm-client" -}}
{{- $pmm := index . 0 -}}
{{- $namespace := index . 1 -}}

- name: "pmm-client"
  image: "vitess/pmm-client:{{ $pmm.pmmTag }}"
  imagePullPolicy: IfNotPresent
  volumeMounts:
    - name: vtdataroot
      mountPath: "/vtdataroot"
  ports:
    - containerPort: 42001
      name: query-data
    - containerPort: 42002
      name: mysql-metrics

  securityContext:
    # PMM requires root privileges
    runAsUser: 0

  resources:
{{ toYaml $pmm.client.resources | indent 4 }}

  command: ["bash"]
  args:
    - "-c"
    - |
      set -ex

      mkdir -p /vtdataroot/pmm

      # redirect logs to PV
      ln -s /vtdataroot/pmm/pmm-mysql-metrics-42002.log /var/log/pmm-mysql-metrics-42002.log

      # --force is used because the pod ip address may have changed
      pmm-admin config --server pmm.{{ $namespace }} --force

      # creates a systemd service
      # TODO: remove "|| true" after https://jira.percona.com/projects/PMM/issues/PMM-1985 is resolved
      pmm-admin add mysql:metrics --user root --socket /vtdataroot/tabletdata/mysql.sock --force || true

      # keep the container alive but still responsive to stop requests
      trap : TERM INT; sleep infinity & wait

- name: pmm-client-metrics-log
  image: busybox
  command: ["/bin/sh"]
  args: ["-c", "tail -n+1 -F /vtdataroot/pmm/pmm-mysql-metrics-42002.log"]
  volumeMounts:
    - name: vtdataroot
      mountPath: /vtdataroot

{{- end -}}