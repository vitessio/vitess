###################################
# keyspace initializations
###################################

{{- define "vtctlclient-job" -}}
{{- $job := index . 0 -}}
{{- $defaultVtctlclient := index . 1 -}}
{{- $namespace := index . 2 -}}

{{- $vitessTag := $job.vitessTag | default $defaultVtctlclient.vitessTag -}}
{{- $secrets := $job.secrets | default $defaultVtctlclient.secrets }}
---
###################################
# Vitess vtctlclient Job
###################################
apiVersion: batch/v1
kind: Job
metadata:
  name: vtctlclient-{{ $job.name }}
spec:
  backoffLimit: 1
  template:
    spec:
      restartPolicy: OnFailure
      containers:
      - name: vtjob
        image: "vitess/vtctlclient:{{$vitessTag}}"
        volumeMounts:
{{ include "user-secret-volumeMounts" $defaultVtctlclient.secrets | indent 10 }}
        resources:
{{ toYaml ($job.resources | default $defaultVtctlclient.resources) | indent 10 }}

        command: ["bash"]
        args:
          - "-c"
          - |
            set -ex

            VTCTLD_SVC=vtctld.{{ $namespace }}:15999
            VTCTL_EXTRA_FLAGS=({{ include "format-flags-inline" $defaultVtctlclient.extraFlags }})
            vtctlclient ${VTCTL_EXTRA_FLAGS[@]} -server $VTCTLD_SVC {{ $job.command }}
      volumes:
{{ include "user-secret-volumes" $secrets | indent 8 }}

{{- end -}}

{{- define "vtworker-job" -}}
{{- $job := index . 0 -}}
{{- $defaultVtworker := index . 1 -}}
{{- $namespace := index . 2 -}}

{{- $vitessTag := $job.vitessTag | default $defaultVtworker.vitessTag -}}
{{- $secrets := $job.secrets | default $defaultVtworker.secrets }}
---
###################################
# Vitess vtworker Job
###################################
apiVersion: batch/v1
kind: Job
metadata:
  name: vtworker-{{ $job.name }}
spec:
  backoffLimit: 1
  template:
    spec:
{{ include "pod-security" . | indent 6 }}
      restartPolicy: OnFailure
      containers:
      - name: vtjob
        image: "vitess/vtworker:{{$vitessTag}}"
        volumeMounts:
{{ include "user-secret-volumeMounts" $defaultVtworker.secrets | indent 10 }}
        resources:
{{ toYaml ($job.resources | default $defaultVtworker.resources) | indent 10 }}

        command: ["bash"]
        args:
          - "-c"
          - |
            set -ex

            eval exec /vt/bin/vtworker $(cat <<END_OF_COMMAND
              -topo_implementation="etcd2"
              -topo_global_server_address="etcd-global-client.{{ $namespace }}:2379"
              -topo_global_root=/vitess/global
              -cell={{ $job.cell | quote }}
              -logtostderr=true
              -stderrthreshold=0
            END_OF_COMMAND
            ) {{ $job.command }}
      volumes:
{{ include "user-secret-volumes" $secrets | indent 8 }}

{{- end -}}
