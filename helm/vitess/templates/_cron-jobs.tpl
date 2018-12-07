###################################
# backup cron
###################################
{{ define "vttablet-backup-cron" -}}
# set tuple values to more recognizable variables
{{- $cellClean := index . 0 -}}
{{- $keyspaceClean := index . 1 -}}
{{- $shardClean := index . 2 -}}
{{- $shardName := index . 3 -}}
{{- $keyspace := index . 4 -}}
{{- $shard := index . 5 -}}
{{- $vitessTag := index . 6 -}}
{{- $backup := index . 7 -}}
{{- $namespace := index . 8 -}}
{{- $defaultVtctlclient := index . 9 }}

{{ if $backup.enabled }}
# create cron job for current shard
---
apiVersion: batch/v1beta1
kind: CronJob
metadata:
  name: {{ $shardName }}-backup
  labels:
    app: vitess
    component: vttablet
    cell: {{ $cellClean | quote }}
    keyspace: {{ $keyspaceClean | quote }}
    shard: {{ $shardClean | quote }}
    backupJob: "true"

spec:
  schedule: {{ $shard.backup.cron.schedule | default $backup.cron.schedule | quote }}
  concurrencyPolicy: Forbid
  suspend: {{ $shard.backup.cron.suspend | default $backup.cron.suspend }}
  successfulJobsHistoryLimit: 3
  failedJobsHistoryLimit: 20

  jobTemplate:
    spec:
      template:
        metadata:
          labels:
            app: vitess
            component: vttablet
            cell: {{ $cellClean | quote }}
            keyspace: {{ $keyspaceClean | quote }}
            shard: {{ $shardClean | quote }}
            backupJob: "true"

        # pod spec
        spec:
          restartPolicy: Never
{{ include "pod-security" . | indent 10 }}

          containers:
          - name: backup
            image: "vitess/vtctlclient:{{$vitessTag}}"
            volumeMounts:
{{ include "user-secret-volumeMounts" $defaultVtctlclient.secrets | indent 14 }}

            command: ["bash"]
            args:
              - "-c"
              - |
                set -ex

                VTCTLD_SVC=vtctld.{{ $namespace }}:15999
                VTCTL_EXTRA_FLAGS=({{ include "format-flags-inline" $defaultVtctlclient.extraFlags }})

                vtctlclient ${VTCTL_EXTRA_FLAGS[@]} -server $VTCTLD_SVC BackupShard {{ $keyspace.name }}/{{ $shard.name }}

            resources:
              requests:
                cpu: 10m
                memory: 20Mi

{{ end }}

{{- end -}}
