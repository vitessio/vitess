###################################
# keyspace initializations
###################################

{{- define "keyspace" -}}
{{- $cell := index . 0 -}}
{{- $keyspace := index . 1 -}}
{{- $defaultVtctlclient := index . 2 -}}
{{- $namespace := index . 3 -}}

# sanitize inputs for labels
{{- $keyspaceClean := include "clean-label" $keyspace.name -}}

{{- with $cell.vtctld -}}

# define image to use
{{- $vitessTag := .vitessTag | default $defaultVtctlclient.vitessTag -}}
{{- $secrets := .secrets | default $defaultVtctlclient.secrets -}}

{{- range $name, $schema := $keyspace.schema }}
---
###################################
# ApplySchema Job
###################################
apiVersion: batch/v1
kind: Job
metadata:
  name: {{ $keyspaceClean }}-apply-schema-{{ $name }}
spec:
  backoffLimit: 1
  template:
    spec:
      restartPolicy: OnFailure
      containers:
      - name: apply-schema
        image: "vitess/vtctlclient:{{$vitessTag}}"
        volumeMounts:
{{ include "user-secret-volumeMounts" $defaultVtctlclient.secrets | indent 10 }}

        command: ["bash"]
        args:
          - "-c"
          - |
            set -ex

            VTCTLD_SVC=vtctld.{{ $namespace }}:15999
            SECONDS=0
            TIMEOUT_SECONDS=600
            VTCTL_EXTRA_FLAGS=({{ include "format-flags-inline" $defaultVtctlclient.extraFlags }})

            # poll every 5 seconds to see if vtctld is ready
            until vtctlclient ${VTCTL_EXTRA_FLAGS[@]} -server $VTCTLD_SVC ListAllTablets {{ $cell.name }} > /dev/null 2>&1; do
              if (( $SECONDS > $TIMEOUT_SECONDS )); then
                echo "timed out waiting for vtctlclient to be ready"
                exit 1
              fi
              sleep 5
            done

            while true; do
              if (( $SECONDS > $TIMEOUT_SECONDS )); then
                echo "timed out waiting for master"
                exit 1
              fi

              # wait for all shards to have a master
              {{- range $shard := $keyspace.shards }}
              master_alias=$(vtctlclient ${VTLCTL_EXTRA_FLAGS[@]} -server $VTCTLD_SVC GetShard {{ $keyspace.name }}/{{ $shard.name }} | jq '.master_alias.uid')
              if [ "$master_alias" == "null" -o "$master_alias" == "" ]; then
                  echo "no master for '{{ $keyspace.name }}/{{ $shard.name }}' yet, continuing to wait"
                  sleep 5
                  continue
              fi
              {{- end }}

              break
            done

            vtctlclient ${VTCTL_EXTRA_FLAGS[@]} -server $VTCTLD_SVC ApplySchema -sql "$(cat <<END_OF_COMMAND
{{ $schema | indent 14}}
            END_OF_COMMAND
            )" {{ $keyspace.name }}
      volumes:
{{ include "user-secret-volumes" $secrets | indent 8 }}
{{ end }}

{{- range $name, $vschema := $keyspace.vschema }}
---
###################################
# ApplyVSchema job
###################################
apiVersion: batch/v1
kind: Job
metadata:
  name: {{ $keyspaceClean }}-apply-vschema-{{ $name }}
spec:
  backoffLimit: 1
  template:
    spec:
      restartPolicy: OnFailure
      containers:
      - name: apply-vschema
        image: "vitess/vtctlclient:{{$vitessTag}}"
        volumeMounts:
{{ include "user-secret-volumeMounts" $defaultVtctlclient.secrets | indent 10 }}

        command: ["bash"]
        args:
          - "-c"
          - |
            set -ex

            VTCTLD_SVC=vtctld.{{ $namespace }}:15999
            SECONDS=0
            TIMEOUT_SECONDS=600
            VTCTL_EXTRA_FLAGS=({{ include "format-flags-inline" $defaultVtctlclient.extraFlags }})

            # poll every 5 seconds to see if keyspace is created
            until vtctlclient ${VTCTL_EXTRA_FLAGS[@]} -server $VTCTLD_SVC GetKeyspace {{ $keyspace.name }} > /dev/null 2>&1; do
              if (( $SECONDS > $TIMEOUT_SECONDS )); then
                echo "timed out waiting for keyspace {{ $keyspace.name }} to be ready"
                exit 1
              fi
              sleep 5
            done

            vtctlclient ${VTCTL_EXTRA_FLAGS[@]} -server $VTCTLD_SVC ApplyVSchema -vschema "$(cat <<END_OF_COMMAND
{{ $vschema | indent 14 }}
            END_OF_COMMAND
            )" {{ $keyspace.name }}
      volumes:
{{ include "user-secret-volumes" $secrets | indent 8 }}

{{- end -}}
{{- end -}}
{{- end -}}
