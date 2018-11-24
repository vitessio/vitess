###################################
# keyspace initialization
###################################

{{- define "keyspace" -}}
{{- $cell := index . 0 -}}
{{- $keyspace := index . 1 -}}
{{- $defaultVtctlclient := index . 2 -}}
{{- $namespace := index . 3 -}}

{{- with $cell.vtctld -}}

# define image to use
{{- $vitessTag := .vitessTag | default $defaultVtctlclient.vitessTag -}}

{{ if $keyspace.vschema }}

###################################
# ApplyVschema job
###################################
apiVersion: batch/v1
kind: Job
metadata:
  name: {{ $keyspace.name }}-apply-vschema
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
{{ $keyspace.vschema | indent 14 }}
            END_OF_COMMAND
            )" {{ $keyspace.name }}
      volumes:
{{ include "user-secret-volumes" (.secrets | default $defaultVtctlclient.secrets) | indent 8 }}

{{- end -}}
{{- end -}}
{{- end -}}
