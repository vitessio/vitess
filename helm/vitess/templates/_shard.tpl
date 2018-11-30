###################################
# shard initializations
###################################

{{ define "shard" -}}
{{- $cell := index . 0 -}}
{{- $keyspace := index . 1 -}}
{{- $shard := index . 2 -}}
{{- $defaultVtctlclient := index . 3 -}}
{{- $namespace := index . 4 -}}
{{- $totalTabletCount := index . 5 -}}

# sanitize inputs for labels
{{- $cellClean := include "clean-label" $cell.name -}}
{{- $keyspaceClean := include "clean-label" $keyspace.name -}}
{{- $shardClean := include "clean-label" $shard.name -}}
{{- $shardName := printf "%s-%s-%s" $cellClean $keyspaceClean $shardClean | lower -}}

{{- with $cell.vtctld }}
# define image to use
{{- $vitessTag := .vitessTag | default $defaultVtctlclient.vitessTag }}
---
###################################
# InitShardMaster Job
###################################
apiVersion: batch/v1
kind: Job
metadata:
  name: {{ $shardName }}-init-shard-master
spec:
  backoffLimit: 1
  template:
    spec:
      restartPolicy: OnFailure
      containers:
      - name: init-shard-master
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

            until [ $TABLETS_READY ]; do
              # get all the tablets in the current cell
              cellTablets="$(vtctlclient ${VTCTL_EXTRA_FLAGS[@]} -server $VTCTLD_SVC ListAllTablets {{ $cell.name }})"

              # filter to only the tablets in our current shard
              shardTablets=$( echo "$cellTablets" | awk 'substr( $5,1,{{ len $shardName }} ) == "{{ $shardName }}" {print $0}')

              # check for a master tablet from the ListAllTablets call
              masterTablet=$( echo "$shardTablets" | awk '$4 == "master" {print $1}')
              if [ $masterTablet ]; then
                  echo "'$masterTablet' is already the master tablet, exiting without running InitShardMaster"
                  exit
              fi

              # check for a master tablet from the GetShard call
              master_alias=$(vtctlclient ${VTLCTL_EXTRA_FLAGS[@]} -server $VTCTLD_SVC GetShard {{ $keyspace.name }}/{{ $shard.name }} | jq '.master_alias.uid')
              if [ "$master_alias" != "null" -a "$master_alias" != "" ]; then
                  echo "'$master_alias' is already the master tablet, exiting without running InitShardMaster"
                  exit
              fi

              # count the number of newlines for the given shard to get the tablet count
              tabletCount=$( echo "$shardTablets" | wc | awk '{print $1}')

              # check to see if the tablet count equals the expected tablet count
              if [ $tabletCount == {{ $totalTabletCount }} ]; then
                TABLETS_READY=true
              else
                if (( $SECONDS > $TIMEOUT_SECONDS )); then
                  echo "timed out waiting for tablets to be ready"
                  exit 1
                fi

                # wait 5 seconds for vttablets to continue getting ready
                sleep 5
              fi

            done

            # find the tablet id for the "-replica-0" stateful set for a given cell, keyspace and shard
            tablet_id=$( echo "$shardTablets" | awk 'substr( $5,1,{{ add (len $shardName) 10 }} ) == "{{ $shardName }}-replica-0" {print $1}')

            # initialize the shard master
            until vtctlclient ${VTCTL_EXTRA_FLAGS[@]} -server $VTCTLD_SVC InitShardMaster -force {{ $keyspace.name }}/{{ $shard.name }} $tablet_id; do
              if (( $SECONDS > $TIMEOUT_SECONDS )); then
                echo "timed out waiting for InitShardMaster to succeed"
                exit 1
              fi
              sleep 5
            done
      volumes:
{{ include "user-secret-volumes" (.secrets | default $defaultVtctlclient.secrets) | indent 8 }}

---
{{- if $keyspace.copySchema }}
---
###################################
# CopySchemaShard Job
###################################
apiVersion: batch/v1
kind: Job
metadata:
  name: {{ $keyspaceClean }}-copy-schema-{{ $shardClean }}
spec:
  backoffLimit: 1
  template:
    spec:
      restartPolicy: OnFailure
      containers:
      - name: copy-schema
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
              master_alias=$(vtctlclient ${VTLCTL_EXTRA_FLAGS[@]} -server $VTCTLD_SVC GetShard {{ $keyspace.name }}/{{ $shard.name }} | jq '.master_alias.uid')
              if [ "$master_alias" == "null" -o "$master_alias" == "" ]; then
                  echo "no master for '{{ $keyspace.name }}/{{ $shard.name }}' yet, continuing to wait"
                  sleep 5
                  continue
              fi

              break
            done

            vtctlclient ${VTCTL_EXTRA_FLAGS[@]} -server $VTCTLD_SVC CopySchemaShard {{ if $keyspace.copySchema.tables -}}
            -tables='
              {{- range $index, $table := $keyspace.copySchema.tables -}}
                {{- if $index -}},{{- end -}}
                {{ $table }}
              {{- end -}}
            '
            {{- end }} {{ $keyspace.copySchema.source }} {{ $keyspace.name }}/{{ $shard.name }}
      volumes:
{{ include "user-secret-volumes" (.secrets | default $defaultVtctlclient.secrets) | indent 8 }}
{{ end }}


{{- end -}}
{{- end -}}
