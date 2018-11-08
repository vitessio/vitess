# Helper templates

#############################
# Format a flag map into a command line,
# as expected by the golang 'flag' package.
# Boolean flags must be given a value, such as "true" or "false".
#############################
{{- define "format-flags" -}}
{{- range $key, $value := . -}}
-{{$key}}={{$value | quote}}
{{end -}}
{{- end -}}

#############################
# Repeat a string N times, where N is the total number
# of replicas. Len must be used on the calling end to
# get an int
#############################
{{- define "tablet-count" -}}
{{- range . -}}
{{- repeat (int .vttablet.replicas) "x" -}}
{{- end -}}
{{- end -}}

#############################
# Format a list of flag maps into a command line.
#############################
{{- define "format-flags-all" -}}
{{- range . }}{{template "format-flags" .}}{{end -}}
{{- end -}}

#############################
# Clean labels, making sure it starts and ends with [A-Za-z0-9].
# This is especially important for shard names, which can start or end with
# '-' (like -80 or 80-), which would be an invalid kubernetes label.
#############################
{{- define "clean-label" -}}
{{- $replaced_label := . | replace "_" "-"}}
{{- if hasPrefix "-" . -}}
x{{$replaced_label}}
{{- else if hasSuffix "-" . -}}
{{$replaced_label}}x
{{- else -}}
{{$replaced_label}}
{{- end -}}
{{- end -}}

#############################
# injects default vitess environment variables
#############################
{{- define "vitess-env" -}}
- name: VTROOT
  value: "/vt"
- name: VTDATAROOT
  value: "/vtdataroot"
- name: GOBIN
  value: "/vt/bin"
- name: VT_MYSQL_ROOT
  value: "/usr"
- name: PKG_CONFIG_PATH
  value: "/vt/lib"
{{- end -}}

#############################
# inject default pod security
#############################
{{- define "pod-security" -}}
securityContext:
  runAsUser: 1000
  fsGroup: 2000
{{- end -}}

#############################
# support region nodeAffinity if defined
#############################
{{- define "node-affinity" -}}
{{- $region := . -}}
{{ with $region }}
nodeAffinity:
  requiredDuringSchedulingIgnoredDuringExecution:
    nodeSelectorTerms:
    - matchExpressions:
      - key: "failure-domain.beta.kubernetes.io/region"
        operator: In
        values: [{{ $region | quote }}]
{{- end -}}
{{- end -}}

#############################
# mycnf exec - expects extraMyCnf config map name
#############################
{{- define "mycnf-exec" -}}

if [ "$VT_DB_FLAVOR" = "percona" ]; then
  FLAVOR_MYCNF=/vt/config/mycnf/master_mysql56.cnf

elif [ "$VT_DB_FLAVOR" = "mysql" ]; then
  FLAVOR_MYCNF=/vt/config/mycnf/master_mysql56.cnf

elif [ "$VT_DB_FLAVOR" = "mysql56" ]; then
  FLAVOR_MYCNF=/vt/config/mycnf/master_mysql56.cnf

elif [ "$VT_DB_FLAVOR" = "maria" ]; then
  FLAVOR_MYCNF=/vt/config/mycnf/master_mariadb.cnf

elif [ "$VT_DB_FLAVOR" = "mariadb" ]; then
  FLAVOR_MYCNF=/vt/config/mycnf/master_mariadb.cnf

elif [ "$VT_DB_FLAVOR" = "mariadb103" ]; then
  FLAVOR_MYCNF=/vt/config/mycnf/master_mariadb103.cnf

fi

export EXTRA_MY_CNF="$FLAVOR_MYCNF:/vtdataroot/tabletdata/report-host.cnf:/vt/config/mycnf/rbr.cnf"

{{ if . }}
for filename in /vt/userconfig/*; do
  export EXTRA_MY_CNF="$EXTRA_MY_CNF:$filename"
done
{{ end }}

{{- end -}}

#############################
#
# all backup helpers below
#
#############################

#############################
# backup flags - expects config.backup
#############################
{{- define "backup-flags" -}}
{{- $backup := index . 0 -}}
{{- $caller := index . 1 -}}

{{ with $backup }}

  {{ if .enabled }}
    {{ if eq $caller "vttablet" }}
-restore_from_backup
    {{ end }}

-backup_storage_implementation=$VT_BACKUP_SERVICE

    {{ if eq .backup_storage_implementation "gcs" }}
-gcs_backup_storage_bucket=$VT_GCS_BACKUP_STORAGE_BUCKET
-gcs_backup_storage_root=$VT_GCS_BACKUP_STORAGE_ROOT

    {{ else if eq .backup_storage_implementation "s3" }}
-s3_backup_aws_region=$VT_S3_BACKUP_AWS_REGION
-s3_backup_storage_bucket=$VT_S3_BACKUP_STORAGE_BUCKET
-s3_backup_storage_root=$VT_S3_BACKUP_STORAGE_ROOT
-s3_backup_server_side_encryption=$VT_S3_BACKUP_SERVER_SIDE_ENCRYPTION

    {{ else if eq .backup_storage_implementation "ceph" }}
-ceph_backup_storage_config=$CEPH_CREDENTIALS_FILE
    {{ end }}

  {{ end }}

{{ end }}

{{- end -}}

#############################
# backup env - expects config.backup
#############################
{{- define "backup-env" -}}

{{ if .enabled }}

- name: VT_BACKUP_SERVICE
  valueFrom:
    configMapKeyRef:
      name: vitess-cm
      key: backup.backup_storage_implementation

{{ if eq .backup_storage_implementation "gcs" }}

- name: VT_GCS_BACKUP_STORAGE_BUCKET
  valueFrom:
    configMapKeyRef:
      name: vitess-cm
      key: backup.gcs_backup_storage_bucket
- name: VT_GCS_BACKUP_STORAGE_ROOT
  valueFrom:
    configMapKeyRef:
      name: vitess-cm
      key: backup.gcs_backup_storage_root

{{ else if eq .backup_storage_implementation "s3" }}

- name: VT_S3_BACKUP_AWS_REGION
  valueFrom:
    configMapKeyRef:
      name: vitess-cm
      key: backup.s3_backup_aws_region
- name: VT_S3_BACKUP_STORAGE_BUCKET
  valueFrom:
    configMapKeyRef:
      name: vitess-cm
      key: backup.s3_backup_storage_bucket
- name: VT_S3_BACKUP_STORAGE_ROOT
  valueFrom:
    configMapKeyRef:
      name: vitess-cm
      key: backup.s3_backup_storage_root
- name: VT_S3_BACKUP_SERVER_SIDE_ENCRYPTION
  valueFrom:
    configMapKeyRef:
      name: vitess-cm
      key: backup.s3_backup_server_side_encryption

{{ end }}

{{ end }}

{{- end -}}

#############################
# backup volume - expects config.backup
#############################
{{- define "backup-volume" -}}

{{ if .enabled }}

  {{ if eq .backup_storage_implementation "gcs" }}

    {{ if .gcsSecret }}
- name: backup-creds
  secret:
    secretName: {{ .gcsSecret }}
    {{ end }}

  {{ else if eq .backup_storage_implementation "s3" }}

    {{ if .s3Secret }}
- name: backup-creds
  secret:
    secretName: {{ .s3Secret }}
    {{ end }}

  {{ else if eq .backup_storage_implementation "ceph" }}

- name: backup-creds
  secret:
    secretName: {{required ".cephSecret necessary to use backup_storage_implementation: ceph!" .cephSecret }}

  {{ end }}

{{ end }}

{{- end -}}

#############################
# backup volumeMount - expects config.backup
#############################
{{- define "backup-volumeMount" -}}

{{ if .enabled }}

  {{ if eq .backup_storage_implementation "gcs" }}

    {{ if .gcsSecret }}
- name: backup-creds
  mountPath: /etc/secrets/creds
    {{ end }}

  {{ else if eq .backup_storage_implementation "s3" }}

    {{ if .s3Secret }}
- name: backup-creds
  mountPath: /etc/secrets/creds
    {{ end }}

  {{ else if eq .backup_storage_implementation "ceph" }}

- name: backup-creds
  mountPath: /etc/secrets/creds

  {{ end }}

{{ end }}

{{- end -}}

#############################
# backup exec
#############################
{{- define "backup-exec" -}}

{{ if .enabled }}

  {{ if eq .backup_storage_implementation "gcs" }}

    {{ if .gcsSecret }}
credsPath=/etc/secrets/creds/$(ls /etc/secrets/creds/ | head -1)

export GOOGLE_APPLICATION_CREDENTIALS=$credsPath
cat $GOOGLE_APPLICATION_CREDENTIALS
    {{ end }}

  {{ else if eq .backup_storage_implementation "s3" }}

    {{ if .s3Secret }}
credsPath=/etc/secrets/creds/$(ls /etc/secrets/creds/ | head -1)

export AWS_SHARED_CREDENTIALS_FILE=$credsPath
cat $AWS_SHARED_CREDENTIALS_FILE
    {{ end }}

  {{ else if eq .backup_storage_implementation "ceph" }}

credsPath=/etc/secrets/creds/$(ls /etc/secrets/creds/ | head -1)
export CEPH_CREDENTIALS_FILE=$credsPath
cat $CEPH_CREDENTIALS_FILE

  {{ end }}

{{ end }}

{{- end -}}

#############################
# user config volume - expects config map name
#############################
{{- define "user-config-volume" -}}

{{ if . }}

- name: user-config
  configMap:
    name: {{ . }}

{{ end }}

{{- end -}}

#############################
# user config volumeMount - expects config map name
#############################
{{- define "user-config-volumeMount" -}}

{{ if . }}

- name: user-config
  mountPath: /vt/userconfig

{{ end }}

{{- end -}}
