###################################
# PersistentVolumeClaim
###################################
{{- define "vtbackup-pvc" -}}
{{ if and .enabled (eq .backup_storage_implementation "file") }} 
{{ if eq .file_backup_storage_volume.name "persistentVolumeClaim" }}
{{ if .file_backup_storage_volume.persitent_volume_claim_name }}
{{ else }}
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: vtbackup-pvc
spec:
  accessModes:
  {{ if .persistent_volume_access_modes }}
    {{ toYaml (.persistent_volume_access_modes)}}
  {{ else }}
    - ReadWriteOnce
  {{ end }}
  storageClassName: {{ .file_backup_storage_volume.storage_class_name | default "default" }}
{{ if or .file_backup_storage_volume.persitent_volume_name .file_backup_storage_volume.persistent_volume_parameters }}  
  volumeName: {{ .file_backup_storage_volume.persitent_volume_name | default "vtbackup-pv" }}
{{ end }}
  resources:
    requests:
      storage: {{ .file_backup_storage_volume.storage | default "1Mi" }}
{{ end }}
{{ end }}
{{ end }}
{{- end -}}      


###################################
# PersistentVolume
###################################
{{- define "vtbackup-pv" -}}
{{ if and .enabled (eq .backup_storage_implementation "file") }} 
{{ if .file_backup_storage_volume.persistent_volume_parameters }}
---
apiVersion: v1
kind: PersistentVolume
metadata:
  name: {{ .file_backup_storage_volume.persitent_volume_name | default "vtbackup-pv" }}
spec:
  storageClassName: {{ .file_backup_storage_volume.storage_class_name | default "default" }}
  capacity:
    storage: {{ .file_backup_storage_volume.storage | default "1Mi" }}  
  accessModes:
  {{ if .persistent_volume_access_modes }}
    {{ toYaml (.persistent_volume_access_modes)}}
  {{ else }}
    - ReadWriteOnce
  {{ end }}

{{ printf "%s:" (.file_backup_storage_volume.persistent_volume_parameters.name) | indent 2 }}  
{{ toYaml (.file_backup_storage_volume.persistent_volume_parameters.parameters) | indent 4 }}  
{{ end }}
{{ end }}
{{- end -}}