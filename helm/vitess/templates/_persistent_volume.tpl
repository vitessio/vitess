###################################
# PersistentVolumeClaim
###################################
{{- define "vtbackup-pvc" -}}
{{ if eq .file_backup_storage_volume.name "persistentVolumeClaim" }}
{{ if .file_backup_storage_volume.persitent_volume_claim_name }}
{{ else }}
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: vtbackup-pvc
spec:  
  accessModes:
    - ReadWriteMany
  storageClassName: {{ .file_backup_storage_volume.storage_class_name | default "" }}
{{ if or .file_backup_storage_volume.persitent_volume_name .file_backup_storage_volume.persistent_volume_parameters }}  
  volumeName: {{ .file_backup_storage_volume.persitent_volume_name | default "vtbackup-pv" }}
{{ end }}
  resources:
    requests:
      storage: {{ .file_backup_storage_volume.storage | default "1Mi" }}
{{ end }}
{{ end }}
{{- end -}}      


###################################
# PersistentVolume
###################################
{{- define "vtbackup-pv" -}}
{{ if .file_backup_storage_volume.persistent_volume_parameters }}
---
apiVersion: v1
kind: PersistentVolume
metadata:
  name: {{ .file_backup_storage_volume.persitent_volume_name | default "vtbackup-pv" }}
spec:  
  accessModes:
    - ReadWriteMany
{{ printf "%s:" (.file_backup_storage_volume.persistent_volume_parameters.name) | indent 2 }}  
{{ toYaml (.file_backup_storage_volume.persistent_volume_parameters.parameters) | indent 4 }}  
{{ end }}
{{- end -}}