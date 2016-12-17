# Helper templates

# Format a flag map into a command line,
# as expected by the golang 'flag' package.
# Boolean flags must be given a value, such as "true" or "false".
{{- define "format-flags" -}}
{{- range $key, $value := . -}}
-{{$key}}={{$value | quote}}
{{end -}}
{{- end -}}

# Format a list of flag maps into a command line.
{{- define "format-flags-all" -}}
{{- range . }}{{template "format-flags" .}}{{end -}}
{{- end -}}

# Format a shard name, making sure it starts and ends with [A-Za-z0-9].
{{- define "format-shard-name" -}}
{{- if hasPrefix "-" . -}}
x{{.}}
{{- else if hasSuffix "-" . -}}
{{.}}x
{{- else -}}
{{.}}
{{- end -}}
{{- end -}}

# Common init-container to set up vtdataroot volume.
{{- define "init-vtdataroot" -}}
{{- $image := . -}}
{
  "name": "init-vtdataroot",
  "image": {{$image | quote}},
  "imagePullPolicy": "IfNotPresent",
  "command": ["bash", "-c", "
    set -ex;
    mkdir -p $VTDATAROOT/tmp;
    chown vitess:vitess $VTDATAROOT $VTDATAROOT/tmp;
  "],
  "volumeMounts": [
    {
      "name": "vtdataroot",
      "mountPath": "/vt/vtdataroot"
    }
  ]
}
{{- end -}}

