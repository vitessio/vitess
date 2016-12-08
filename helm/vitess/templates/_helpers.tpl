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

# Common init-container to set up vtdataroot volume.
{{- define "init-vtdataroot" -}}
{{- $image := . -}}
{
  "name": "init-vtdataroot",
  "image": {{$image | quote}},
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

