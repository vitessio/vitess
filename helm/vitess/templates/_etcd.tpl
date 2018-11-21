###################################
# etcd cluster managed by pre-installed etcd operator
###################################
{{ define "etcd" -}}
# set tuple values to more recognizable variables
{{- $name := index . 0 -}}
{{- $replicas := index . 1 -}}
{{- $version := index . 2 -}}
{{- $resources := index . 3 }}

###################################
# EtcdCluster
###################################
apiVersion: "etcd.database.coreos.com/v1beta2"
kind: "EtcdCluster"
metadata:
  name: "etcd-{{ $name }}"
spec:
  size: {{ $replicas }}
  version: {{ $version | quote }}
  pod:
    resources:
{{ toYaml ($resources) | indent 6 }}
    affinity:
      podAntiAffinity:
        preferredDuringSchedulingIgnoredDuringExecution:
        # prefer to stay away from other same-cell etcd pods
        - weight: 100
          podAffinityTerm:
            topologyKey: kubernetes.io/hostname
            labelSelector:
              matchLabels:
                etcd_cluster: "etcd-{{ $name }}"
{{- end -}}
