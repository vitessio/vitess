# Vitess

[Vitess](http://vitess.io) is a database clustering system for horizontal
scaling of MySQL. It is an open-source project started at YouTube,
and has been used there since 2011.

## Introduction

This chart creates a Vitess cluster on Kubernetes in a single
[release](https://github.com/kubernetes/helm/blob/master/docs/glossary.md#release).
It currently includes all dependencies (e.g. etcd) and Vitess components
(vtctld, vtgate, vttablet) inline (in `templates/`) rather than as sub-charts.

**WARNING: This chart should be considered Beta.**

## Prerequisites

* Install [etcd-operator](https://github.com/coreos/etcd-operator) in the
  namespace where you plan to install this chart.

## Installing the Chart

```console
helm/vitess$ helm install . -f site-values.yaml
```

See the [Configuration](#configuration) section below for what you need to put
in `site-values.yaml`.
You can install the chart without site values, but it will only launch a
skeleton cluster without any keyspaces (logical databases).

## Cleaning up

After deleting an installation of the chart, the PersistentVolumeClaims remain.
If you don't intend to use them again, you should delete them:

```shell
kubectl delete pvc -l app=vitess
```

## Configuration

You will need to provide a `site-values.yaml` file to specify your actual
logical database topology (e.g. whether to shard).
Here are examples of various configurations. To see additional options,
look at the default `values.yaml` file, which is well commented.

### Unsharded keyspace

```
topology:
  cells:
    - name: "zone1"
      etcd:
        replicas: 3
      vtctld:
        replicas: 1
      vtgate:
        replicas: 3
      mysqlProtocol:
        enabled: false
      keyspaces:
        - name: "unsharded_dbname"
          shards:
            - name: "0"
              tablets:
                - type: "replica"
                  vttablet:
                    replicas: 2
```

### Unsharded + sharded keyspaces

```
topology:
  cells:
    - name: "zone1"
      ...
      keyspaces:
        - name: "unsharded_dbname"
          shards:
            - name: "0"
              tablets:
                - type: "replica"
                  vttablet:
                    replicas: 2
        - name: "sharded_db"
          shards:
            - name: "-80"
              tablets:
                - type: "replica"
                  vttablet:
                    replicas: 2
            - name: "80-"
              tablets:
                - type: "replica"
                  vttablet:
                    replicas: 2
```

### Separate pools of replicas and rdonly tablets 

```
topology:
  cells:
    - name: "zone1"
      ...
      keyspaces:
        - name: "unsharded_dbname"
          shards:
            - name: "0"
              tablets:
                - type: "replica"
                  vttablet:
                    replicas: 2
                - type: "rdonly"
                  vttablet:
                    replicas: 2
```

### Append custom my.cnf to default Vitess settings

Create a config map with one or more standard `my.cnf` formatted files. Any settings
provided here will overwrite any colliding values from Vitess defaults.

`kubectl create configmap shared-my-cnf --from-file=shared.my.cnf`

*NOTE:* if using MySQL 8.0.x, this file must contain
`default_authentication_plugin = mysql_native_password`

```
topology:
  cells:
    ...

vttablet:

  # The name of a config map with N files inside of it. Each file will be added
  # to $EXTRA_MY_CNF, overriding any default my.cnf settings
  extraMyCnf: shared-my-cnf
```

### Use a custom database image and a specific Vitess release

```
topology:
  cells:
    ...

vttablet:
  vitessTag: "2.1"
  mysqlImage: "percona:5.7.20"
  flavor: percona
```

### Enable MySQL protocol support

```
topology:
  cells:
    - name: "zone1"
      ...
      # enable or disable mysql protocol support, with accompanying auth details
      mysqlProtocol:
        enabled: false
        username: myuser
        # this is the secret that will be mounted as the user password
        # kubectl create secret generic myuser_password --from-literal=password=abc123
        passwordSecret: myuser-password

      keyspaces:
         ...
```

### Enable backup/restore using Google Cloud Storage

```
topology:
  cells:
    ...

config:
  backup:
    enabled: true
    backup_storage_implementation: gcs

    # Google Cloud Storage bucket to use for backups
    gcs_backup_storage_bucket: vitess-backups

    # root prefix for all backup-related object names
    gcs_backup_storage_root: vtbackups
```

### Custom requests/limits

```
topology:
  cells:
    ...

vttablet:
  resources:
    # common production values 2-4CPU/4-8Gi RAM
    limits:
      cpu: 2
      memory: 4Gi
  mysqlResources:
    # common production values 4CPU/8-16Gi RAM
    limits:
      cpu: 4
      memory: 8Gi
  # PVC for mysql
  dataVolumeClaimAnnotations:
  dataVolumeClaimSpec:
    # pd-ssd (Google Cloud)
    # managed-premium (Azure)
    # standard (AWS) - not sure what the default class is for ssd
    storageClassName: "default"
    accessModes: ["ReadWriteOnce"]
    resources:
      requests:
        storage: "10Gi"
```

### Custom PVC for MySQL data

```
topology:
  cells:
    ...

vttablet:
  dataVolumeClaimSpec:
    # Google Cloud SSD
    storageClassName: "pd-ssd"
    accessModes: ["ReadWriteOnce"]
    resources:
      requests:
        storage: "100Gi"
```

### Enable PMM (Percona Monitoring and Management)

```
topology:
  cells:
    ...

pmm:
  enabled: true
  pmmTag: "1.14.1"
  client:
    resources:
      requests:
        cpu: 50m
        memory: 128Mi
      limits:
        cpu: 200m
        memory: 256Mi
  server:
    resources:
      limits:
        cpu: 2
        memory: 4Gi
    dataVolumeClaimSpec:
      storageClassName: "default"
      accessModes: ["ReadWriteOnce"]
      resources:
        requests:
          storage: "150Gi"
    env:
      metricsMemory: "3000000"
```

### Enable Orchestrator
#### NOTE: This requires at least Kubernetes 1.9

```
topology:
  cells:
    ...

orchestrator:
  enabled: true
```
