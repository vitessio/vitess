# Helm Chart

This directory contains [Helm](https://github.com/kubernetes/helm)
charts for running [Vitess](http://vitess.io) on
[Kubernetes](http://kubernetes.io).

Note that this is not in the `examples` directory because these are the
sources for canonical packages that we plan to publish to the official
[Kubernetes Charts Repository](https://github.com/kubernetes/charts).
However, you may also find them useful as a starting point for creating
customized charts for your site, or other general-purpose charts for
common cluster variations.

## Example `site-values.yaml` configurations

You will need to provide a `site-values.yaml` file in order for Vitess to properly
install. Here are examples of various configurations. To see additional options,
look at the default `values.yaml` file, which is well commented.

### Minimum config

```
topology:
  cells:
    - name: "datacenter-1"
      keyspaces:
        - name: "unsharded-dbname"
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
    - name: "datacenter-1"
      keyspaces:
        - name: "unsharded-dbname"
          shards:
            - name: "0"
              tablets:
                - type: "replica"
                  vttablet:
                    replicas: 2
        - name: "sharded-db"
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
    - name: "datacenter-1"
      keyspaces:
        - name: "unsharded-dbname"
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
    - name: "datacenter-1"
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