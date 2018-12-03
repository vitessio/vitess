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

Enabling backups creates a cron job per shard that defaults to executing once per day at midnight.
This can be overridden on a per shard level so you can stagger when backups occur.

```
topology:
  cells:
    - name: "zone1"
      ...
      keyspaces:
        - name: "unsharded_dbname"
          shards:
            - name: "0"
              backup:
                cron:
                  schedule: "0 1 * * *"
                  suspend: false
              tablets:
                - type: "replica"
                  vttablet:
                    replicas: 2
        - name: "sharded_db"
          shards:
            - name: "-80"
              backup:
                cron:
                  schedule: "0 2 * * *"
                  suspend: false
              tablets:
                - type: "replica"
                  vttablet:
                    replicas: 2
            - name: "80-"
              backup:
                cron:
                  schedule: "0 3 * * *"
                  suspend: false
              tablets:
                - type: "replica"
                  vttablet:
                    replicas: 2

config:
  backup:
    enabled: true

    cron:
      # the default schedule runs daily at midnight unless overridden by the individual shard
      schedule: "0 0 * * *"

      # if this is set to true, the cron jobs are created, but never execute
      suspend: false

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
  pmmTag: "1.17.0"
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

### Enable TLS encryption for vitess grpc communication

Each component of vitess requires a certificate and private key to secure incoming requests and further configuration for every outgoing connection. In this example TLS certificates were generated and stored in several kubernetes secrets:
```yaml
vttablet:
  extraFlags:
    # configure which certificates to use for serving grpc requests
    grpc_cert: /vt/usersecrets/vttablet-tls/vttablet.pem 
    grpc_key: /vt/usersecrets/vttablet-tls/vttablet-key.pem
    tablet_grpc_ca: /vt/usersecrets/vttablet-tls/vitess-ca.pem 
    tablet_grpc_server_name: vttablet 
  secrets:
  - vttablet-tls

vtctld:
  extraFlags:
    grpc_cert: /vt/usersecrets/vtctld-tls/vtctld.pem
    grpc_key: /vt/usersecrets/vtctld-tls/vtctld-key.pem
    tablet_grpc_ca: /vt/usersecrets/vtctld-tls/vitess-ca.pem
    tablet_grpc_server_name: vttablet
    tablet_manager_grpc_ca: /vt/usersecrets/vtctld-tls/vitess-ca.pem
    tablet_manager_grpc_server_name: vttablet
  secrets:
  - vtctld-tls

vtctlclient: # configuration used by both InitShardMaster-jobs and orchestrator to be able to communicate with vtctld
  extraFlags:
    vtctld_grpc_ca: /vt/usersecrets/vitess-ca/vitess-ca.pem
    vtctld_grpc_server_name: vtctld
  secrets:
  - vitess-ca

vtgate:
  extraFlags:
    grpc_cert: /vt/usersecrets/vtgate-tls/vtgate.pem
    grpc_key: /vt/usersecrets/vtgate-tls/vtgate-key.pem
    tablet_grpc_ca: /vt/usersecrets/vtgate-tls/vitess-ca.pem
    tablet_grpc_server_name: vttablet
  secrets:
  - vtgate-tls
```

### Slave replication traffic encryption

To encrypt traffic between slaves and master additional flags can be provided. By default MySQL generates self-signed certificates on startup (otherwise specify `ssl_*` settings within you `extraMyCnf`), that can be used to encrypt the traffic:
```
vttablet:
  extraFlags:
    db_flags: 2048
    db_repl_use_ssl: true
    db-config-repl-flags: 2048

```

### Percona at rest encryption using the vault plugin

To use the [percona at rest encryption](https://www.percona.com/doc/percona-server/LATEST/management/data_at_rest_encryption.html) several additional settings have to be provided via an `extraMyCnf`-file. This makes only sense if the traffic is encrypted as well (see above sections), since binlog replication is unencrypted by default.
```
apiVersion: v1
kind: ConfigMap
metadata:
  name: vttablet-extra-config
  namespace: vitess
data:
  extra.cnf: |-
    early-plugin-load=keyring_vault=keyring_vault.so
    # this includes default rpl plugins, see https://github.com/vitessio/vitess/blob/master/config/mycnf/master_mysql56.cnf for details
    plugin-load=rpl_semi_sync_master=semisync_master.so;rpl_semi_sync_slave=semisync_slave.so;keyring_udf=keyring_udf.so
    keyring_vault_config=/vt/usersecrets/vttablet-vault/vault.conf # load keyring configuration from secret
    innodb_encrypt_tables=ON # encrypt all tables by default
    encrypt_binlog=ON # binlog encryption
    master_verify_checksum=ON # necessary for binlog encryption
    binlog_checksum=CRC32 # necessary for binlog encryption
    encrypt-tmp-files=ON # use temporary AES keys to encrypt temporary files
```

An example vault configuration, which is provided by the `vttablet-vault`-Secret in the above example:
```
vault_url = https://10.0.0.1:8200                                                                                
secret_mount_point = vitess
token = 11111111-1111-1111-1111111111
vault_ca = /vt/usersecrets/vttablet-vault/vault-ca-bundle.pem
```

At last add the secret containing the vault configuration and the additional MySQL-configuration to your helm values:
```
vttablet:
  flavor: "percona" # only works with percona
  mysqlImage: "percona:5.7.23"
  extraMyCnf: vttablet-extra-config
  secrets:
  - vttablet-vault
```
