This document explains how to create and restore data backups with
Vitess. Vitess uses backups for two purposes:

* Provide a point-in-time backup of the data on a tablet
* Bootstrap new tablets in an existing shard

## Prerequisites

Vitess stores data backups on a Backup Storage service. Currently,
Vitess supports backups to either [Google Cloud Storage](https://cloud.google.com/storage/)
or any network-mounted drive (such as NFS). The core Vitess software's
[BackupStorage interface](https://github.com/youtube/vitess/blob/master/go/vt/mysqlctl/backupstorage/interface.go)
defines methods for creating, listing, and removing backups. Plugins for other
storage services just need to implement the interface.

Before you can back up or restore a tablet, you need to ensure that the
tablet is aware of the Backup Storage system that you are using. To do so,
use the following command-line flags when starting a vttablet that has
access to the location where you are storing backups.

<table class="responsive">
  <thead>
    <tr>
      <th colspan="2">Flags</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><nobr><code>-backup_storage_implementation</code></nobr></td>
      <td>Specifies the implementation of the Backup Storage interface to use.<br><br>
          Current plugin options available are:
          <ul>
          <li><code>gcs</code>: For Google Cloud Storage.</li>
          <li><code>ceph</code>: For Ceph Object Gateway S3 API.</li>
          <li><code>file</code>: For NFS or any other filesystem-mounted network drive.</li>
          </ul>
      </td>
    </tr>
    <tr>
      <td><nobr><code>-file_backup_storage_root</code></nobr></td>
      <td>For the <code>file</code> plugin, this identifies the root directory for backups.</td>
    </tr>
    <tr>
      <td><nobr><code>-gcs_backup_storage_project</code></nobr></td>
      <td>For the <code>gcs</code> plugin, this identifies the <a href="https://cloud.google.com/storage/docs/projects">project</a> to use.</td>
    </tr>
    <tr>
      <td><nobr><code>-gcs_backup_storage_bucket</code></nobr></td>
      <td>For the <code>gcs</code> plugin, this identifies the <a href="https://cloud.google.com/storage/docs/concepts-techniques#concepts">bucket</a> to use.</td>
    </tr>
    <tr>
      <td><nobr><code>-ceph_backup_storage_config</code></nobr></td>
      <td>For the <code>ceph</code> plugin, this identifies the path to a text file with a JSON object as configuration. The JSON object requires the following keys: <code>accessKey</code>, <code>secretKey</code> and  <code>endPoint</code>. Bucket name is computed from keyspace name and is separate for different keyspaces.</td>
    </tr>
    <tr>
      <td><nobr><code>-restore_from_backup</code></nobr></td>
      <td>Indicates that, when started with an empty MySQL instance, the tablet should restore the most recent backup from the specified storage plugin.</td>
    </tr>
  </tbody>
</table>

### Authentication

Note that for the Google Cloud Storage plugin, we currently only support
[Application Default Credentials](https://developers.google.com/identity/protocols/application-default-credentials),
which means that access to Cloud Storage is automatically granted by virtue of
the fact that you're already running within Google Compute Engine or Container Engine.

For this to work, the GCE instances must have been created with the
[scope](https://cloud.google.com/compute/docs/authentication#using) that grants
read-write access to Cloud Storage. When using Container Engine, you can do this
for all the instances it creates by adding `--scopes storage-rw` to the
`gcloud container clusters create` command as shown in the [Vitess on Kubernetes guide]
(http://vitess.io/getting-started/#start-a-container-engine-cluster).

## Creating a backup

Run the following vtctl command to create a backup:

``` sh
vtctl Backup <tablet-alias>
```

In response to this command, the designated tablet performs the following sequence of actions:

1. Removes itself from the healthy serving tablets in the serving graph.

1. Shuts down its mysqld process.

1. Copies the necessary files to the Backup Storage implementation
    that was specified when the tablet was started.

1. Restarts mysql.

1. Restarts replication. By default, the tablet changes its status to
    <code>spare</code> until it catches up on replication and proceeds
    to the next step.
    If you override the default recommended behavior by setting the
    <code>-target_tablet_type</code> flag to <code>""</code> when starting the tablet,
    the tablet will restart replication and then proceed to the following
    step even if it has not caught up on the replication process.

1. Updates the serving graph to rejoin the cluster as a healthy, serving tablet.

## Restoring a backup

When a tablet starts, Vitess checks the value of the
<code>-restore_from_backup</code> command-line flag to determine whether
to restore a backup to that tablet.

* If the flag is present, Vitess tries to restore the most recent backup
    from the Backup Storage system when starting the tablet.
* If the flag is absent, Vitess does not try to restore a backup to the
    tablet. This is the equivalent of starting a new tablet in a new shard.

As noted in the [Prerequisites](#prerequisites) section, the flag is
generally enabled all of the time for all of the tablets in a shard.
If Vitess cannot find a backup in the Backup Storage system, it just
starts the vttablet as a new tablet.

``` sh
vttablet ... -backup_storage_implementation=file \
             -file_backup_storage_root=/nfs/XXX \
             -restore_from_backup
```

## Managing backups

**vtctl** provides two commands for managing backups:

* [ListBackups](/reference/vtctl.html#listbackups) displays the
    existing backups for a keyspace/shard in chronological order.

    ``` sh
vtctl ListBackups <keyspace/shard>
```

* [RemoveBackup](/reference/vtctl.html#removebackup) deletes a
    specified backup for a keyspace/shard.

    ``` sh
RemoveBackup <keyspace/shard> <backup name>
```

## Bootstrapping a new tablet

Bootstrapping a new tablet is almost identical to restoring an existing tablet.
The only thing you need to be cautious about is that the tablet specifies its keyspace, shard and tablet type when it registers itself at the topology.
Specifically, make sure that the following vttablet parameters are set:

``` sh
    -init_keyspace <keyspace>
    -init_shard <shard>
    -init_tablet_type replica|rdonly
```

The bootstrapped tablet will restore the data from the backup and then apply changes, which occurred after the backup, by restarting replication.


## Backup Frequency

We recommend to take backups regularly e.g. you should set up a cron
job for it.

To determine the proper frequency for creating backups, consider
the amount of time that you keep replication logs and allow enough
time to investigate and fix problems in the event that a backup
operation fails.

For example, suppose you typically keep four days of replication logs
and you create daily backups. In that case, even if a backup fails,
you have at least a couple of days from the time of the failure to
investigate and fix the problem.
        
## Concurrency

The back-up and restore processes simultaneously copy and either
compress or decompress multiple files to increase throughput. You
can control the concurrency using command-line flags:

* The vtctl [Backup](/reference/vtctl.html#backup) command uses the
    <code>-concurrency</code> flag.
* vttablet uses the <code>-restore_concurrency</code> flag.

If the network link is fast enough, the concurrency matches the CPU
usage of the process during the backup or restore process.

