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

The following steps explain how the backup process is used to bootstrap
new tablets as part of the normal lifecyle of a shard:

1. A shard is initially created without an existing backup, and all
    of the shard's tablets are started as spares.

1. By default, Vitess enables health checks on each tablet. As long as
    these default checks are used, each tablet recognizes that replication
    is not running and remains in an unhealthy state as a spare tablet.

1. After the requisite number of spare tablets is running, vtctl's
    [InitShardMaster](/reference/vtctl.html#initshardmaster) command
    designates one tablet as the master. The remaining tablets are
    slaves of the master tablet. In the serving graph, the master
    tablet is in a healthy state, but the slaves remain unhealthy
    because no database exists.

1. The initial schema is applied to the master using either usual schema
    change tools or vtctl's
    [CopySchemaShard](/reference/vtctl.html#copyschemashard) command.
    That command is typically used during resharding to clone data to a
    destination shard. After being applied to the master tablet, the
    schema propagates to the slave tablets.

1. The slave tablets all transition to a healthy state like
   <code>rdonly</code> or <code>replica</code>. At this point,
   the shard is working and functional.

1. Once the shard is accumulating data, a cron job runs regularly to
    create new backups. Backups are created frequently enough to ensure
    that one is always available if needed.

    To determine the proper frequency for creating backups, consider
    the amount of time that you keep replication logs and allow enough
    time to investigate and fix problems in the event that a backup
    operation fails.

    For example, suppose you typically keep four days of replication logs
    and you create daily backups. In that case, even if a backup fails,
    you have at least a couple of days from the time of the failure to
    investigate and fix the problem.

1. When a spare tablet comes up, it restores the latest backup, which
    contains data as well as the backup's replication position. The
    tablet then resets its master to the current shard master and starts
    replicating.

    This process is the same for new slave tablets and slave tablets that
    are being restarted. For example, to add a new rdonly tablet to your
    existing implementation, you would run the following steps:

    1. Run the vtctl [InitTablet](/reference/vtctl.html#inittablet)
        command to create the new tablet as a spare. Specify the
        appropriate values for the <nobr><code>-keyspace</code></nobr>
        and <nobr><code>-shard</code></nobr> flags, enabling Vitess to
        identify the master tablet associated with the new spare.

    1. Start the tablet using the flags specified in the
        [Prerequisites](#prerequisites) section. As described earlier in
        this step, the new tablet will load the latest backup, reset its
        master tablet, and start replicating.

## Concurrency

The back-up and restore processes simultaneously copy and either
compress or decompress multiple files to increase throughput. You
can control the concurrency using command-line flags:

* The vtctl [Backup](/reference/vtctl.html#backup) command uses the
    <code>-concurrency</code> flag.
* vttablet uses the <code>-restore_concurrency</code> flag.

If the network link is fast enough, the concurrency matches the CPU
usage of the process during the backup or restore process.

