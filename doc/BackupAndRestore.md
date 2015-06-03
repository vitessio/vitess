# Backup and Restore

This document describes Vitess Backup and Restore strategy.

### Overview

Backups are used in Vitess for two purposes: to provide a point-in-time backup for the data, and to bootstrap new instances.

### Backup Storage

Backups are stored on a Backup Storage service. Vitess core software contains an implementation that uses a local filesystem to store the files. Any network-mounted drive can then be used as the repository for backups.

We have plans to implement a version of the Backup Storage service for Google Cloud Storage (contact us if you are interested).

(The interface definition for the Backup Storage service is in [interface.go](https://github.com/youtube/vitess/blob/master/go/vt/mysqlctl/backupstorage/interface.go), see comments there for more details).

Concretely, the following command line flags are used for Backup Storage:

* -backup\_storage\_implementation: which implementation of the Backup Storage interface to use.
* -file\_backup\_storage\_root: the root of the backups if 'file' is used as a Backup Storage.

### Taking a Backup

To take a backup is very straightforward: just run the 'vtctl Backup <tablet-alias>' command. The designated tablet will take itself out of the healthy serving tablets, shutdown its mysqld process, copy the necessary files to the Backup Storage, restart mysql, restart replication, and join the cluster back.

With health-check enabled (the recommended default), the tablet goes back to spare state. Once it catches up on replication, it will go back to a serving state.

Note for this to work correctly, the tablet must be started with the right parameters to point it to the Backup Storage system (see previous section).

### Life of a Shard

To illustrate how backups are used in Vitess to bootstrap new instances, let's go through the creation and life of a Shard:

* A shard is initially brought up with no existing backup. All instances are started as replicas. With health-check enabled (the recommended default), each instance will realize replication is not running, and just stay unhealthy as spare.
* Once a few replicas are up, InitShardMaster is run, one host becomes the master, the others replicas. Master becomes healthy, replicas are not as no database exists.
* Initial schema can then be applied to the Master. Either use the usual Schema change tools, or use CopySchemaShard for shards created as targets for resharding.
* After replicating the schema creation, all replicas become healthy. At this point, we have a working and functionnal shard.
* The initial backup is taken (that stores the data and the current replication position), and backup data is copied to a network storage.
* When a replica comes up (either a new replica, or one whose instance was just restarted), it restores the latest backup, resets its master to the current shard master, and starts replicating.
* A Cronjob to backup the data on a regular basis should then be run. The frequency of the backups should be high enough (compared to MySQL binlog retention), so we can always have a backup to fall back upon.

Restoring a backup is enabled by the --restore\_from\_backup command line option in vttablet. It can be enabled all the time for all the tablets in a shard, as it doesn't prevent vttablet from starting if no backup can be found.

### Backup Management

Two vtctl commands exist to manage the backups:

* 'vtctl ListBackups <keyspace/shard>' will display the existing backups for a keyspace/shard in the order they were taken (oldest first).
* 'vtctl RemoveBackup <keyspace/shard> <backup name>' will remove a backup from Backup Storage.

### Details

Both Backup and Restore copy and compress / decompress multiple files simultaneously to increase throughput. The concurrency can be controlled by command-line flags (-concurrency for 'vtctl Backup', and -restore\_concurrency for vttablet). If the network link is fast enough, the concurrency will match the CPU usage of the process during backup / restore.




