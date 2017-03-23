# Separating VTTablet and MySQL

This document explores the setup where VTTablet and MySQL are not running on the
same server, or in the same container. This is mostly useful when using a
managed MySQL server (like CloudSQL or RDS).

In this setup, Vitess is not responsible for any master management, or
backups. Schema Swap is also not possible, as it relies on backups / restores.

`Note`: this document is a work in progress, meant to centralize all findings on
this subject. Eventually, we want to have an end to end test proving this setup
works, probably on Kubernetes / GCE using CloudSQL.

## VTTablet Configuration

The following adjustments need to be made to VTTablet command line parameters:

* Do not specify `-mycnf-file`: instead, specify the `-mycnf_server_id`
  parameter.

* Do not use `-mycnf_socket_file`. There is no local MySQL unix socket file.

* Specify the host and port of the MySQL daemon for the `-db-config-XXX-host`
  and `-db-config-XXX-port` command line parameters. Do not specify
  `-db-config-XXX-unixsocket` parameters.

* Disable restores / backups, by not passing any backup command line
  parameters. Specifically, `-restore_from_backup` and
  `-backup_storage_implementation` shoud not be set.

Since master management and replication are not handled by Vitess, we just need
to make sure the tablet type in the topology is correct before running
vttablet. Usually, vttablet can be started with `-init_tablet_type replica`,
even for a master (as `master` is not allowed), and will figure out the master
and set its type to `master`. When Vitess doesn't manage that at all, running
`vtctl InitShardMaster` is not possible, so there is no way to start as the
master just using vttablet. There are two solutions:

* Preferred: Start the master with `-init_tablet_type replica`, and then run a
  `vtctl TabletExternallyReparented <tablet alias>` for the actual master.

* Run `vtctl InitTablet ... master` for the master, and then run vttablet with
  no `-init...` parameters.
  
## Other Configurations

`vtctl` and `vtctld` can be run with the `-disable_active_reparents` flag. This
would make all explicit reparent commands unreachable (like `InitShardMaster`
or `PlannedReparentShard`).

`vtgate` and `vtworker` don't need any special configuration.

## Runtime Differences

There are some subtle differences when connecting to MySQL using TCP, as opposed
to connecting using a Unix socket. The main one is that when the server closes
the connection, the client knows it when writing on a unix socket, but only
realizes it when reading the response from a TCP socket. So using TCP, it is
harder to distinguish a query that was killed from a query that was sent on a
closed connection. It matters because we don't want to retry a query that was
killed, but we want to retry a query sent on a closed connection.

If this becomes an issue, we can find ways around it.

## Resharding

Given that Vitess doesn't control master management or replication, is
resharding still possible? The following features are necessary:

* being able to start / stop replication on a slave. So we can keep a slave in a
  specific state while we copy or compare data.

* have access to the current replication position, as MariaDB GTID or MySQL 5.6
  GTIDSet.

* be able to connect as a replication slave to MySQL, and start from an
  arbitrary past GTID, to support filtered replication.

`Note`: in some setups, the master for a shard is never moved around, and is
made durable by a persistent drive. It would be possible in that case to have an
alternative implementation of the Vitess GTID code using a filename / file
position. Filtered replication would always need to connect to the master of the
source shard then. Let us know if you are in that situation, it is not a lot of
work to implement.
