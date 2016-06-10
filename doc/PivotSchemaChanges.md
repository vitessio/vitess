# Pivot Schema Changes: A Tutorial

This page describes how to apply long-running schema changes in Vitess/MySQL without disrupting ongoing operations. Examples for long-running changes on large databases are `ALTER TABLE` (for example to add a column), `OPTIMIZE TABLE` or large-scale data changes (e.g. populating a column or clearing out values).

If a schema change is not long-running, please use the simpler [vtctl ApplySchema](/user-guide/schema-management.html) instead.

## Overview

One solution to realize such long-running schema changes is to use a temporary table and keep it in sync with triggers as [originally proposed by Shlomi](http://code.openark.org/blog/mysql/online-alter-table-now-available-in-openark-kit) and further refined by others ([Percona's pt-online-schema-change](https://www.percona.com/doc/percona-toolkit/2.2/pt-online-schema-change.html), [Square's Shift](https://github.com/square/shift)).

Here we describe an alternative solution which uses a combination of MySQL's statement based replication and backups to apply the changes to all tablets. Since the long-running schema changes are applied to an offline tablet, ongoing operations are not affected. Internally within Google this process is dubbed **pivot** and therefore we refer to it by this name throughout the document.

This tutorial outlines the necessary manual steps for a pivot and is based on the [Vitess Kubernetes Getting Started Guide](http://vitess.io/getting-started/). Eventually, we plan to provide an integrated solution within Vitess which will automate the steps for you.

**At the high level, a pivot comprises the following phases:**

1. Apply the schema changes to an offline tablet.
1. Re-enable replication on the tablet, let it catch up and then create a backup of it.
1. Restore all remaining tablets (excluding the master) from the backup.
1. Failover the master to a replica tablet which has the new schema. Restore the old master from the backup.
1. At this point, all tablets have the new schema and you can start using it.

**You may be wondering: Why does this work?**

The key here is that the new schema is backward compatible with respect to statements sent by the app. The replication stream remains backward compatible as well because we use statement based replication. As a consequence, the new schema must not be used until it has been changed on all tablets. If the schema would have been used e.g. when an insert uses a new column, replication would break on tablets which have the old schema. Pivoting all tablets first ensures this doesn't happen.

Also note that the changes are applied to only one tablet and then all other tablets are restored from the backup. This is more efficient than applying the long-running changes on every single tablet.

Now let's carry out an actual pivot based on our Guestbook example schema. We'll add a column to it.

## Prerequisites

We assume that you have followed the [Vitess Kubernetes Getting Started Guide](http://vitess.io/getting-started/) up to and including the step "9. Create a table".

Furthermore, we assume that the tablet with the ID 100 is the current master.

## Remarks

Note: The example uses the `vtctl ExecuteFetchAsDba` command to read rows for demonstration purposes only. It is not recommended for daily operations since queries are executed with SUPER rights and e.g. can insert data on a read-only slave.

Throughout the tutorial many steps are marked as "optional" or meant for verification only (e.g. looking at the GTIDs with `vtctl ShardReplicationPositions`). They are not relevant for the pivot process. However, they help to explain what is happening under the hood e.g. that a tablet did catch up to the latest GTID.

## Pivot Steps

### Optional: Insert initial data

Initially, the database will be empty. Let's insert one row. Let's also compare the executed GTIDs before and after the insert.


``` sh
vitess/examples/kubernetes$ ./kvtctl.sh ShardReplicationPositions test_keyspace/0
Starting port forwarding to vtctld...
test-0000000103 test_keyspace 0 rdonly 10.64.2.5:15002 10.64.2.5:3306 [] MySQL56/c2f5cc53-239a-11e6-a17b-02420a400405:1-7 0
test-0000000104 test_keyspace 0 rdonly 10.64.1.3:15002 10.64.1.3:3306 [] MySQL56/c2f5cc53-239a-11e6-a17b-02420a400405:1-7 0
test-0000000102 test_keyspace 0 replica 10.64.3.3:15002 10.64.3.3:3306 [] MySQL56/c2f5cc53-239a-11e6-a17b-02420a400405:1-7 0
test-0000000101 test_keyspace 0 replica 10.64.0.6:15002 10.64.0.6:3306 [] MySQL56/c2f5cc53-239a-11e6-a17b-02420a400405:1-7 0
test-0000000100 test_keyspace 0 master 10.64.4.5:15002 10.64.4.5:3306 [] MySQL56/c2f5cc53-239a-11e6-a17b-02420a400405:1-7 0
```

Before the insert, the highest executed GTID is 7.

Insert a row:

``` sh
vitess/examples/kubernetes$ ./kvtctl.sh ExecuteFetchAsDba test-0000000100 "INSERT INTO messages (page, time_created_ns, message) VALUES (1, UNIX_TIMESTAMP(now()), 'before pivot')"
Starting port forwarding to vtctld...
+
+
```

Verify that it's there:

``` sh
vitess/examples/kubernetes$ ./kvtctl.sh ExecuteFetchAsDba test-0000000104 "SELECT * FROM messages"
Starting port forwarding to vtctld...
+------+-----------------+--------------+
| page | time_created_ns |   message    |
+------+-----------------+--------------+
|    1 |      1464305998 | before pivot |
+------+-----------------+--------------+
```

Check the highest executed GTID:

``` sh
vitess/examples/kubernetes$ ./kvtctl.sh ShardReplicationPositions test_keyspace/0
Starting port forwarding to vtctld...
test-0000000104 test_keyspace 0 rdonly 10.64.1.3:15002 10.64.1.3:3306 [] MySQL56/c2f5cc53-239a-11e6-a17b-02420a400405:1-8 0
test-0000000103 test_keyspace 0 rdonly 10.64.2.5:15002 10.64.2.5:3306 [] MySQL56/c2f5cc53-239a-11e6-a17b-02420a400405:1-8 0
test-0000000102 test_keyspace 0 replica 10.64.3.3:15002 10.64.3.3:3306 [] MySQL56/c2f5cc53-239a-11e6-a17b-02420a400405:1-8 0
test-0000000101 test_keyspace 0 replica 10.64.0.6:15002 10.64.0.6:3306 [] MySQL56/c2f5cc53-239a-11e6-a17b-02420a400405:1-8 0
test-0000000100 test_keyspace 0 master 10.64.4.5:15002 10.64.4.5:3306 [] MySQL56/c2f5cc53-239a-11e6-a17b-02420a400405:1-8 0
```

The GTID increased by one from 7 to 8 and all replicas catched up to it.

### Apply the schema to an offline tablet

Let's take the *rdonly* tablet with the ID 104 offline and apply the changes there:

``` sh
vitess/examples/kubernetes$ ./kvtctl.sh StopSlave test-0000000104
Starting port forwarding to vtctld...
```

Note: If a vtctl command does not produce any output (like `StopSlave`), it means that it succeeded.

Verify that replication is turned off:

``` sh
vitess/examples/kubernetes$ ./kvtctl.sh ExecuteFetchAsDba test-0000000104 "SELECT SERVICE_STATE FROM performance_schema.replication_connection_status;"
Starting port forwarding to vtctld...
+---------------+
| SERVICE_STATE |
+---------------+
| OFF           |
+---------------+
```

`OFF` means the Slave IO thread has been stopped. The Slave SQL thread should have been turned off as well. (Replace `connection` with `applier` in the command above if you want to verify it.)

Now let's simulate that the application continues to make changes to the database while we change the schema.

Insert a second row on the master (ID 100). The offline tablet won't see this row:

``` sh
vitess/examples/kubernetes$ ./kvtctl.sh ExecuteFetchAsDba test-0000000100 "INSERT INTO messages (page, time_created_ns, message) VALUES (2, UNIX_TIMESTAMP(now()), '104 replication stopped')"
Starting port forwarding to vtctld...
+
+
```

The change has been propagated to an online *rdonly* tablet (ID 103):

``` sh
vitess/examples/kubernetes$ ./kvtctl.sh ExecuteFetchAsDba test-0000000103 "SELECT * FROM messages"
Starting port forwarding to vtctld...
+------+-----------------+-------------------------+
| page | time_created_ns |         message         |
+------+-----------------+-------------------------+
|    1 |      1464305998 | before pivot            |
|    2 |      1464306056 | 104 replication stopped |
+------+-----------------+-------------------------+
```

Our offline *rdonly* tablet (ID 104) doesn't receive it as we turned replication off:

``` sh
vitess/examples/kubernetes$ ./kvtctl.sh ExecuteFetchAsDba test-0000000104 "SELECT * FROM messages"
Starting port forwarding to vtctld...
+------+-----------------+--------------+
| page | time_created_ns |   message    |
+------+-----------------+--------------+
|    1 |      1464305998 | before pivot |
+------+-----------------+--------------+
```

We can also see that when we look at the executed GTIDs of all tablets:

``` sh
vitess/examples/kubernetes$ ./kvtctl.sh ShardReplicationPositions test_keyspace/0
Starting port forwarding to vtctld...
test-0000000103 test_keyspace 0 rdonly 10.64.2.5:15002 10.64.2.5:3306 [] MySQL56/c2f5cc53-239a-11e6-a17b-02420a400405:1-9 0
test-0000000104 test_keyspace 0 rdonly 10.64.1.3:15002 10.64.1.3:3306 [] MySQL56/c2f5cc53-239a-11e6-a17b-02420a400405:1-8 0
test-0000000101 test_keyspace 0 replica 10.64.0.6:15002 10.64.0.6:3306 [] MySQL56/c2f5cc53-239a-11e6-a17b-02420a400405:1-9 0
test-0000000102 test_keyspace 0 replica 10.64.3.3:15002 10.64.3.3:3306 [] MySQL56/c2f5cc53-239a-11e6-a17b-02420a400405:1-9 0
test-0000000100 test_keyspace 0 master 10.64.4.5:15002 10.64.4.5:3306 [] MySQL56/c2f5cc53-239a-11e6-a17b-02420a400405:1-9 0
```

Note that the tablet with the ID 104 is lagging behind by one transaction now (8 instead of 9) because we stopped replication on it.

Apply a schema change on the *rdonly* tablet. We'll add the column `views` to the `messages` table.

Since the tablet is a slave and read-only, we will have to use `ExecuteFetchAsDba` to execute the SQL statement with SUPER rights.


``` sh
vitess/examples/kubernetes$ ./kvtctl.sh ExecuteFetchAsDba -disable_binlogs test-0000000104 "ALTER TABLE messages ADD views BIGINT(20) UNSIGNED NULL"
Starting port forwarding to vtctld...
+
+
```

Let's check the highest GTID on each tablet.

``` sh
vitess/examples/kubernetes$ ./kvtctl.sh ShardReplicationPositions test_keyspace/0
Starting port forwarding to vtctld...
test-0000000103 test_keyspace 0 rdonly 10.64.2.5:15002 10.64.2.5:3306 [] MySQL56/c2f5cc53-239a-11e6-a17b-02420a400405:1-9 0
test-0000000104 test_keyspace 0 rdonly 10.64.1.3:15002 10.64.1.3:3306 [] MySQL56/c2f5cc53-239a-11e6-a17b-02420a400405:1-8 0
test-0000000101 test_keyspace 0 replica 10.64.0.6:15002 10.64.0.6:3306 [] MySQL56/c2f5cc53-239a-11e6-a17b-02420a400405:1-9 0
test-0000000102 test_keyspace 0 replica 10.64.3.3:15002 10.64.3.3:3306 [] MySQL56/c2f5cc53-239a-11e6-a17b-02420a400405:1-9 0
test-0000000100 test_keyspace 0 master 10.64.4.5:15002 10.64.4.5:3306 [] MySQL56/c2f5cc53-239a-11e6-a17b-02420a400405:1-9 0
```

During the pivot it's important that the schema change won't be added to the binlogs. Therefore, we ran ExecuteFetchAsDba with the `-disable_binlogs` flags. If we don't skip the binlogs, we would create an alternate future on the offline tablet.

You can see that the schema change DDL was not added to the binlogs because the GTID did not increase on tablet 104 (it stayed at 8).

Verify that the schema has been changed:

``` sh
vitess/examples/kubernetes$ ./kvtctl.sh ExecuteFetchAsDba test-0000000104 "SHOW COLUMNS FROM messages"
Starting port forwarding to vtctld...
+-----------------+---------------------+------+-----+---------+-------+
|      Field      |        Type         | Null | Key | Default | Extra |
+-----------------+---------------------+------+-----+---------+-------+
| page            | bigint(20) unsigned | NO   | PRI |         |       |
| time_created_ns | bigint(20) unsigned | NO   | PRI |         |       |
| message         | varchar(10000)      | YES  |     |         |       |
| views           | bigint(20) unsigned | YES  |     |         |       |
+-----------------+---------------------+------+-----+---------+-------+
```

### Re-enable replication on the offline tablet

Before we create the backup, we'll restart replication first and let the tablet catch up. This way, we minimize the number of new ongoing writes which won't be in the backup and have to be applied after the restore.

``` sh
vitess/examples/kubernetes$ ./kvtctl.sh StartSlave test-0000000104
Starting port forwarding to vtctld...
```

Verify that replication is running again:

``` sh
vitess/examples/kubernetes$ ./kvtctl.sh ExecuteFetchAsDba test-0000000104 "SELECT SERVICE_STATE FROM performance_schema.replication_connection_status;"
Starting port forwarding to vtctld...
+---------------+
| SERVICE_STATE |
+---------------+
| ON            |
+---------------+
```

Once tablet 104 has caught up, it should include the second row as well:

``` sh
vitess/examples/kubernetes$ ./kvtctl.sh ExecuteFetchAsDba test-0000000104 "SELECT * FROM messages"
Starting port forwarding to vtctld...
+------+-----------------+-------------------------+-------+
| page | time_created_ns |         message         | views |
+------+-----------------+-------------------------+-------+
|    1 |      1464305998 | before pivot            |       |
|    2 |      1464306056 | 104 replication stopped |       |
+------+-----------------+-------------------------+-------+
```

Note it includes our newly added column `views` and the second row as well.

When we check the executed GTIDs, we also see that tablet 104 has caught up:

``` sh
vitess/examples/kubernetes$ ./kvtctl.sh ShardReplicationPositions test_keyspace/0
Starting port forwarding to vtctld...
test-0000000104 test_keyspace 0 rdonly 10.64.1.3:15002 10.64.1.3:3306 [] MySQL56/c2f5cc53-239a-11e6-a17b-02420a400405:1-9 0
test-0000000103 test_keyspace 0 rdonly 10.64.2.5:15002 10.64.2.5:3306 [] MySQL56/c2f5cc53-239a-11e6-a17b-02420a400405:1-9 0
test-0000000101 test_keyspace 0 replica 10.64.0.6:15002 10.64.0.6:3306 [] MySQL56/c2f5cc53-239a-11e6-a17b-02420a400405:1-9 0
test-0000000102 test_keyspace 0 replica 10.64.3.3:15002 10.64.3.3:3306 [] MySQL56/c2f5cc53-239a-11e6-a17b-02420a400405:1-9 0
test-0000000100 test_keyspace 0 master 10.64.4.5:15002 10.64.4.5:3306 [] MySQL56/c2f5cc53-239a-11e6-a17b-02420a400405:1-9 0
```

### Create a backup of the offline tablet

Now we'll create a backup of the tablet which has the schema change and restore the other tablets from this backup:

``` sh
vitess/examples/kubernetes$ ./kvtctl.sh Backup test-0000000104
Starting port forwarding to vtctld...
```

When the command returns, the backup will be available:

``` sh
vitess/examples/kubernetes$ ./kvtctl.sh ListBackups test_keyspace/0
Starting port forwarding to vtctld...
2016-05-27.001231.test-0000000104
```

### Restore the slave tablets from the backup

We restore the tablets from the backup by restarting them. Since the Kubernetes pods lose their state after a shutdown, they'll always restore from the latest backup and then connect to the master to catch up on changes which were made after the backup was taken.

``` sh
vitess/examples/kubernetes$ kubectl delete pod vttablet-103
pod "vttablet-103" deleted
```

Note that the shutdown will take several seconds. While it's shutting down, Kubernetes tracks its state as `Terminating`:

``` sh
vitess/examples/kubernetes$ kubectl get pods
...
vttablet-103        2/2       Terminating   1          18m
...
```

Once it's gone, you can restart it:

``` sh
vitess/examples/kubernetes$ TASKS=3 ./vttablet-up.sh
Creating test_keyspace.shard-0 pods in cell test...
Creating pod for tablet test-0000000103...
pod "vttablet-103" created
```

The restore from backup will take several seconds. While a tablet is restoring, Vitess tracks its state as `restore`: 

``` sh
vitess/examples/kubernetes$ ./kvtctl.sh ListAllTablets test
Starting port forwarding to vtctld...
...
test-0000000103 test_keyspace 0 restore 10.64.2.5:15002 10.64.2.5:3306 []
...
```

Eventually, you can query the restarted tablet and check if it has the added column as well.

``` sh
vitess/examples/kubernetes$ ./kvtctl.sh ExecuteFetchAsDba test-0000000103 "SELECT * FROM messages"
Starting port forwarding to vtctld...
+------+-----------------+-------------------------+-------+
| page | time_created_ns |         message         | views |
+------+-----------------+-------------------------+-------+
|    1 |      1464305998 | before pivot            |       |
|    2 |      1464306056 | 104 replication stopped |       |
+------+-----------------+-------------------------+-------+
```

Now repeat the process for the remaining two *replica* tablets:

``` sh
vitess/examples/kubernetes$ kubectl delete pod vttablet-102
vitess/examples/kubernetes$ TASKS=2 ./vttablet-up.sh
```

``` sh
vitess/examples/kubernetes$ kubectl delete pod vttablet-101
vitess/examples/kubernetes$ TASKS=1 ./vttablet-up.sh
```

We recommend to restart at most one or two tablets simultaneously to not impact the overall availability of the system. (For example, be aware that an emergency failover of the master might happen during the restarts.)

### Restore the master tablet from the backup

To recap: tablet 100 is the current master and was not restored yet. All other tablets have already been restored. You can verify this with the `ListAllTablets` command:

``` sh
vitess/examples/kubernetes$ ./kvtctl.sh ListAllTablets test
Starting port forwarding to vtctld...
test-0000000100 test_keyspace 0 master 10.64.4.5:15002 10.64.4.5:3306 []
test-0000000101 test_keyspace 0 replica 10.64.0.6:15002 10.64.0.6:3306 []
test-0000000102 test_keyspace 0 replica 10.64.3.3:15002 10.64.3.3:3306 []
test-0000000103 test_keyspace 0 rdonly 10.64.2.5:15002 10.64.2.5:3306 []
test-0000000104 test_keyspace 0 rdonly 10.64.1.3:15002 10.64.1.3:3306 []
```

To restore tablet 100 as well, we'll failover the master to tablet 101 first:

``` sh
vitess/examples/kubernetes$ ./kvtctl.sh PlannedReparentShard test_keyspace/0 test-0000000101
Starting port forwarding to vtctld...
```

`ListAllTablets` will show that tablet 101 is the new master.

``` sh
vitess/examples/kubernetes$ ./kvtctl.sh ListAllTablets test
Starting port forwarding to vtctld...
test-0000000100 test_keyspace 0 replica 10.64.4.5:15002 10.64.4.5:3306 []
test-0000000101 test_keyspace 0 master 10.64.0.6:15002 10.64.0.6:3306 []
test-0000000102 test_keyspace 0 replica 10.64.3.3:15002 10.64.3.3:3306 []
test-0000000103 test_keyspace 0 rdonly 10.64.2.5:15002 10.64.2.5:3306 []
test-0000000104 test_keyspace 0 rdonly 10.64.1.3:15002 10.64.1.3:3306 []
```

When we check the GTIDs, we can see that the highest GTID for the new master is 3:

``` sh
vitess/examples/kubernetes$ ./kvtctl.sh ShardReplicationPositions test_keyspace/0
Starting port forwarding to vtctld...
test-0000000103 test_keyspace 0 rdonly 10.64.2.5:15002 10.64.2.5:3306 [] MySQL56/c2f5cc53-239a-11e6-a17b-02420a400405:1-9,c3de87c6-239a-11e6-a20e-02420a400006:1-3 0
test-0000000104 test_keyspace 0 rdonly 10.64.1.3:15002 10.64.1.3:3306 [] MySQL56/c2f5cc53-239a-11e6-a17b-02420a400405:1-9,c3de87c6-239a-11e6-a20e-02420a400006:1-3 0
test-0000000100 test_keyspace 0 replica 10.64.4.5:15002 10.64.4.5:3306 [] MySQL56/c2f5cc53-239a-11e6-a17b-02420a400405:1-9,c3de87c6-239a-11e6-a20e-02420a400006:1-3 0
test-0000000102 test_keyspace 0 replica 10.64.3.3:15002 10.64.3.3:3306 [] MySQL56/c2f5cc53-239a-11e6-a17b-02420a400405:1-9,c3de87c6-239a-11e6-a20e-02420a400006:1-3 0
test-0000000101 test_keyspace 0 master 10.64.0.6:15002 10.64.0.6:3306 [] MySQL56/c2f5cc53-239a-11e6-a17b-02420a400405:1-9,c3de87c6-239a-11e6-a20e-02420a400006:1-3 0
```

You see that the list of GTIDs has one more entry because the master has changed.

**Note:** There is currently a known issue that a vttablet does not properly restore from a backup when a reparent happened in the meantime. To work around the issue, you'll have to create a new backup after the reparent to the new master.

``` sh
vitess/examples/kubernetes$ ./kvtctl.sh Backup test-0000000104
Starting port forwarding to vtctld...
```

Now restart the old master:

``` sh
vitess/examples/kubernetes$ kubectl delete pod vttablet-100
vitess/examples/kubernetes$ TASKS=0 ./vttablet-up.sh
```

Once the old master restored, all tablets have been changed to the new schema and we can start using it.

### Use the new schema

At this point, you can change your application to use the new schema. We simulate this by inserting a third row which references the newly added column `views`:

``` sh
vitess/examples/kubernetes$ ./kvtctl.sh ExecuteFetchAsDba test-0000000101 "INSERT INTO messages (page, time_created_ns, message, views) VALUES (3, UNIX_TIMESTAMP(now()), 'after pivot', 301)"
Starting port forwarding to vtctld...
+
+
```

The row will be replicated to all tablets e.g. the old master:

``` sh
vitess/examples/kubernetes$ ./kvtctl.sh ExecuteFetchAsDba test-0000000100 "SELECT * FROM messages"
Starting port forwarding to vtctld...
+------+-----------------+-------------------------+-------+
| page | time_created_ns |         message         | views |
+------+-----------------+-------------------------+-------+
|    1 |      1464823680 | before pivot            |       |
|    2 |      1464823795 | 104 replication stopped |       |
|    3 |      1464824695 | after pivot             |   301 |
+------+-----------------+-------------------------+-------+
```

The highest executed GTID was increased accordingly on all tablets from 3 to 4:

``` sh
vitess/examples/kubernetes$ ./kvtctl.sh ShardReplicationPositions test_keyspace/0
Starting port forwarding to vtctld...
test-0000000104 test_keyspace 0 rdonly 10.64.1.3:15002 10.64.1.3:3306 [] MySQL56/5347b3ed-2850-11e6-92a9-02420a400405:1-9,f20c5e25-2851-11e6-bb74-02420a400006:1-4 0
test-0000000103 test_keyspace 0 rdonly 10.64.2.5:15002 10.64.2.5:3306 [] MySQL56/5347b3ed-2850-11e6-92a9-02420a400405:1-9,f20c5e25-2851-11e6-bb74-02420a400006:1-4 0
test-0000000102 test_keyspace 0 replica 10.64.3.3:15002 10.64.3.3:3306 [] MySQL56/5347b3ed-2850-11e6-92a9-02420a400405:1-9,f20c5e25-2851-11e6-bb74-02420a400006:1-4 0
test-0000000100 test_keyspace 0 replica 10.64.4.5:15002 10.64.4.5:3306 [] MySQL56/5347b3ed-2850-11e6-92a9-02420a400405:1-9,f20c5e25-2851-11e6-bb74-02420a400006:1-4 0
test-0000000101 test_keyspace 0 master 10.64.0.6:15002 10.64.0.6:3306 [] MySQL56/5347b3ed-2850-11e6-92a9-02420a400405:1-9,f20c5e25-2851-11e6-bb74-02420a400006:1-4 0
```
