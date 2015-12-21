This step-by-step guide explains how to split an unsharded keyspace into two shards.
(An unsharded keyspace has exactly one shard.)
The examples assume that the keyspace is named `user_keyspace` and the shard is `0`.
The sharded keyspace will use the `user_keyspace_id` column as the keyspace ID.

You can use the same general instructions to reshard a sharded keyspace.

## Prerequisites

To complete these steps, you must have:

1. A running [keyspace](http://vitess.io/overview/concepts.html#keyspace).
   A keyspace is a logical database that maps to one or more MySQL databases.

1.  Two or more [rdonly tablets](http://vitess.io/overview/concepts.html#tablet)
    running on the source shard. You set the desired tablet type when starting
    `vttablet` with the `-target_tablet_type` flag. See the
    [vttablet-up.sh](https://github.com/youtube/vitess/blob/master/examples/local/vttablet-up.sh)
    script for example.

    During resharding, one of these tablets will pause its replication to ensure
    a consistent snapshot of the data. For this reason, you can't do resharding
    if there are only `master` and `replica` tablets, because those are reserved
    for live traffic and Vitess will never take them out of service for batch
    processes like resharding.

    Having at least two `rdonly` tablets ensures that data updates that occur on
    the source shard during the resharding process propagate to the destination
    shard. Steps 3 and 4 of the resharding process discuss this in more detail.

We recommend that you also review the
[Range-Based Sharding](http://vitess.io/user-guide/sharding.html#range-based-sharding)
section of the *Sharding* guide.

## Step 1: Define your Keyspace ID on the Source Shard

**Note:** Skip this step if your keyspace already has multiple shards.

In this step, you add a column, which will serve as the
[keyspace ID](http://vitess.io/overview/concepts.html#keyspace-id), to each
table in the soon-to-be-sharded keyspace.
After the keyspace has been sharded, Vitess will use the column's value to route
each query to the proper shard.

### Step 1.1: Add keyspace ID to each database table

For each table in the unsharded keyspace, run the following `alter` statement:

``` sh
vtctlclient -server <vtctld host:port> ApplySchema \
    -sql "alter table <table name> add <keyspace ID column>" \
    <keyspace name>
```

In this example, the command looks like this:

``` sh
vtctlclient -server <vtctld host:port> ApplySchema \
    -sql "alter table <table name> add user_keyspace_id" \
    user_keyspace
```

In the above statement, replace `user_keyspace_id` with the column name that you
want to use to store the keyspace ID value.
Also replace `user_keyspace` with the name of your keyspace.

### Step 1.2: Update tables to contain keyspace ID values

Backfill each row in each table with the appropriate keyspace ID value.
In this example, each `user_keyspace_id` column contains a 64-bit hash of the
user ID in that column's row. Using a hash ensures that user IDs are randomly
and evenly distributed across shards.

### Step 1.3: Set keyspace ID in topology server

Tell Vitess which column value identifies the keyspace ID by running the
following command:

``` sh
vtctlclient -server <vtctld host:port> \
    SetKeyspaceShardingInfo <keyspace name> <keyspace ID column> <keyspace type>
```

In this example, the command looks like this:

``` sh
vtctlclient -server <vtctld host:port> \
    SetKeyspaceShardingInfo user_keyspace user_keyspace_id uint64
```

Note that each table in the keyspace must have a column to identify the keyspace ID.
In addition, all of those columns must have the same name.

## Step 2: Prepare the destination shards

In this step, you create the destination shards and tablets.
At the end of this step, the destination shards will have been created,
but they will not contain any data and will not serve any traffic.

This example shows how to split an unsharded database into two destination shards.
As noted in the
[Key Ranges and Partitions](http://vitess.io/user-guide/sharding.html#key-ranges-and-partitions)
section, the value [ 0x80 ] is the middle value for sharding keys.
So, when you split this database into two shards, the
[range-based shard names](http://vitess.io/user-guide/sharding.html#shard-names-in-range-based-keyspaces)
for those shards will be:

* -80
* 80-

### Step 2.1: Create destination shards

To create the destination shards, call the `CreateShard` command.
You would have used the same command to create the source shard. Repeat the
following command for each shard you need to create:

``` sh
vtctlclient -server <vtctld host:port> CreateShard <keyspace name>/<shard name>
```

In this example, you would run the command twice:

``` sh
vtctlclient -server <vtctld host:port> CreateShard user_keyspace/80-
vtctlclient -server <vtctld host:port> CreateShard user_keyspace/-80
```

### Step 2.2: Create destination tablets

Start up `mysqld` and `vttablet` for the destination shards just like you did
for the source shards, but with a different `-init_shard` argument and a
different unique tablet ID (specified via `-tablet-path`).

The example [vttablet-up.sh](https://github.com/youtube/vitess/blob/master/examples/local/vttablet-up.sh)
script has parameters at the top named `shard` and `uid_base` that can be used
to make these modifications.

As with the source shard, you should have two [rdonly tablets](http://vitess.io/overview/concepts.html#tablet)
on each of the destination shards. The `tablet_type` parameter at the top of
`vttablet-up.sh` can be used to set this.

### Step 2.3: Initialize replication on destination shards

Next call the `InitShardMaster` command to initialize MySQL replication in each destination shard.
You would have used the same commands to elect the master tablet on the source shard.

``` sh
vtctlclient -server <vtctld host:port> \
    InitShardMaster -force <keyspace name>/<shard name> <tablet alias>
```

In this example, you would run these commands:

``` sh
vtctlclient -server <vtctld host:port> \
    InitShardMaster -force user_keyspace/-80 <tablet alias>
vtctlclient -server <vtctld host:port> \
    InitShardMaster -force user_keyspace/80- <tablet alias>
```

## Step 3: Clone data to the destination shards

In this step, you copy the database schema to each destination shard.
Then you copy the data to the destination shards. At the end of this
step, the destination tablets will be populated with data but will not
yet be serving traffic.

### Step 3.1: Copy schema to destination shards

Call the `CopySchemaShard` command to copy the database schema
from a rdonly tablet on the source shard to the destination shards:

``` sh
vtctlclient -server <vtctld host:port> CopySchemaShard \
    <keyspace>/<source shard> \
    <keyspace>/<destination shard>
```

In this example, you would run these two commands:

``` sh
vtctlclient -server <vtctld host:port> \
    CopySchemaShard user_keyspace/0 user_keyspace/-80
vtctlclient -server <vtctld host:port> \
    CopySchemaShard user_keyspace/0 user_keyspace/80-
```

### Step 3.2: Copy data from source shard to destination shards

This step uses a `vtworker` process to copy data from the source shard
to the destination shards. The `vtworker` performs the following tasks:

1. It finds a `rdonly` tablet on the source shard and stops data
   replication on the tablet. This prevents the data from changing
   while it is being copied. During this time, the `rdonly` tablet's
   status is changed to `worker`, and Vitess will stop routing app
   traffic to it since it might not have up-to-date data.

1. It does a (concurrent) full scan of each table on the source shard.

1. It identifies the appropriate destination shard for each source row
   based on the row's sharding key.

1. It streams the data to the master tablet on the correct destination shard.

The following command starts the `vtworker`:

```
vtworker -min_healthy_rdonly_endpoints 1 -cell=<cell name> \
    SplitClone -strategy=-populate_blp_checkpoint \
    <keyspace name>/<source shard name>
```

For this example, run this command:

```
vtworker -min_healthy_rdonly_endpoints 1 -cell=<cell name> \
    SplitClone -strategy=-populate_blp_checkpoint user_keyspace/0
```

The amount of time that the worker takes to complete will depend
on the size of your dataset. When the process completes, the destination
shards contain the correct data but do not yet serve traffic.
The destination shards are also now running
[filtered replication](http://vitess.io/user-guide/sharding.html#filtered-replication).

## Step 4: Run a data diff to verify integrity

Before the destination shard starts serving data, you want to ensure that
its data is up-to-date. Remember that the source tablet would not have
received updates to any of its records while the vtworker process was
copying data to the destination shards.

### Step 4.1: Use filtered replication to catch up to source data changes

Vitess uses [filtered replication](http://vitess.io/user-guide/sharding.html#filtered-replication) to ensure that
data changes on the source shard during step 3 propagate successfully
to the destination shards. While this process happens automatically, the
time it takes to complete depends on how long step 3 took to complete and
the scope of the data changes on the source shard during that time.

You can see the filtered replication state for a destination shard by
viewing the status page of the shard's master tablet in your browser
(the vtctld web UI will link there from the tablet's **STATUS** button).
The Binlog Player table shows a **SecondsBehindMaster** column that
indicates how far the destination master is still behind the source shard.

### Step 4.2: Compare data on source and destination shards

In this step, you use another `vtworker` process to ensure that the data
on the source and destination shards is identical. The vtworker can also
catch potential problems that might have occurred during the copying
process. For example, if the sharding key changed for a particular row
during step 3 or step 4.1, the data on the source and destination shards
might not be equal.

To start the `vtworker`, run the following `SplitDiff` command:

``` sh
vtworker -min_healthy_rdonly_endpoints=1 -cell=<cell name> \
    SplitDiff <keyspace name>/<shard name>
```

The commands for the two new destination shards in this example are shown
below. You need to complete this process for each destination shard.
However, you must remove one rdonly tablet from the source shard for each
diff process that is running. As such, it is recommended to run diffs
sequentially rather than in parallel.


``` sh
vtworker -min_healthy_rdonly_endpoints=1 -cell=<cell name> \
    SplitDiff user_keyspace/-80
vtworker -min_healthy_rdonly_endpoints=1 -cell=<cell name> \
    SplitDiff user_keyspace/80-
```

The vtworker performs the following tasks:

1. It finds a health `rdonly` tablet in the source shard and a healthy
   `rdonly` tablet in the destination shard.

1. It sets both tablets to stop serving app traffic, so data can be compared
   reliably.

1. It pauses filtered replication on the destination master tablet.

1. It pauses replication on the source `rdonly` tablet at a position higher
   than the destination master's filtered replication position.

1. It resumes the destination master's filtered replication.

1. It allows the destination `rdonly` tablet to catch up to the same position
   as the source `rdonly` tablet and then stops replication on the
   destination `rdonly` tablet.

1. It compares the schema on the source and destination `rdonly` tablets.

1. It streams data from the source and destination tablets, using the
   same sharding key constraints, and verifies that the data is equal.

If the diff is successful on the first destination shard, repeat it
on the next destination shard.

## Step 5: Direct traffic to destination shards

After verifying that your destination shards contain the correct data,
you can start serving traffic from those shards.

### Step 5.1 Migrate read-only traffic

The safest process is to migrate read-only traffic first. You will migrate
write operations in the following step, after the read-only traffic is
stable. The reason for splitting the migration into two steps is that
you can reverse the migration of read-only traffic without creating data
inconsistencies. However, you cannot reverse the migration of master
traffic without creating data inconsistencies.

Use the `MigrateServedTypes` command to migrate `rdonly` and `replica` traffic.

```
vtctlclient -server <vtctld host:port> \
    MigrateServedTypes <keyspace name>/<source shard name> rdonly
vtctlclient -server <vtctld host:port> \
    MigrateServedTypes <keyspace name>/<source shard name> replica
```

If something goes wrong during the migration of read-only traffic,
run the same commands with the `-reverse` flag to return
read-only traffic to the source shard:

```
vtctlclient -server <vtctld host:port> \
    MigrateServedTypes -reverse <keyspace name>/<source shard name> rdonly
vtctlclient -server <vtctld host:port> \
    MigrateServedTypes -reverse <keyspace name>/<source shard name> replica
```

### Step 5.2 Migrate master traffic

Use the `MigrateServedTypes` command again to migrate `master`
traffic to the destination shard:

```
vtctlclient -server <vtctld host:port> \
    MigrateServedTypes <keyspace name>/<source shard name> master
```

For this example, the command is:

```
vtctlclient -server <vtctld host:port> MigrateServedTypes user_keyspace/0 master
```

## Step 6: Scrap source shard

If all of the other steps were successful, you can remove the source
shard, which should no longer be in use.

### Step 6.1: Remove source shard tablets

Run the following command for each tablet in the source shard:

``` sh
vtctlclient -server <vtctld host:port> DeleteTablet -allow_master <source tablet alias>
```

### Step 6.2: Delete source shard

Run the following command:

``` sh
vtctlclient -server <vtctld host:port> \
    DeleteShard <keyspace name>/<source shard name>
```

For this example, the command is:

``` sh
vtctlclient -server <vtctld host:port> DeleteShard user_keyspace/0
```

