This step-by-step guide explains how to split an unsharded keyspace into two shards. (An unsharded keyspace has exactly one shard.) The examples assume that the keyspace is named <code>user_keyspace</code> and the shard is <code>0</code>. The sharded keyspace will use the <code>user_keyspace_id</code> column as the keyspace ID.

You can use the same general instructions to reshard a sharded keyspace.

## Prerequisites

To complete these steps, you must have:

1. A running <a href="/overview/concepts.html#keyspace">keyspace</a>. A keyspace is a logical database that maps to one or more MySQL databases.
1. Two or more <code>rdonly</code> tablets running on the source shard.<br><br>
    Having at least two <code>rdonly</code> tablets ensures that data updates that occur on the source shard during the resharding process propagate to the destination shard. Steps 3 and 4 of the resharding process discuss this in more detail.

We recommend that you also review the [range-based sharding](/user-guide/sharding.html#range-based-sharding) section of the <i>Sharding</i> guide.

## Step 1: Define your Keyspace ID on the Source Shard
###### Note: Skip this step if your keyspace already has multiple shards.

In this step, you add a column, which will serve as the <a href="/overview/concepts#keyspace-id">keyspace ID</a> (or sharding key), to each table in the soon-to-be-sharded keyspace. After the keyspace has been sharded, Vitess will use the column's value to route each query to the proper shard. 

### Step 1.1: Add the Keyspace ID to each database table

For each table in the unsharded keyspace, run the following <code>alter</code> statement:

``` sh
vtctl ApplySchema \
      -sql="alter table <table name> add <keyspace ID column>" \
      <keyspace name>
```

In this example, the command looks like this:

``` sh
vtctl ApplySchema \
      -sql="alter table <table name> add user_keyspace_id" \
      user_keyspace
```

In the above statement, replace <code>user_keyspace_id</code> with the column name that you want to use to store the keyspace ID value. Also replace <code>user_keyspace</code> with the name of your keyspace.

### Step 1.2: Update tables to contain keyspace ID values

Backfill each row in each table with the appropriate keyspace ID value. In this example, each <code>user_keyspace_id</code> column contains a 64-bit hash of the user ID in that column's row.

### Step 1.3: Set the Keyspace ID in the Topology Server

Tell Vitess which column value identifies the keyspace ID by running the following command:

``` sh
vtctl SetKeyspaceShardingInfo <keyspace name> \
      <keyspace ID column> <keyspace type>
```

In this example, the command looks like this:

``` sh
vtctl SetKeyspaceShardingInfo user_keyspace user_keyspace_id uint64
```

Note that each table in the keyspace must have a column to identify the keyspace ID. In addition, all of those columns must have the same name.

## Step 2: Prepare the destination shards

In this step, you create the destination shards and tablets. At the end of this step, the destination shards will have been created, but they will not contain any data and will not serve any traffic.

This example shows how to split an unsharded database into two destination shards. As noted in the [sharding keys](sharding-keys) section, the value [ 0x80 ] is the middle value for sharding keys. So, when you split this database into two shards, the [range-based shard names](#range-based-shard-names) for those shards will be:

* -80
* 80-

### Step 2.1: Create the destination shards

To create the destination shards, call the <code>CreateShard</code> command.
You would have used the same command to create the source shard. Repeat the
following command for each shard you need to create:

``` sh
vtctl CreateShard <keyspace name>/<shard name>
```

In this example, you would run the command twice:

``` sh
vtctl CreateShard user_keyspace/80-
vtctl CreateShard user_keyspace/-80
```

### Step 2.2: Create the destination tablets

To create the destination master tablets, call the
<code>InitTablet</code> command. Then call the <code>InitShardMaster</code>
command to initialize MySQL replication in each destination shard.
You would have used the same commands to create the master tablet on the
source shard.

``` sh
vtctl InitTablet -keyspace=<keyspace name> -shard=<shard name> \
                 <tablet alias> <tablet type>
vtctl InitShardMaster -force <keyspace name>/<shard name> \
                      <tablet alias>
```

In this example, you would run these commands:

``` sh
vtctl InitTablet -keyspace=user_keyspace -shard=-80 <tablet alias> \
                 master
vtctl InitShardMaster -force user_keyspace/-80 <tablet alias>

vtctl InitTablet -keyspace=user_keyspace -shard=user_keyspace/80- \
                 <tablet alias> master
vtctl InitShardMaster -force user_keyspace/80- <tablet alias>
```

### Step 2.3: Create rdonly tablets in each destination shard

Each destination shard should have two rdonly tablets. Again, use the
<code>InitTablet</code> command to create each tablet.

In this example, you would run these commands to create two rdonly tablets
in each of the two destination shards:

```
vtctl InitTablet -keyspace=user_keyspace -shard=-80 <tablet alias> \
                 rdonly
vtctl InitTablet -keyspace=user_keyspace -shard=-80 <tablet alias> \
                 rdonly

vtctl InitTablet -keyspace=user_keyspace -shard=80- <tablet alias> \
                 rdonly
vtctl InitTablet -keyspace=user_keyspace -shard=80- <tablet alias> \
                 rdonly
```

## Step 3: Clone data to the destination shards

In this step, you copy the database schema to each destination shard.
Then you copy the data to the destination shards. At the end of this
step, the destination tablets will be populated with data but will not
yet be serving traffic.

### Step 3.1: Copy the schema to destination shards

Call the <code>CopySchemaShard</code> command to copy the database schema
from a rdonly tablet on the source shard to the destination shards:

``` sh
vtctl CopySchemaShard <source rdonly tablet alias> \
                      <keyspace name>/<shard name>
```

In this example, you would run these two commands:

``` sh
vtctl CopySchemaShard <source rdonly tablet alias> user_keyspace/-80
vtctl CopySchemaShard <source rdonly tablet alias> user_keyspace/80-
```

### Step 3.2: Copy data from source shard to destination shards

This step uses a vtworker process to copy data from the source shard
to the destination shards. The vtworker performs the following tasks:

1. It finds an rdonly tablet on the source shard and stops data
    replication on the tablet. This prevents the data from changing
    while it is being copied. During this time, the rdonly tablet's
    status is changed to <code>spare</code>, and the tablet is removed
    from the serving graph since it might not have up-to-date data.
1. It does a (concurrent) full scan of each table on the source shard.
1. It identifies the appropriate destination shard for each source row
    based on the row's sharding key.
1. It streams the data to the master tablet on the correct destination shard.

The following command starts the vtworker:

```
vtworker --min_healthy_rdonly_endpoints 1 --cell=<cell name> \
         SplitClone --strategy=-populate_blp_checkpoint \
         <keyspace name>/<source shard name>
```

For this example, run this command:

```
vtworker --min_healthy_rdonly_endpoints 1 --cell=<cell name> \
         SplitClone --strategy=-populate_blp_checkpoint user_keyspace/0
```

The amount of time that the worker takes to complete will depend
on the size of your dataset. When the process completes, the destination
shards contain the correct data but do not yet serve traffic.
The destination shards are also now running
[filtered replication](#filtered-replication).

### Step 3.3: Restore the rdonly tablet on the source shard

The source tablet is not automatically returned to the serving graph.
Use the following command to change the tablet's type back to
<code>rdonly</code> and have the tablet catch up on replication:

``` sh
vtctl ChangeSlaveType <source rdonly tablet alias> rdonly
```

## Step 4: Running a diff of the data to verify accuracy

Before the destination shard starts serving data, you want to ensure that
its data is up-to-date. Remember that the source tablet would not have
received updates to any of its records while the vtworker process was
copying data to the destination shards.

### Step 4.1: Catch up to source data changes via filtered replication

Vitess uses [filtered replication](#filtered-replication) to ensure that
data changes on the source shard during step 3 propagate successfully
to the destination shards. While this process happens automatically, the
time it takes to complete depends on how long step 3 took to complete and
the scope of the data changes on the source shard during that time.

You can see the filtered replication state for a destination shard by
viewing the status page of the shard's master tablet in your browser.
The Binlog Player table shows a **SecondsBehindMaster** column that
indicates how far the destination master is still behind the source shard.

### Step 4.2: Compare data on source and destination shards

In this step, you use another vtworker process to ensure that the data
on the source and destination shards is identical. The vtworker can also
catch potential problems that might have occurred during the copying
process. For example, if the sharding key changed for a particular row
during step 3 or step 4.1, the data on the source and destination shards
might not be equal.

To start the vtworker, run the following <code>SplitDiff</code> command:

``` sh
vtworker -min_healthy_rdonly_endpoints=1 --cell=<cell name> \
         SplitDiff <keyspace name>/<shard name>
```

The command for the first new destination shard in this example is shown
below. You need to complete this process for each destination shard.
However, you must remove one rdonly tablet from the source shard for each
diff process that is running. As such, it is recommended to run diffs
sequentially rather than in parallel.  


``` sh
vtworker -min_healthy_rdonly_endpoints=1 --cell=<cell name> \
         SplitDiff user_keyspace/-80
```

The vtworker performs the following tasks:

1. It finds a health rdonly tablet in the source shard and a healthy
    rdonly tablet in the destination shard.
1. It removes both tablets from the serving graph so data can be compared
    reliably.
1. It pauses filtered replication on the destination master tablet.
1. It pauses replication on the source rdonly tablet at a position higher
    than the destination master's filtered replication position.
1. It resumes the destination master's filtered replication.
1. It allows the destination rdonly tablet to catch up to the same position
    as the source rdonly tablet and then stops replication on the
    destination rdonly tablet.
1. It compares the schema on the source and destination rdonly tablets.
1. It streams data from the source and destination tablets, using the
    same sharding key constraints, and verifies that the data is equal.

If the diff is successful on the first destination shard, repeat it
on the next destination shard. 

### Step 4.3: Return tablets to the serving graph

Put the source and destination rdonly tablets used in step 4.2 back into
the serving graph:

```
vtctl ChangeSlaveType <source rdonly tablet alias> rdonly
vtctl ChangeSlaveType <destination rdonly tablet alias> rdonly
```

## Step 5: Direct traffic to destination shards

After verifying that your destination shards contain the correct data, you can start serving traffic from those shards.

### Step 5.1 Migrate read-only traffic

The safest process is to migrate read-only traffic first. You will migrate
write operations in the following step, after the read-only traffic is
stable. The reason for splitting the migration into two steps is that
you can reverse the migration of read-only traffic without creating data
inconsistencies. However, you cannot reverse the migration of master
traffic without creating data inconsistencies.

Use the <code>MigrateServedTypes</code> command to migrate **rdonly**
and **replica** traffic:

```
vtctl MigrateServedTypes <keyspace name>/<source shard name> rdonly
vtctl MigrateServedTypes <keyspace name>/<source shard name> replica
```

If something goes wrong during the migration of read-only traffic,
run the same commands with the <code>reverse</code> flag to return
read-only traffic to the source shard:

```
vtctl MigrateServedTypes -reverse <keyspace name>/<source shard name> \
                         rdonly
vtctl MigrateServedTypes -reverse <keyspace name>/<source shard name> \
                         replica
```

### Step 5.2 Migrate master traffic

Use the <code>MigrateServedTypes</code> command again to migrate **master**
traffic to the destination shard:

```
vtctl MigrateServedTypes <keyspace name>/<source shard name> master
```

For this example, the command is:

```
vtctl MigrateServedTypes user_keyspace/0 master
```

## Step 6: Scrap the source shard

If all of the other steps were successful, you can remove the source
shard, which should no longer be in use.

### Step 6.1: Remove source shard tablets

Run the following commands for each tablet in the source shard:

``` sh
vtctl ScrapTablet <source tablet alias>`
vtctl DeleteTablet <source tablet alias>`
```

### Step 6.2: Rebuild the serving graph

Run the following command:

``` sh
vtctl RebuildKeyspaceGraph <keyspace name>
```

For this example, the command is:

``` sh
vtctl RebuildKeyspaceGraph user_keyspace
```

### Step 6.3: Delete the source shard

Run the following command:

``` sh
vtctl DeleteShard <keyspace name>/<source shard name>
```

For this example, the command is:

``` sh
vtctl DeleteShard user_keyspace/0
```
