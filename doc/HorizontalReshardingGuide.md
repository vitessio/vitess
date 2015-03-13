# Horizontal resharding guide

This is a step-by-step "cookbook" style guide of instructions to follow in order to split a single-sharded keyspace into two.

## Preparing the source shard

Let’s assume that you’ve already got a keyspace up and running, with a single shard (the default case). For example, you might have the following keyspace/shard: `test_keyspace/0`. It’s also a requirement that you have at least one (but preferably two) rdonly tablets for the source shard.

The first thing that we need to do is add a column to the soon-to-be-sharded keyspace which will be used as the "sharding key". This column will tell Vitess which shard a particular row of data should go to. You can add the column by running an alter on the unsharded keyspace - probably by running something like:

`vtctl ApplySchemaKeyspace -simple -sql="alter table <table name> add keyspace_id" test_keyspace`

for each table in the keyspace. Once the column is added everywhere, each row needs to be backfilled with the appropriate keyspace ID.

Once we have a column that we can use as the sharding key, we need to tell Vitess what we’ll use as the sharding key for a particular keyspace. This can be done with, for example:

`vtctl SetKeyspaceShardingInfo test_keyspace keyspace_id uint64`

## Preparing the destination shards

Create destination shards and tablets just like the source shard was created. For example, you might have `test_keyspace/-80` and `test_keyspace/80-`, if you were splitting the original shard into two shards.

You will need to make sure that each destination shard has a master tablet, and at least one (but preferably two) rdonly tablets. It is necessary to reparent each destination shard, after creating the master tablet, to choose a master. This can be done by, for example:

```
vtctl ReparentShard -force test_keyspace/-80 <master tablet alias>
vtctl ReparentShard -force test_keyspace/80- <master tablet alias>
```

At this point, the destination shards should be created, but do not have any data, and will not be serving any traffic.

## Cloning the data into the destination shards

Before copying the data from the source to the destination, it’s necessary to copy the schema to each destination shard. This can be done easily by running, for example:

```
vtctl CopySchemaShard <source rdonly tablet alias> test_keyspace/-80
vtctl CopySchemaShard <source rdonly tablet alias> test_keyspace/80-
```

Next, we will copy the data from the source shard and split it into the correct destination shards. This is done by running a vtworker process, which does the following:

1. Finds an rdonly tablet on the source shard and "freezes" it in place. This prevents the data from changing while it is being copied. However, it also means that the rdonly tablet is removed from the serving graph, as live updates to the source shard will not propagate to it. This is why it’s recommended to have at least two rdonly tablets on the source shard.
2. Does a (concurrent) full scan of each source shard table.
3. Looks at each source row’s sharding key, and chooses the appropriate destination shard.
4. Streams the data to the correct destination shard master.

The vtworker can be started by, for example:

```
vtworker --min_healthy_rdonly_endpoints 1 SplitClone --cell=test --strategy=-populate_blp_checkpoint test_keyspace/0
```

The worker will take some time, depending on how large your dataset is that needs to be copied.

Once it returns successfully, the destination shards should have the correct data, but still won’t be serving any traffic. The source rdonly that was used will not be returned to the serving graph automatically. The following command can be run to change its type from spare to rdonly, and have it start catching up on replication:

`vtctl ChangeSlaveType <rdonly tablet alias> rdonly`

The destination shards will also now be running [filtered replication](Resharding.md#filtered-replication).

## Running a diff of the data to verify accuracy

##### Note: This step is optional, but highly recommended.

Now that the data has been copied (up to a certain point in time) to the destination shard, we will still want to do a couple of things before serving data from the destination shard:

1. Catch up on [filtered replication](Resharding.md#filtered-replication). Since we copied the data from a frozen point in time, we will need to now wait for the data changes that were applied to the source shard to propagate to the destination shards. This should happen automatically, but will take some time (depending on how long the copy took, and how many data changes there were since then). It should also be possible to see the state of filtered replication by viewing the status page of a destination master tablet in your browser. For example: `http://localhost:<destination master tablet port>/debug/status` will show a SecondsBehindMaster column in the Binlog Player table.
2. Compare a destination shard to the source shard, and make sure that the data on each side matches. This is a safeguard to catch potential problems that may have occurred during the copying process (for example, if the sharding key was changed for a particular row after the copy step was run).

The vtworker process has a diff command which can accomplish both of the above steps. It does the following (for each destination shard):

1. Finds a healthy rdonly tablet in the source shard, and a healthy rdonly tablet in the destination shard.
2. Removes both tablets from the serving graph, as they will be used to compare data between them.
3. Pauses filtered replication on the destination master tablet.
4. Pauses replication on the source rdonly tablet, at a position higher than the destination master’s filtered replication position.
5. Resumes the destination master’s filtered replication
6. Catches up the destination rdonly to the exact same position as the source rdonly, and then stops replication on it.
7. Compares schema between the source and destination
8. Streams data from both source and destination (with the same sharding key constraints), and verifies that it’s equal

A single SplitDiff vtworker can be run by, for example:

`vtworker -min_healthy_rdonly_endpoints=1 --cell=test SplitDiff test_keyspace/-80`

After completion, the source and destination rdonly tablets need to be put back into the serving graph:
```
vtctl ChangeSlaveType <source rdonly tablet alias> rdonly
vtctl ChangeSlaveType <destination rdonly tablet alias> rdonly
```

If the diff is successful on the first destination shard, it should be be repeated on the next one. It is recommended to not run multiple diffs in parallel, unless you are willing to remove multiple rdonly tablets from the source shard at the same time:

```
vtworker -min_healthy_rdonly_endpoints=1 --cell=test SplitDiff test_keyspace/80-
vtctl ChangeSlaveType <source rdonly tablet alias> rdonly
vtctl ChangeSlaveType <destination rdonly tablet alias> rdonly
```

## Serve data from the destination shards

Now that you’ve verified the destination shards have the data you want, we can start serving traffic from the destination shards.

The safest way to do this is to migrate read only traffic first, and only migrate masters once the read only traffic is stabilized. Migrating the master traffic is not reversible without data inconsistencies, so it’s best to be careful.

When ready, you can migrate each tier of traffic with the following commands:

```
vtctl MigrateServedTypes test_keyspace/0 rdonly
vtctl MigrateServedTypes test_keyspace/0 replica
vtctl MigrateServedTypes test_keyspace/0 master
```

If something goes wrong during the migration of read only traffic, those can be reverted:

```
vtctl MigrateServedTypes -reverse test_keyspace/0 rdonly
vtctl MigrateServedTypes -reverse test_keyspace/0 replica
```

## Scrap the source shard

If all the above steps were successful, it’s safe to remove the source shard (which should no longer be in use):
- For each tablet in the source shard: `vtctl ScrapTablet <source tablet alias>`
- For each tablet in the source shard: `vtctl DeleteTablet <source tablet alias>`
- Rebuild the serving graph: `vtctl RebuildKeyspaceGraph test_keyspace`
- Delete the source shard: `vtctl DeleteShard test_keyspace/0`
