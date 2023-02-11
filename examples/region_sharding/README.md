# Instructions

Detailed instructions for running this example can be found at https://vitess.io.
This document contains the summary of the commands to be run.


```
# setup environment and aliases
source ../common/env.sh

# Bring up initial cluster and main keyspace (unsharded)
./101_initial_cluster.sh

# Insert and verify data
mysql < insert_customers.sql
mysql --table < show_initial_data.sql

# create schema and vschema for sharding (+lookup vindex)
./201_main_sharded.sh

# bring up shards and tablets
./202_new_tablets.sh

# reshard
./203_reshard.sh

# SwitchReads
./204_switch_reads.sh

# run script to create traffic before switching writes
#./client.sh

# SwitchWrites
./205_switch_writes.sh
# show no / minimal write errors during switch

# verify sharded data
mysql --table < show_data.sql

# down shard
./206_down_shard_0.sh

# delete shard 0
./207_delete_shard_0.sh

# Down cluster
./301_teardown.sh
```
