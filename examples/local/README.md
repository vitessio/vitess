# Instructions

Detailed instructions for running this example can be found at https://vitess.io.
This document contains the summary of the commands to be run.


```
# Setup environment and aliases
source ../common/env.sh

# Bring up initial cluster and commerce keyspace
./101_initial_cluster.sh

# Insert and verify data
mysql < ../common/insert_commerce_data.sql
mysql --table < ../common/select_commerce_data.sql

# Bring up customer keyspace
./201_customer_tablets.sh

# Initiate move tables
vtctldclient MoveTables --workflow commerce2customer --target-keyspace customer create --source-keyspace commerce --tables "customer,corder"

# Validate
vtctldclient vdiff --workflow commerce2customer --target-keyspace customer create
vtctldclient vdiff --workflow commerce2customer --target-keyspace customer show last

# Cut-over
vtctldclient MoveTables --workflow commerce2customer --target-keyspace customer switchtraffic --tablet-types "rdonly,replica"
vtctldclient MoveTables --workflow commerce2customer --target-keyspace customer switchtraffic --tablet-types primary

# Clean-up
vtctldclient MoveTables --workflow commerce2customer --target-keyspace customer complete

# Prepare for resharding
./301_customer_sharded.sh
./302_new_shards.sh

# Reshard
vtctldclient Reshard --workflow cust2cust --target-keyspace customer create --source-shards '0' --target-shards '-80,80-'

# Validate
vtctldclient vdiff --workflow cust2cust --target-keyspace customer create
vtctldclient vdiff --workflow cust2cust --target-keyspace customer show last

# Cut-over
vtctldclient Reshard --workflow cust2cust --target-keyspace customer switchtraffic --tablet-types "rdonly,replica"
vtctldclient Reshard --workflow cust2cust --target-keyspace customer switchtraffic --tablet-types primary

# Down shard 0
vtctldclient Reshard --workflow cust2cust --target-keyspace customer complete
./306_down_shard_0.sh
vtctldclient DeleteShards --force --recursive customer/0

# Down cluster
./401_teardown.sh
```
