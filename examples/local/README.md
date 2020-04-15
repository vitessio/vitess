# Instructions

Detailed instructions for running this example can be found at https://vitess.io.
This document contains the summary of the commands to be run.


```
# Bring up initial cluster and commerce keyspace
./101_initial_cluster.sh

# Setup aliases
source alias.source

# Insert and verify data
mysql < ../common/insert_commerce_data.sql
mysql --table < ../common/select_commerce_data.sql

# Bring up customer keyspace
./201_customer_tablets.sh

# Initiate move tables
vclient MoveTables -workflow=commerce2customer commerce customer '{"customer":{}, "corder":{}}'

# Cut-over
vclient SwitchReads -tablet_type=rdonly customer.commerce2customer
vclient SwitchReads -tablet_type=replica customer.commerce2customer
vclient SwitchWrites customer.commerce2customer

# Clean-up
vclient SetShardTabletControl -blacklisted_tables=customer,corder -remove commerce/0 rdonly
vclient SetShardTabletControl -blacklisted_tables=customer,corder -remove commerce/0 replica
vclient SetShardTabletControl -blacklisted_tables=customer,corder -remove commerce/0 master
vclient ApplyRoutingRules -rules='{}'

# Prepare for resharding
./301_customer_sharded.sh
./302_new_shards.sh

# Reshard
vclient Reshard customer.cust2cust '0' '-80,80-'
vclient SwitchReads -tablet_type=rdonly customer.cust2cust
vclient SwitchReads -tablet_type=replica customer.cust2cust
vclient SwitchWrites customer.cust2cust

# Down shard 0
./306_down_shard_0.sh
vclient DeleteShard -recursive customer/0

# Down cluster
./401_teardown.sh
```
