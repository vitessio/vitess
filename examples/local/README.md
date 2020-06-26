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
vtctlclient MoveTables -workflow=commerce2customer commerce customer '{"customer":{}, "corder":{}}'

# Cut-over
vtctlclient SwitchReads -tablet_type=rdonly customer.commerce2customer
vtctlclient SwitchReads -tablet_type=replica customer.commerce2customer
vtctlclient SwitchWrites customer.commerce2customer

# Clean-up
vtctlclient SetShardTabletControl -blacklisted_tables=customer,corder -remove commerce/0 rdonly
vtctlclient SetShardTabletControl -blacklisted_tables=customer,corder -remove commerce/0 replica
vtctlclient SetShardTabletControl -blacklisted_tables=customer,corder -remove commerce/0 master
vtctlclient ApplyRoutingRules -rules='{}'

# Prepare for resharding
./301_customer_sharded.sh
./302_new_shards.sh

# Reshard
vtctlclient Reshard customer.cust2cust '0' '-80,80-'
vtctlclient SwitchReads -tablet_type=rdonly customer.cust2cust
vtctlclient SwitchReads -tablet_type=replica customer.cust2cust
vtctlclient SwitchWrites customer.cust2cust

# Down shard 0
./306_down_shard_0.sh
vtctlclient DeleteShard -recursive customer/0

# Down cluster
./401_teardown.sh
```
