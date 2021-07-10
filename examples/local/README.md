# Instructions

Detailed instructions for running this example can be found at https://vitess.io.
This document contains the summary of the commands to be run.


```
# Setup environment and aliases
source env.sh

# Bring up initial cluster and commerce keyspace
./101_initial_cluster.sh

# Insert and verify data
mysql < ../common/insert_commerce_data.sql
mysql --table < ../common/select_commerce_data.sql

# Bring up customer keyspace
./201_customer_tablets.sh

# Initiate move tables
vtctlclient MoveTables -workflow=commerce2customer commerce customer '{"customer":{}, "corder":{}}'

# Validate
vtctlclient VDiff customer.commerce2customer

# Cut-over
vtctlclient SwitchReads -tablet_type=rdonly customer.commerce2customer
vtctlclient SwitchReads -tablet_type=replica customer.commerce2customer
vtctlclient SwitchWrites customer.commerce2customer

# Clean-up
vtctlclient DropSources customer.commerce2customer

# Prepare for resharding
./301_customer_sharded.sh
./302_new_shards.sh

# Reshard
vtctlclient Reshard customer.cust2cust '0' '-80,80-'

# Validate
vtctlclient VDiff customer.cust2cust

# Cut-over
vtctlclient SwitchReads -tablet_type=rdonly customer.cust2cust
vtctlclient SwitchReads -tablet_type=replica customer.cust2cust
vtctlclient SwitchWrites customer.cust2cust

# Down shard 0
./306_down_shard_0.sh
vtctlclient DeleteShard -recursive customer/0

# Down cluster
./401_teardown.sh
```
