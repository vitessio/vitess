# Instructions

Detailed instructions for running this example can be found at https://vitess.io.
This document contains the summary of the commands to be run.


```
# Start minikube
minikube start --kubernetes-version v1.15.0 --cpus=4 --memory=5000

# Bring up initial cluster and commerce keyspace
helm install vitess ../../helm/vitess -f 101_initial_cluster.yaml

# Setup aliases
source alias.source

# Insert and verify data
vmysql < ../common/insert_commerce_data.sql
vmysql --table < ../common/select_commerce_data.sql

# Bring up customer keyspace
helm upgrade vitess ../../helm/vitess/ -f 201_customer_tablets.yaml

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
helm upgrade vitess ../../helm/vitess/ -f 301_customer_sharded.yaml
helm upgrade vitess ../../helm/vitess/ -f 302_new_shards.yaml

# Reshard
vclient Reshard customer.cust2cust '0' '-80,80-'
vclient SwitchReads -tablet_type=rdonly customer.cust2cust
vclient SwitchReads -tablet_type=replica customer.cust2cust
vclient SwitchWrites customer.cust2cust

# Down shard 0
helm upgrade vitess ../../helm/vitess/ -f 306_down_shard_0.yaml
vclient DeleteShard -recursive customer/0
```
