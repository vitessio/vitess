# Instructions

Detailed instructions for running this example can be found at https://vitess.io.
This document contains the summary of the commands to be run.


```
# Start minikube
minikube start --cpus=4 --memory=8000

# Bring up initial cluster and commerce keyspace
helm install vitess ../../helm/vitess -f 101_initial_cluster.yaml

# Setup aliases
source alias.source

# Insert and verify data
mysql < ../common/insert_commerce_data.sql
mysql --table < ../common/select_commerce_data.sql

# Bring up customer keyspace
helm upgrade vitess ../../helm/vitess/ -f 201_customer_tablets.yaml

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
helm upgrade vitess ../../helm/vitess/ -f 301_customer_sharded.yaml
helm upgrade vitess ../../helm/vitess/ -f 302_new_shards.yaml

# Reshard
vtctlclient Reshard customer.cust2cust '0' '-80,80-'
vtctlclient SwitchReads -tablet_type=rdonly customer.cust2cust
vtctlclient SwitchReads -tablet_type=replica customer.cust2cust
vtctlclient SwitchWrites customer.cust2cust

# Down shard 0
helm upgrade vitess ../../helm/vitess/ -f 306_down_shard_0.yaml
vtctlclient DeleteShard -recursive customer/0

# Delete deployment
helm delete vitess
kubectl delete pvc -l "app=vitess"
kubectl delete vitesstoponodes --all
```
