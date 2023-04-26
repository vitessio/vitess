# Instructions

```
# Start minikube
minikube start --cpus=8 --memory=11000 --disk-size=50g --kubernetes-version=v1.19.5

# Install Operator
kubectl apply -f operator.yaml

# Bring up initial cluster and commerce keyspace
# NOTE: If you are using MySQL 8, update the images section to use mysql80 images
# Example:
#  images:
#    vtctld: vitess/lite:mysql80
#    vtadmin: vitess/vtadmin:latest
#    vtgate: vitess/lite:mysql80
#    vttablet: vitess/lite:mysql80
#    vtbackup: vitess/lite:mysql80
#    vtorc: vitess/lite:mysql80
#    mysqld:
#      mysql80Compatible: vitess/lite:mysql80

kubectl apply -f 101_initial_cluster.yaml

# Port-forward vtctld, vtgate and vtadmin and apply schema and vschema
# VTAdmin's UI will be available at http://localhost:14000/
./pf.sh &
alias mysql="mysql -h 127.0.0.1 -P 15306 -u user"
alias vtctlclient="vtctlclient --server localhost:15999 --alsologtostderr"
vtctlclient ApplySchema -- --sql="$(cat create_commerce_schema.sql)" commerce
vtctlclient ApplyVSchema -- --vschema="$(cat vschema_commerce_initial.json)" commerce

# Insert and verify data
mysql < ../common/insert_commerce_data.sql
mysql --table < ../common/select_commerce_data.sql

# Bring up customer keyspace
kubectl apply -f 201_customer_tablets.yaml

# Initiate move tables
vtctlclient MoveTables -- --source commerce --tables 'customer,corder' Create customer.commerce2customer

# Validate
vtctlclient VDiff customer.commerce2customer

# Cut-over
vtctlclient MoveTables -- --tablet_types=rdonly,replica SwitchTraffic customer.commerce2customer
vtctlclient MoveTables -- --tablet_types=primary SwitchTraffic customer.commerce2customer

# Clean-up
vtctlclient MoveTables Complete customer.commerce2customer

# Prepare for resharding
vtctlclient ApplySchema -- --sql="$(cat create_commerce_seq.sql)" commerce
vtctlclient ApplyVSchema -- --vschema="$(cat vschema_commerce_seq.json)" commerce
vtctlclient ApplySchema -- --sql="$(cat create_customer_sharded.sql)" customer
vtctlclient ApplyVSchema -- --vschema="$(cat vschema_customer_sharded.json)" customer
kubectl apply -f 302_new_shards.yaml

# Reshard
vtctlclient Reshard -- --source_shards '-' --target_shards '-80,80-' Create customer.cust2cust

# Validate
vtctlclient VDiff customer.cust2cust

# Cut-over
vtctlclient Reshard -- --tablet_types=rdonly,replica SwitchTraffic customer.cust2cust
vtctlclient Reshard -- --tablet_types=primary SwitchTraffic customer.cust2cust

# Down shard 0
vtctlclient Reshard Complete customer.cust2cust
kubectl apply -f 306_down_shard_0.yaml

# Down cluster
kubectl delete -f 101_initial_cluster.yaml
```
