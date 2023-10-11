# Instructions

```
# Start minikube
minikube start --cpus=8 --memory=11000 --disk-size=50g --kubernetes-version=v1.25.8

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
alias vtctldclient="vtctldclient --server localhost:15999 --alsologtostderr"
vtctldclient ApplySchema --sql="$(cat create_commerce_schema.sql)" commerce
vtctldclient ApplyVSchema --vschema="$(cat vschema_commerce_initial.json)" commerce

# Insert and verify data
mysql < ../common/insert_commerce_data.sql
mysql --table < ../common/select_commerce_data.sql

# Bring up customer keyspace
kubectl apply -f 201_customer_tablets.yaml

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
vtctldclient ApplySchema --sql="$(cat create_commerce_seq.sql)" commerce
vtctldclient ApplyVSchema --vschema="$(cat vschema_commerce_seq.json)" commerce
vtctldclient ApplySchema --sql="$(cat create_customer_sharded.sql)" customer
vtctldclient ApplyVSchema --vschema="$(cat vschema_customer_sharded.json)" customer
kubectl apply -f 302_new_shards.yaml

# Reshard
vtctldclient Reshard --workflow cust2cust --target-keyspace customer create --source-shards '-' --target-shards '-80,80-'

# Validate
vtctldclient vdiff --workflow cust2cust --target-keyspace customer create
vtctldclient vdiff --workflow cust2cust --target-keyspace customer show last

# Cut-over
vtctldclient Reshard --workflow cust2cust --target-keyspace customer switchtraffic --tablet-types "rdonly,replica"
vtctldclient Reshard --workflow cust2cust --target-keyspace customer switchtraffic --tablet-types primary

# Down shard 0
vtctldclient Reshard --workflow cust2cust --target-keyspace customer complete
kubectl apply -f 306_down_shard_0.yaml

# Down cluster
kubectl delete -f 101_initial_cluster.yaml
```
