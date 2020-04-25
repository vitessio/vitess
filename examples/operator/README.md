kubectl apply -f operator.yaml
kubectl apply -f 101_initial_cluster.yaml

vtctlclient ApplySchema -sql="$(cat create_commerce_schema.sql)" commerce
vtctlclient ApplyVSchema -vschema="$(cat vschema_commerce_initial.json)" commerce

# Insert and verify data
mysql < ../common/insert_commerce_data.sql
mysql --table < ../common/select_commerce_data.sql

# Bring up customer keyspace
kubectl apply -f 201_customer_tablets.yaml

# Initiate move tables
vtctlclient MoveTables -workflow=commerce2customer commerce customer '{"customer":{}, "corder":{}}'

# Cut-over
vtctlclient SwitchReads -tablet_type=rdonly customer.commerce2customer
vtctlclient SwitchReads -tablet_type=replica customer.commerce2customer
vtctlclient SwitchWrites customer.commerce2customer
vtctlclient DropSources customer.commerce2customer

# Prepare for resharding
vtctlclient ApplySchema -sql="$(cat create_commerce_seq.sql)" commerce
vtctlclient ApplyVSchema -vschema="$(cat vschema_commerce_seq.json)" commerce
vtctlclient ApplySchema -sql="$(cat create_customer_sharded.sql)" customer
vtctlclient ApplyVSchema -vschema="$(cat vschema_customer_sharded.json)" customer

kubectl apply -f 302_new_shards.yaml

# Reshard
vtctlclient Reshard customer.cust2cust '0' '-80,80-'
vtctlclient SwitchReads -tablet_type=rdonly customer.cust2cust
vtctlclient SwitchReads -tablet_type=replica customer.cust2cust
vtctlclient SwitchWrites customer.cust2cust

# Down shard 0
TODO
vtctlclient DeleteShard -recursive customer/0

# Down cluster
TODO

