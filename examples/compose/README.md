# Local Vitess cluster using docker-compose 

This directory has a docker-compose sample application.
To understand it better, you can run it.

First you will need to [install docker-compose](https://docs.docker.com/compose/install/).

### Start the cluster
To start Consul(which saves the topology config), vtctld, vtgate and a few vttablets with MySQL running on them.
```
vitess/examples/compose$ docker-compose up -d
```

### Check the status of the containers
You can check the logs of the containers (vtgate, vttablet1, vttablet2, vttablet3) at any time.
For example to check vtgate logs, run the following;
```
vitess/examples/compose$ docker-compose logs -f vtgate
```

### Load the schema
We need to create a few tables into our new cluster. To do that, we can run the `ApplySchema` command.
```
vitess/examples/compose$ ./lvtctl.sh ApplySchema -sql "$(cat create_test_table.sql)" test_keyspace
```

### Run the client to insert and read some data
This will build and run the `client.go` file. It will insert and read data from the master and from the replica.
```
vitess/examples/compose$ ./client.sh
```

### Connect to vgate and run queries
vtgate responds to the MySQL protocol, so we can connect to it using the default MySQL client command line.
```
vitess/examples/compose$ ./lmysql.sh --port=15306 --host=127.0.0.1
```


### Play around with vtctl commands

```
vitess/examples/compose$ ./lvtctl.sh Help
```

## Exploring

- vtctld web ui:
  http://localhost:15000

- vttablets web ui:
  http://localhost:15001/debug/status
  http://localhost:15002/debug/status
  http://localhost:15003/debug/status

- vtgate web ui:
  http://localhost:15099/debug/status
  

## Troubleshooting
If the cluster gets in a bad state, you most likely will have to stop and kill the containers. Note: you will lose all the data.
```
vitess/examples/compose$ docker-compose kill
vitess/examples/compose$ docker-compose rm
```

## Advanced Usage

### External mysql instance
The compose example has the capability to run against an external mysql instance.
To start vitess against unsharded external mysql, change the following variables in your .env file to match your external database;
KEYSPACE=external_db_name
DB=external_db_name
EXTERNAL_DB=1
DB_HOST=external_db_host
DB_PORT=external_db_port
DB_USER=external_db_user
DB_PASS=external_db_password
DB_CHARSET=CHARACTER SET utf8 COLLATE utf8_general_ci

Ensure you have log bin enabled on your external database.
You may add the following configs to your conf.d directory and reload mysqld on your server
```
vitess/config/mycnf/master_mysql56.cnf
vitess/config/mycnf/rbr.cnf
```

### Start the cluster
```
vitess/examples/compose$ docker-compose up -d
```

### Check replication status
Once vitess starts, check if replication is working
```
vitess/examples/compose$ ./lfixrepl.sh status
```

### Fix replication
To fix replication, place a mysqldump of the database in vitess/examples/compose/script with filename 'database.sql'
You may then run the following command
```
vitess/examples/compose$ ./lfixrepl.sh
```

### Apply Vschema
Apply Vschema for the unsharded keyspace
```
vitess/examples/compose$ ./lvtctl.sh ApplyVschema -vschema '{"sharded":false, "tables": {"*": {} } }' external_db_name
```

### Connect to vgate and run queries
vtgate responds to the MySQL protocol, so we can connect to it using the default MySQL client command line.
```
vitess/examples/compose$ ./lmysql.sh --port=15306 --host=<host of machine containers are running in e.g. 127.0.0.1, docker-machine ip e.t.c>

mysql> show databases;
+--------------------+
| Databases          |
+--------------------+
| external_db_name   |
+--------------------+
1 row in set (0.00 sec)
mysql> use external_db_name;
Reading table information for completion of table and column names
You can turn off this feature to get a quicker startup with -A

Database changed
mysql> use external_db_name@replica;
Reading table information for completion of table and column names
You can turn off this feature to get a quicker startup with -A

Database changed
mysql> show tables;
+--------------------------------------------+
| Tables_in_external_db_name                 |
+--------------------------------------------+
| DATABASECHANGELOG                          |
| ......................                     |
| table_name_1                               |
| table_name_2                               |
| ......................                     |
|                                            |
+--------------------------------------------+
29 rows in set (0.00 sec)
```
