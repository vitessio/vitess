# Local Vitess cluster using docker-compose 

This directory has a docker-compose sample application.
To understand it better, you can run it.

First you will need to [install docker-compose](https://docs.docker.com/compose/install/).

### Before you begin
You will need to create a docker-compose.yml file. There are 2 ways to do this.
1. Run `go run vtcompose/vtcompose.go --args`. [Instructions can be found here](#programatically-create-vitess-configuration-for-docker "Generate docker-compose")
2. Rename the `docker-compose.beginners.yml` file. Do this with the following command:  
```
cp docker-compose.beginners.yml docker-compose.yml
```

You can then proceed to the instructions under `Start the Cluster` heading.

### Programatically create Vitess configuration for Docker
To create a configuration to your specifications, run vtcompose. Creates corresponding docker-compose file, vschema files per keyspace, and loads schemas.
```
vitess/examples/compose$ go run vtcompose/vtcompose.go  --args(optional)
```

Use `-h` or `--help` to get list of flags with descriptions.

Flags available:
* **baseDockerComposeFile** - Specifies starting docker-compose yaml file.
* **baseVschemaFile** - Specifies starting vschema json file.
* **topologyFlags** - Specifies Vitess topology flags config
* **webPort** - Specifies web port to be used.
* **gRpcPort** - Specifies gRPC port to be used.
* **mySqlPort** - Specifies mySql port to be used.
* **cell** - `Specifies Vitess cell name to be used.
* **keyspaces** - List of `keyspace_name:num_of_shards:num_of_replica_tablets:schema_file_names:<optional>lookup_keyspace_name` separated by ';'. 
    * This is where you specify most of the data for the program to build your vSchema and docker-compose files.
    * Use `0` for `num_of_shards` to specify an unsharded keyspace
    ```
    go run vtcompose/vtcompose.go -keyspaces="test_keyspace:2:1:create_messages.sql,create_tokens.sql:lookup_keyspace;lookup_keyspace:1:1:create_tokens_token_lookup.sql,create_messages_message_lookup.sql"
    ```
* **externalDbData** - Specifies which databases/keyspaces are external and provides data along with it to connect to the external db.
    List of `<external_db_name>,<DB_HOST>,<DB_PORT>,<DB_USER>,<DB_PASS>,<DB_CHARSET>` seperated by ';'.  
    When using this, make sure to have the external_db_name/keyspace in the `keyspaceData` flag with no schema_file_names specified.  
    ```
    go run vtcompose/vtcompose.go -keyspaces="test:2:1::" -externalDbData="test:192.68.99.101:3306:admin:pass:CHARACTER SET utf8 COLLATE utf8_general_ci"
    ```


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
***Note: Should not be needed if VtCompose was used.***

We need to create a few tables into our new cluster. To do that, we can run the `ApplySchema` command.	
```	
vitess/examples/compose$ ./lvtctl.sh ApplySchema -sql "$(cat create_test_table.sql)" test_keyspace	
```

### Create Vschema	
***Note: Should not be needed if VtCompose was used.***  
Create Vschema
```	
vitess/examples/compose$ ./lvtctl.sh ApplyVschema -vschema '{"tables": {"messages": {} } }' test_keyspace	
```

### Run the client to insert and read some data
This will build and run the `client.go` file. It will insert and read data from the master and from the replica.
[See Possible Errors.](#common-errors "Go to common errors")
```
vitess/examples/compose$ ./client.sh
```

### Connect to vgate and run queries
vtgate responds to the MySQL protocol, so we can connect to it using the default MySQL client command line.
```
vitess/examples/compose$ mysql --port=15306 --host=127.0.0.1
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
The compose example has the capability to run against an external mysql instance. Kindly take care to secure your connection to the database.
To start vitess against unsharded external mysql, change the following variables in your .env file to match your external database;
```
KEYSPACE=external_db_name
DB=external_db_name
EXTERNAL_DB=1
DB_HOST=external_db_host
DB_PORT=external_db_port
DB_USER=external_db_user
DB_PASS=external_db_password
DB_CHARSET=CHARACTER SET utf8 COLLATE utf8_general_ci
```

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
```sh
vitess/examples/compose$ mysql --port=15306 --host=<host of machine containers are running in e.g. 127.0.0.1, docker-machine ip e.t.c>

mysql> show databases;
+--------------------+
| Databases          |
+--------------------+
| external_db_name   |
+--------------------+
1 row in set (0.00 sec)
mysql> use external_db_name@master or use external_db_name@replica or use external_db_name@rdonly;
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

## Helper Scripts
The following helper scripts are included to help you perform various actions easily
* vitess/examples/compose/lvtctl.sh
* vitess/examples/compose/lfixrepl.sh
* vitess/examples/compose/lmysql.sh

You may run them as below
```
vitess/examples/compose$ ./lvtctl.sh <args>
```

To run against a specific compose service/container, use the environment variable **$CS**

```
vitess/examples/compose$ (export CS=vttablet2; ./lvtctl.sh <args> )
```

## Common Errors

1. Running ./client.sh may generate the following error
```sh
vitess/examples/compose$ ./client.sh
Inserting into master...
exec failed: Code: FAILED_PRECONDITION
vtgate: ...vttablet: The MySQL server is running with the --read-only option so it cannot execute this statement (errno 1290) ...
exit status 1
```
To resolve use the [SetReadWrite](../../doc/Troubleshooting.md#master-starts-up-read-only) command on master.
```sh
vitess/examples/compose$ ./lvtctl.sh SetReadWrite test-1

```

2. Running ./lvtctl.sh ApplyVschema -vschema '{"sharded":false }' may result in an error referenced by this [issue](https://github.com/vitessio/vitess/issues/4013 )


A quick fix for unsharded db is;
```
vitess/examples/compose$ ./lvtctl.sh ApplyVschema -vschema '{"sharded":false, "tables": {"*": {} } }' external_db_name
```
