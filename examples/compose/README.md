# Local Vitess cluster using docker-compose 

This directory has a docker-compose sample application.
To understand it better, you can run it.

First you will need to [install docker-compose](https://docs.docker.com/compose/install/).

### Before you begin
You will need to create a docker-compose.yml file. There are 2 ways to do this.
1. Run `go run vtcompose/vtcompose.go --args`. [Instructions can be found here](#programatically-create-vitess-configuration-for-docker "Generate docker-compose")
2. Use the `docker-compose.beginners.yml` to generate your `docker-compose.yml` file. Run:  
```
vitess/examples/compose$ cp docker-compose.beginners.yml docker-compose.yml
```
Create your .env file  
```
vitess/examples/compose$ cp template.env .env
```

You can then proceed to the instructions under [`Start the Cluster`](#start-the-cluster "Start the cluster") section.

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
* **keyspaceData** - List of `keyspace_name:num_of_shards:num_of_replica_tablets:schema_file_names:<optional>lookup_keyspace_name` separated by ';'. 
    * This is where you specify most of the data for the program to build your vSchema and docker-compose files.
    * Examples you can run;
    * Default
    ```
    go run vtcompose/vtcompose.go
    ```
    * Use `0` for `num_of_shards` to specify an unsharded keyspace
    ```
    go run vtcompose/vtcompose.go -keyspaceData="test_keyspace:0:2:create_messages.sql"
    ```
    * Multiple keyspaces with sharded test_keyspace
    ```
    go run vtcompose/vtcompose.go -keyspaceData="test_keyspace:2:1:create_messages.sql,create_tokens.sql:lookup_keyspace;lookup_keyspace:1:1:create_tokens_token_lookup.sql,create_messages_message_lookup.sql"
    ```
* **externalDbData** - Specifies which databases/keyspaces are external and provides data along with it to connect to the external db.
    List of `<external_db_name>,<DB_HOST>,<DB_PORT>,<DB_USER>,<DB_PASS>,<DB_CHARSET>` separated by ';'.  
    When using this, make sure to have the external_db_name/keyspace in the `keyspaceData` flag with no schema_file_names specified.  
    ```
    go run vtcompose/vtcompose.go -keyspaceData="commerce:0:2::" -externalDbData="commerce:external_db_host:3306:external_db_user:external_db_password:"
    ```


### Start the cluster
To start Consul(which saves the topology config), vtctld, vtgate, orchestrator and a few vttablets with MySQL running on them.
```
vitess/examples/compose$ docker-compose up -d
```

### Check cluster status
Check the status of the cluster.
```
vitess/examples/compose$ docker-compose ps
           Name                         Command                  State                                                                     Ports
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
compose_consul1_1            docker-entrypoint.sh agent ...   Up             8300/tcp, 8301/tcp, 8301/udp, 8302/tcp, 8302/udp, 0.0.0.0:8400->8400/tcp, 0.0.0.0:8500->8500/tcp, 0.0.0.0:8600->8600/tcp, 8600/udp
compose_consul2_1            docker-entrypoint.sh agent ...   Up             8300/tcp, 8301/tcp, 8301/udp, 8302/tcp, 8302/udp, 8400/tcp, 8500/tcp, 8600/tcp, 8600/udp
compose_consul3_1            docker-entrypoint.sh agent ...   Up             8300/tcp, 8301/tcp, 8301/udp, 8302/tcp, 8302/udp, 8400/tcp, 8500/tcp, 8600/tcp, 8600/udp
compose_external_db_host_1   docker-entrypoint.sh mysqld      Up (healthy)   0.0.0.0:32835->3306/tcp, 33060/tcp
compose_schemaload_1         sh -c /script/schemaload.sh      Exit 0
compose_vreplication_1       sh -c [ $EXTERNAL_DB -eq 1 ...   Exit 0
compose_vtctld_1             sh -c  /vt/bin/vtctld -top ...   Up             0.0.0.0:32836->15999/tcp, 0.0.0.0:15000->8080/tcp
compose_vtgate_1             sh -c /vt/bin/vtgate -topo ...   Up             0.0.0.0:15306->15306/tcp, 0.0.0.0:32845->15999/tcp, 0.0.0.0:15099->8080/tcp
compose_vtorc_1              sh -c /script/vtorc-up.sh        Up (healthy)   0.0.0.0:13000->3000/tcp
compose_vttablet100_1        sh -c [ $EXTERNAL_DB -eq 1 ...   Exit 0
compose_vttablet101_1        sh -c /script/vttablet-up. ...   Up (healthy)   0.0.0.0:32838->15999/tcp, 0.0.0.0:32840->3306/tcp, 0.0.0.0:15101->8080/tcp
compose_vttablet102_1        sh -c /script/vttablet-up. ...   Up (healthy)   0.0.0.0:32842->15999/tcp, 0.0.0.0:32844->3306/tcp, 0.0.0.0:15102->8080/tcp
compose_vttablet103_1        sh -c /script/vttablet-up. ...   Up (healthy)   0.0.0.0:32841->15999/tcp, 0.0.0.0:32843->3306/tcp, 0.0.0.0:15103->8080/tcp
```

### Check the status of the containers
You can check the logs of the containers (vtgate, vttablet101, vttablet102, vttablet103) at any time.
For example to check vtgate logs, run the following;
```
vitess/examples/compose$ docker-compose logs -f vtgate
```

### Load the schema
This step is carried out by the **schemaload** container

We need to create a few tables into our new cluster. To do that, we can run the `ApplySchema` command.	
```	
vitess/examples/compose$ ./lvtctl.sh ApplySchema -- --sql "$(cat tables/create_messages.sql)" test_keyspace	
```

### Create Vschema	
This step is carried out by the **schemaload** container

Create Vschema
```	
vitess/examples/compose$ ./lvtctl.sh ApplyVschema -- --vschema '{"sharded": false }' test_keyspace	
```

### Connect to vgate and run queries
vtgate responds to the MySQL protocol, so we can connect to it using the default MySQL client command line.
```
vitess/examples/compose$ mysql --port=15306 --host=127.0.0.1
```
**Note that you may need to replace `127.0.0.1` with `docker ip` or `docker-machine ip`** 

You can also use the `./lmysql.sh` helper script.
```
vitess/examples/compose$ ./lmysql.sh --port=15306 --host=<DOCKER_HOST_IP>
```

where `<DOCKER_HOST_IP>` is `docker-machine ip` or external docker host ip addr

### Play around with vtctl commands

```
vitess/examples/compose$ ./lvtctl.sh Help
```

## Exploring

- vtctld web ui:
  http://localhost:15000

- vttablets web ui:
  http://localhost:15101/debug/status
  http://localhost:15102/debug/status
  http://localhost:15103/debug/status

- vtgate web ui:
  http://localhost:15099/debug/status

- orchestrator web ui:
  http://localhost:13000
  
- Stream querylog
  `curl -S localhost:15099/debug/querylog`
  
**Note that you may need to replace `localhost` with `docker ip` or `docker-machine ip`**  
 
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

**Important** Ensure you have log bin enabled on your external database.
Refer to the .cnf files [here](./external_db/mysql)

### Start the cluster
To simulate running vitess against an external cluster, simply change
`EXTERNAL_DB=1` when you copy your `template.env` then run the command below
```
vitess/examples/compose$ docker-compose up -d
```

### Check cluster status
Once vitess starts, check cluster status
```
vitess/examples/compose$ docker-compose ps

           Name                         Command                  State                                                                     Ports
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
compose_consul1_1            docker-entrypoint.sh agent ...   Up             8300/tcp, 8301/tcp, 8301/udp, 8302/tcp, 8302/udp, 0.0.0.0:8400->8400/tcp, 0.0.0.0:8500->8500/tcp, 0.0.0.0:8600->8600/tcp, 8600/udp
compose_consul2_1            docker-entrypoint.sh agent ...   Up             8300/tcp, 8301/tcp, 8301/udp, 8302/tcp, 8302/udp, 8400/tcp, 8500/tcp, 8600/tcp, 8600/udp
compose_consul3_1            docker-entrypoint.sh agent ...   Up             8300/tcp, 8301/tcp, 8301/udp, 8302/tcp, 8302/udp, 8400/tcp, 8500/tcp, 8600/tcp, 8600/udp
compose_external_db_host_1   docker-entrypoint.sh mysqld      Up (healthy)   0.0.0.0:32846->3306/tcp, 33060/tcp
compose_schemaload_1         sh -c /script/schemaload.sh      Exit 0
compose_vreplication_1       sh -c [ $EXTERNAL_DB -eq 1 ...   Exit 0
compose_vtctld_1             sh -c  /vt/bin/vtctld -top ...   Up             0.0.0.0:32847->15999/tcp, 0.0.0.0:15000->8080/tcp
compose_vtgate_1             sh -c /vt/bin/vtgate -topo ...   Up             0.0.0.0:15306->15306/tcp, 0.0.0.0:32856->15999/tcp, 0.0.0.0:15099->8080/tcp
compose_vtorc_1              sh -c /script/vtorc-up.sh        Up (healthy)   0.0.0.0:13000->3000/tcp
compose_vttablet100_1        sh -c [ $EXTERNAL_DB -eq 1 ...   Up (healthy)   0.0.0.0:32848->15999/tcp, 0.0.0.0:32849->3306/tcp, 0.0.0.0:15100->8080/tcp
compose_vttablet101_1        sh -c /script/vttablet-up. ...   Up (healthy)   0.0.0.0:32850->15999/tcp, 0.0.0.0:32851->3306/tcp, 0.0.0.0:15101->8080/tcp
compose_vttablet102_1        sh -c /script/vttablet-up. ...   Up (healthy)   0.0.0.0:32853->15999/tcp, 0.0.0.0:32855->3306/tcp, 0.0.0.0:15102->8080/tcp
compose_vttablet103_1        sh -c /script/vttablet-up. ...   Up (healthy)   0.0.0.0:32852->15999/tcp, 0.0.0.0:32854->3306/tcp, 0.0.0.0:15103->8080/tcp

```

### Check replication
The vreplication container included performs the following actions;
1. Identifies the correct source and destination tablets for you
2. Copies schema from external database to Vitess managed tablet
3. Starts VReplication from external keyspace to managed keyspace
4. Prints out helpful debug information for you.
```
vitess/examples/compose$ docker-compose logs -f vreplication
vreplication_1      | + /vt/bin/vtctlclient --server vtctld:15999 VReplicationExec local-0000000101 'insert into _vt.vreplication (db_name, source, pos, max_tps, max_replication_lag, tablet_types, time_updated, transaction_timestamp, state) values('\''commerce'\'', '\''keyspace:\"ext_commerce\" shard:\"0\" filter:<rules:<match:\"/.*\" > > on_ddl:EXEC_IGNORE '\'', '\'''\'', 9999, 9999, '\''primary'\'', 0, 0, '\''Running'\'')'
vreplication_1      | + /vt/bin/vtctlclient --server vtctld:15999 VReplicationExec local-0000000101 'select * from _vt.vreplication'
vreplication_1      | +----+----------+--------------------------------+-----+----------+---------+---------------------+------+--------------+--------------+-----------------------+---------+---------+----------+
vreplication_1      | | id | workflow |             source             | pos | stop_pos | max_tps | max_replication_lag | cell | tablet_types | time_updated | transaction_timestamp |  state  | message | db_name  |
vreplication_1      | +----+----------+--------------------------------+-----+----------+---------+---------------------+------+--------------+--------------+-----------------------+---------+---------+----------+
vreplication_1      | |  1 |          | keyspace:"ext_commerce"        |     |          |    9999 |                9999 |      | primary       |            0 |                     0 | Running |         | commerce |
vreplication_1      | |    |          | shard:"0"                      |     |          |         |                     |      |              |              |                       |         |         |         |
vreplication_1      | |    |          | filter:<rules:<match:"/.*" > > |     |          |         |                     |      |              |              |                       |         |         |         |
vreplication_1      | |    |          | on_ddl:EXEC_IGNORE             |     |          |         |                     |      |              |              |                       |         |         |         |
vreplication_1      | +----+----------+--------------------------------+-----+----------+---------+---------------------+------+--------------+--------------+-----------------------+---------+---------+----------+
compose_vreplication_1 exited with code 0
```

### Connect to vgate and run queries
vtgate responds to the MySQL protocol, so we can connect to it using the default MySQL client command line.
Verify that data was copied and is replicating successfully.
```sh
vitess/examples/compose$ ./lmysql --port=15306 --host=<host of machine containers are running in e.g. 127.0.0.1, docker-machine ip e.t.c>

mysql> show databases;
mysql> show databases;
+--------------+
| Databases    |
+--------------+
| commerce     |
| ext_commerce |
+--------------+
2 rows in set (0.01 sec)
mysql> use commerce@replica;
Reading table information for completion of table and column names
You can turn off this feature to get a quicker startup with -A

Database changed
mysql> show tables;
+--------------------+
| Tables_in_commerce |
+--------------------+
| users              |
+--------------------+
1 row in set (0.00 sec)

mysql> select count(*) from users;
+----------+
| count(*) |
+----------+
|     1000 |
+----------+
1 row in set (0.00 sec)

```

## Helper Scripts
The following helper scripts are included to help you perform various actions easily
* vitess/examples/compose/lvtctl.sh
* vitess/examples/compose/lmysql.sh

You may run them as below
```
vitess/examples/compose$ ./lvtctl.sh <args>
```

To run against a specific compose service/container, use the environment variable **$CS**

```
vitess/examples/compose$ (export CS=vttablet101; ./lvtctl.sh <args> )
```

## Custom Image Tags
You  may specify a custom `vitess:lite` image tag by setting the evnironment variable `VITESS_TAG`.  
This is optional and defaults to the `latest` tag. Example;
* Set `VITESS_TAG=8.0.0` in your `.env` before running `docker-compose up -d`
* Run `VITESS_TAG=8.0.0; docker-compose up -d`


## Reference
Checkout this excellent post about [The Life of a Vitess Cluster](https://vitess.io/blog/2020-04-27-life-of-a-cluster/)


