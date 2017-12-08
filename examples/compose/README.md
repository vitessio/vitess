# Local Vitess cluster using docker-compose 

This directory have a docker-compose sample application.
To understand it better, you can run it.

First you will need to [install docker-compose](https://docs.docker.com/compose/install/).

### Start the cluster
To start Consul(which saves the topology config), vtctld, vtgate and a few vttablets with MySQL running on them.
```
vitess/examples/compose$ docker-compose up -d
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
$ mysql --port=15306 --host=127.0.0.1
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
