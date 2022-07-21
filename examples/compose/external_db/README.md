**This README is kept here for reference, however the parent [README](../README) contains all the information necesaary to simulate this in a better way**

# Simulate external/remote database for Vitess using docker-compose 

This directory has a docker-compose that will bring up a mysql instance.
You can then point your vitess cluster to it to understand how to use Vitess for your existing database 
when you cannot install Vitess on the mysql instance.

First you will need to [install docker-compose](https://docs.docker.com/compose/install/).


### Create new docker-machine
Create a new docker-machine that will run your mysql container.  
Creating a new machine allows you to more comprehensively test the remote functionality.
```
vitess/examples/compose/external_db$ docker-machine create remote-db
```

Grab the docker-machine ip
```
vitess/examples/compose/external_db$ docker-machine ip remote-db
192.168.99.101
```

Set the environment variables for the remote-db machine
```
vitess/examples/compose/external_db$ eval $(docker-machine ip remote-db)
```

### Start mysql
Start the mysql instance
```
vitess/examples/compose/external_db$ docker-compose up -d
```
This will do the following;
1. Starts mysql service and exposes it at `<docker-machine-ip>:3306`  
2. Creates a `commerce` database with `users` table  
3. Adds sample data to the users table
4. Starts a lightweight adminer container to interact with the database accessible at `<docker-machine-ip>:8081`  
5. Default credentials  
   ```
   MYSQL_DB: commerce
   MYSQL_USER: dbuser  
   MYSQL_PASSWORD: dbpass  
   MYSQL_ROOT_PASSWORD:  pass
   ```

### Confirm containers are up
Run the following
```
vitess/examples/compose/external_db$ docker-compose ps
```

A valid response should look like below
```sh
        Name                       Command                  State                     Ports
---------------------------------------------------------------------------------------------------------
external_db_adminer_1   entrypoint.sh docker-php-e ...   Up             0.0.0.0:8081->8080/tcp
external_db_db_1        docker-entrypoint.sh mysqld      Up (healthy)   0.0.0.0:3306->3306/tcp, 33060/tcp
```
You now have a mysql instance ready to be *migrated* to Vitess.

### Start Vitess pointed to this remote database
Head on to [vitess compose instructions](../README.md )  

If using docker-compose.beginners.yml, run;  
```
vitess/examples/compose$ cp docker-compose.beginners.yml docker-compose.yml
```
Update your `.env` file with these;  
```
KEYSPACE=commerce
DB=commerce
EXTERNAL_DB=1
DB_HOST=<docker-machine-ip>
DB_PORT=3306
DB_USER=dbuser
DB_PASS=dbpass
DB_CHARSET=CHARACTER SET latin1 COLLATE latin1_swedish_ci
```


If using `vtcompose` command, run;  
```
vitess/examples/compose$ go run vtcompose/vtcompose.go -keyspaceData="commerce:0:2::" -externalDbData="commerce:<docker-machine-ip>:3306:dbuser:dbpass:CHARACTER SET latin1 COLLATE latin1_swedish_ci"
```

**Ensure you start Vitess in a different docker-machine!!**
If not, run;
```
vitess/examples/compose$ docker-machine create vitess
vitess/examples/compose$ $(docker-machine env vitess)
```

Start Vitess
```
vitess/examples/compose$ docker-compose up -d
```

You should now have Vitess running against your external database instance.
 
* [Follow this guide for advanced usage](../README.md#advanced-usage "Advanced Usage" )  
* [See this for common issues](../README.md#common-errors "Common Issues" )  

### Migrating to Vitess
Migrating to Vitess entirely can be done from;  
a) The Vitess Control Panel at http://<vitess_ip>:15000  
b) The `lvtcl.sh` Helper Script;  

The steps are same
1. Do an EmergencyReparentShard to make a replica the new primary.
2. Ran InitShardPrimary on the new primary.
3. If Vitess is wrong about who the MySQL primary is, you can update it with TabletExternallyReparented
