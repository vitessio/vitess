This document describe the testing strategy we use for all Vitess components, and the progression in scope / complexity.

As Vitess developers, our goal is to have great end to end test coverage. In the past, these tests were mostly written in python 2.7 is coming to end of life we are moving all of those into GO. 


## End to End Tests

These tests are meant to test end-to-end behaviors of the Vitess ecosystem, and complement the unit tests. For instance, we test each RPC interaction independently (client to vtgate, vtgate to vttablet, vttablet to MySQL, see previous sections). But is also good to have an end-to-end test that validates everything works together.

These tests almost always launch a topology service, a few mysqld instances, a few vttablets, a vtctld process, a few vtgates, ... They use the real production processes, and real replication. This setup is mandatory for properly testing re-sharding, cluster operations, ... They all however run on the same machine, so they might be limited by the environment.


## Strategy 

All the end to end test are placed under path go/test/endtoend. 
The main purpose of grouping them together is to make sure we have single place for reference and to combine similar test to run them in the same cluster and save test running time.  

### Setup
All the tests should be launching a real cluster just like the production setup and execute the tests on that setup followed by a teardown of all the services.

The cluster launch functions are provided under go/test/endtoend/cluster. This is still work in progress so feel free to add new function as required or update the existing ones.   

In general the cluster is build in following order
- Define Keyspace
- Define Shards
- Start topology service [default etcd]
- Start vtctld client
- Start required mysqld instances
- Start corresponding vttablets (atleast 1 master and 1 replica)
- Start Vtgate 

A good example to refer will be  go/test/endtoend/clustertest

## Progress
So far we have converted the following Python end to end test cases
- Keyspace tests
- mysqlctl tests
- sharded tests
- tabletmanager tests
- vtgate v3 tests
 
### In-progress 
- Inital sharding
- resharding
- vsplit 


After a Python test is migrated in Go it will be removed from end to end ci test run by updating the shard value to 5 in `test/config.json`

    
