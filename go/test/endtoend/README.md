This document describe the testing strategy we use for all Vitess components, and the progression in scope / complexity.

As Vitess developers, our goal is to have great end to end test coverage. In the past, these tests were mostly written in python 2.7 is coming to end of life we are moving all of those into GO. 


## End to End Tests

These tests are meant to test end-to-end behaviors of the Vitess ecosystem, and complement the unit tests. For instance, we test each RPC interaction independently (client to vtgate, vtgate to vttablet, vttablet to MySQL, see previous sections). But is also good to have an end-to-end test that validates everything works together.

These tests almost always launch a topology service, a few mysqld instances, a few vttablets, a vtctld process, a few vtgates, ... They use the real production binaries, packaged as Docker containers, and real replication. This setup is mandatory for properly testing re-sharding, cluster operations, ... Each component runs in its own container, so a test is isolated from whatever else is on the host.


## Strategy 

All the end to end test are placed under path go/test/endtoend. 
The main purpose of grouping them together is to make sure we have single place for reference and to combine similar test to run them in the same cluster and save test running time.  


### Setup
All the tests should be launching a real cluster just like the production setup and execute the tests on that setup followed by a teardown of all the services.

The cluster launch functions are provided under go/vitesst. A test builds a cluster with `vitesst.NewCluster`, passing keyspace, shard, and component options, then calls `Start`, which brings up the containers and returns a cleanup function.

In general the cluster is built in the following order
- Define the keyspace and its shards, with the schema
- Start the topology service [default etcd]
- Start the required mysqld instances
- Start the corresponding vttablets (at least one primary per shard)
- Start vtctld and vtgate

A good example to refer to is any `main_test.go` under go/test/endtoend; go/vitesst holds the framework itself.

### Pre-Requisite
Make sure Docker is running. The framework builds the component images on demand from the current tree, so no local vitess binaries are required.
