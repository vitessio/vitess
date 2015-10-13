This document describe the testing strategy we use for all Vitess components, and the progression in scope / complexity.

As Vitess developers, our goal is to have great unit test coverage, and complete that with whatever integration or end-to-end test makes sense. In the past, we have been relying too much on end-to-end tests, so we are in the process on scaling down our tests to lower levels when appropriate, and increaing our coverage.

## Unit Tests

We aim for all code to have great unit test coverage, no exception. This is mostly the 'go test' unit tests for go, and the other language unit tests.

However, since we use other components extensively, and multiple languages, it is not sufficient. Sometimes, we just gotta have a real MySQL process, if we're testing interactions with MySQL.

## Client Library Tests and vtgateclienttest

All client libraries should be tested with vtgateclienttest. It is meant to validate all client inputs, and make sure all corner cases are handled properly in the client. For instance, to make sure all errors the server can return are translated into the right form on the client.

It is a single stand-alone process, light-weight enough that it runs very quickly and doesn't take many resources at all, and just requires a port to listen on.

(note the go version is run in-process, doesn't even fork vtgateclienttest).

## vttest Library, run\_local\_database.py

This set of helpers has two purposes:

* bring up a very light weight cluster to help test client applications. YouTube uses this internally with thousands of tests.

* help with Vitess unit tests when a real instance is needed (for instance, to test the interactions with MySQL are what we expect).

It is expected that Vitess users may change py/vttest/environment.py to match their setup. For instance, YouTube internally does this to use different scripts to bring up a MySQL instance.

These tests need to be as light weight as possible, so the developers who use Vitess can run as many unit tests as they want with minimal resources. We are running a single MySQL and a single vtcombo instance (that encapsulates one vtgate and multiple vttablets). The MySQL process has multiple databases, one per keyspace / shard, and replication is not enabled. It is the smallest setup with a real MySQL.

This framework supports an initial schema to apply to all keyspaces / shards, per keyspace. This is also the base for testing schema changes before rolling them out to production.

Due to its constant nature, this is not an appropriate framework to test cluster events, like re-sharding, re-parenting, advanced query routing, ... No replication is setup on the single MySQL instance, obviously.

## End to End Tests

These tests run more complicated setups, and take a lot more resources. They are meant to test end-to-end behaviors of the Vitess ecosystem, and complement the unit tests.

For instance, we test each RPC interaction independently (client to vtgate, vtgate to vttablet, vttablet to MySQL, see previous sections). But is is also good to have an end-to-end test that validates everything works together.

These tests almost always launch a topology service, a few mysqld instances, a few vttablets, a vtctld process, a few vtgates, ... They use the real production processes, and real replication. This setup is mandatory for properly testing re-sharding, cluster operations, ... They all however run on the same machine, so they might be limited by the environment.

These tests are located in the toplevel test/ directory in our tree.

## Sandbox Tests

The vitest sandbox is a multi-server framework that runs production processes exactly as they would be running in production, in a cloud environment. It is very mature and powerful. It is also used for benchmarking, not just workflow tests.

We have made a lot of progress for the Google internal sandbox, however the external one (on Kubernetes) is not there yet.

## Consolidation Work

The following action items exist to make it all consistent:

* Switch java tests to use vttest if they need a full cluster, and not java\_vtgate\_test\_helper.py, retire java\_vtgate\_test\_helper.py. Client unit tests should already use vtgateclienttest.

* the python client tests (using vtgateclienttest) are in test/python\_client\_test.py. They need to be finished. Note these are hard to differentiate from the tests in vtgatev2\_test.py. The rule of thumb is if you are testing a client-library feature, tests should be in test/python\_client\_test.py. If you are testing an end-to-end behavior, tests should be in vtgatev2\_test.py.

* Have an external equivalent of the sandbox that can run on a regular basis in Kubernetes / GCE.
