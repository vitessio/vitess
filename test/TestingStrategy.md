This document describe the testing strategy we use for all Vitess components.

## Unit Tests

All code should have great unit test coverage, no exception.

However, since we use other components extensively, and multiple languages, it is not sufficient. Sometimes, we just gotta have a real MySQL process, if we're testing interactions with MySQL.

## Client Library Tests and vtgateclienttest

All client libraries should be tested with vtgateclienttest. It is meant to validate all client inputs, and make sure all corner cases are handled properly in the client. For instance, to make sure all errors the server can return are translated into the right form on the client.

It is a single stand-alone process, light-weight enough that it runs very quickly and doesn't take many resources at all, and just requires a port to listen on.

# vttest Library, run\_local\_database.py

This set of helpers has two purposes:

* bring up a very light well cluster to help test client applications. YouTube uses this internally with thousands of tests.

* help with Vitess unit tests when a real instance is needed (for instance, to test the interactions with MySQL are what we expect).

It is expected that Vitess users may change py/vttest/environment.py to match their setup. For instance, YouTube internally does is to use different scripts to bring up a MySQL instance.

These tests need to be as light weight as possible, so the developers who use Vitess can run as many unit tests as they want with minimal resources. We are currently running a single MySQL, multiple vtocc, and one vtgate. The plan is to switch to vtcombo to replace vtgate+vtocc processes with a single process. The MySQL process has multiple databases, one per keyspace / shard. It is the smallest setup with a real MySQL.

This framework supports an initial schema to apply to all keyspaces / shards, per keyspace. This is also the base for testing schema changes before rolling them out to production.

Due to its constant nature, this is not an appropriate framework to test cluster events, like re-sharding, re-parenting, advanced query routing, ... No replication is setup on the single MySQL instance, obviously.

# Integration Tests

These tests run more complicated setups, and take a lot more resources. They are meant to test end-to-end behaviours of the Vitess echosystem, and complement the unit tests.

For instance, we test each RPC interaction independently (client to vtgate, vtgate to vttablet, vttablet to MySQL, see previous sections). But is is also good to have an end-to-end test that validates everything works together.

These tests almost always launch a topology service, a few mysqld instances, a few vttablets, a vtctld process, a few vtgates, ... They use the real production processes, and real replication. This setup is mandatory for properly testing re-sharding, cluster operations, ...

# Consolidation Work

The following action items exist to make it all consistent:

* Consolidate to use use vttest everywhere, instead of the old google3-only run\_local\_database.py. Note vttest library is working in Google3 and passes unit test scenarios. Haven't had time to clean up the old stuff yet.

* Switch query\_service.py tests to unit tests using vttest. Sugu is on it, first thing is to let vttest only launch a MySQL (and nothing else), and fix its MySQL parameters.

* Switch java tests to use vttest if they need a full cluster, and not java\_vtgate\_test\_helper.py, retire java\_vtgate\_test\_helper.py. Client unit tests should already use vtgateclienttest.

* Same for google3 C++ client.

* We are removing direct vttablet access to python. This in turn will remove a lot of code and tests, like vtclient.py, tablet.py, zkocc.py, ... Less surface area is good, we just need to make sure we maintain good code coverage. As part of this:

  * test/vtdb\_test.py needs to go away. But we need to make sure what it tests is covered in other places. So if it's just testing the python vttablet library, that should go. If it's also testing vttablet timeout behaviours, that should stay.

  * no integration test can depend on direct vttablet access. vtctl has helper query methods for this if needed.

* the python client tests (using vtgateclienttest) are in test/python\_client\_test.py. They need to be finished. Note these are hard to differentiate from the tests in vtgatev2\_test.py. The rule of thumb is if you are testing a client-library feature, tests should be in test/python\_client\_test.py. If you are testing an end-to-end behaviour, tests should be in vtgatev2\_test.py.

