## Wrangler test framework

This document has some initial details on how the wrangler unit tests are setup. Updating tests is currently tedious,
especially if sql queries get added/modified. We hope to simplify the framework "soon".

We will illustrate by walking through a test.

### TestCanSwitch

We first create mock tablets for source and targets and the associated database mocks. `traffic_switcher_env_test.go`
and `fake_tablet_test.go` contains the framework code.  `newTestTableMigrater` is called to setup the
workflow. `newTestTableMigrater` is just a wrapper on `newTestTableMigraterCustom` to create a workflow with two source
shards "-40", "40-" and two target shards "-80", "80-". The filter it uses is a `select *` signifying MoveTables (rather
than a Materialize).

#### `newTestTableMigraterCustom`

* setups a topo (memorytopo)
* instantiates a wrangler with this topo, logging to the console
* instantiates a mock db from `fakesqldb`
* creates a fake tablet, one (primary) per shard
* start tablets (see below)
* creates db clients (the `fakeDBClient` for VR engine queries) and the VR engines using this mock db
* sets starting gtid positions
* setups a MoveTables: on the targets create vreplication stream entries for MoveTables and also create the reverse
  workflow on the sources. Expected sql queries for the vreplication table are also added here. Routing rules are setup.

#### The fake tablet

`go/vt/wrangler/fake_tablet_test.go`

Fake tablets are "active": they run a goroutine listening to the tablet grpc port and are registered in the topo. The
database however is a mock db and not an actual mysql server. Starting an actual mysql server is very slow and hence we
prefer the mock db.

The fake tablet has, as attributes, keyspace, shard and tablet uid. The key functionality is the action loop which
creates listeners on the http and grpc ports and instantiates a `TabletManager` using the test topo server and fake
mysql daemon and registers it with the grpc server.

This ensures that the grpc calls made during the test are actually handled by tablet manager code and any vrengine
database calls are intercepted by the mock. This setup is good enough for the tests that just setup the workflow and
test the workflow state machine. There is no actual data being vreplicated.

#### The fake MySQLDaemon

`go/vt/mysqlctl/fakemysqldaemon.go`

Used to set primary positions to provide/validate gtids.

#### The fake db client

`go/vt/wrangler/fake_dbclient_test.go`

This defines a mock db which is limited in scope to the vreplication engine. All queries that it mocks are related to
the `_vt` database only. The queries specified serve to validate that the expected set of queries were generated. When
updating/adding tests, you will need to tell each test what queries are valid. For queries that can happen often and
asynchronously, like updating heartbeats or setting gtid positions, we have the mechanism to ignore them or return a
fixed result.

### Pain points / Possible improvements

* Multiple mock db frameworks are used (different ones for wrangler, materialize) resulting in too many paradigms and
  duplication of functionality
* sequence of queries matters. This was introduced for precise testing of logic when VReplication was first invented and
  many moving parts had to be validated. Do we still need this or can we switch to a query map based mock?

Another option is to use a db server with a mysql API, ideally an in-memory one so that we can test based on actual
data. We won't have to define the queries each time they change. We can of course just setup a regular mysql server.
This is done in e2e tests, but it might be unacceptable for unit tests.  

