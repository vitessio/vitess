## Wrangler test framework

Some details on how the tests are setup for unit tests in this directory. Updating tests is currently tedious if sql 
queries get added/modified. We hope to simplify the framework "soon".

We will illustrate with by walking through some tests 

### TestCanSwitch

The first thing in all tests is to create mock tablets for source and targets 
and the associated database mocks. `traffic_switcher_env_test.go` and `fake_tablet_test.go` contains the 
framework code.  `newTestTableMigrater` is called to setup the workflow. `newTestTableMigrater` is just a
wrapper on `newTestTableMigraterCustom` to create a workflow with two source
shards "-40", "40-" and two target shards "-80", "80-". The filter it uses is a `select *` 
signifying MoveTables (instead of a Materialize). 

#### `newTestTableMigraterCustom` 
* setups a topo (memorytopo) 
* instantiates a wrangler with this topo, logging to the console
* instantiates a mock db from `fakesqldb`
* creates a fake tablet, one (primary) per shard
* start tablets (see below)
* creates db clients (the `fakeDBClient` for VR engine queries) and the VR engines using this mock db
* set starting gtid positions 
* Mimic a MoveTables: on the targets create vreplication stream entries to mimic MoveTables and also create the reverse
workflow on the sources. Expected sql queries for the vreplication table are
also added here. Routing rules are setup for this MoveTables.

#### The fake tablet
`go/vt/wrangler/fake_tablet_test.go`

Fake tablets are active in that they run a go routine listening to the tablet grpc 
port and are registered in the topo. The database however is a mock db and not 
an actual mysql server. Starting an actual mysql server is very slow and hence 
we prefer the mock db.

The fake tablet has as attributes the keyspace, shard and tablet uid. The key
functionality is the action loop which creates listeners on the http and grpc ports
and instantiates a `TabletManager` using  the test top server and
fake mysql daemon and registers it with the grpc server.

This ensures that the grpc calls made during the test are actually handled by
tablet manager code and any vrengine database calls are intercepted by the mock. 
This setup is good enough for the tests that just setup the workflow and test
the workflow state machine and are not dependent on the data being vreplicated.


#### The fake MySQLDaemon
`go/vt/mysqlctl/fakemysqldaemon/fakemysqldaemon.go`

Used to set primary positions to provide/validate gtids


#### The fake db client
`go/vt/wrangler/fake_dbclient_test.go`

This defines a mock db *only* for the vreplication engine. Hence all queries that
it mocks related to the `_vt` database only.


 

### Pain points / Possible improvements
* Multiple mock db frameworks used (for wrangler, materialize)
* sequence of queries matters. This was introduced for precise testing of logic
when VReplication was first invented and many moving parts had to be validated. We 
have probably moved past that point and should move to a simpler mock.

Ideally we should just use a mysql-backed test server so that we can test based
on actual data and also won't have to define the queries each time they change. This
is done in e2e tests, but it is a bit expensive. Maybe it is worth the price in developer-hours if we create
a single mysql server for all tests in the wrangler directory.

Note
`fake_dbclient_test.go` is used in materializer/movetables tests and is a separate 
mock db
