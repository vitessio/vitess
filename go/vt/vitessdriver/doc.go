// Package vitessdriver contains the Vitess Go SQL driver.
/*
Vitess

Vitess is an SQL middleware which turns MySQL/MariaDB into a fast, scalable and
highly-available distributed database.
For more information, visit http://www.vitess.io.


Example

Using this SQL driver is as simple as:

  import (
    "time"
    "github.com/youtube/vitess/go/vt/vitessdriver"
  )

  func main() {
    // Connect to vtgate (v1 mode).
    db, err := vitessdriver.Open("localhost:15991", "keyspace", "0", "master", 30*time.Second)

    // Use "db" via the Golang sql interface.
  }

For a full example, please see: https://github.com/youtube/vitess/blob/master/examples/local/client.go

The full example is based on our tutorial for running Vitess locally: http://vitess.io/getting-started/local-instance.html


Vtgate

This driver will connect to a so called "vtgate" process.

Within Vitess, vtgate is a proxy server which takes care of routing queries to
the respective shards.

It decouples the routing logic from the application and
therefore enables Vitess features like consistent, application-transparent
reshardings:
When you scale up the number of shards, vtgate will become aware of the new
shards and you do not have to update your application at all.

Another Vitess feature is the vtgate v3 API. Once enabled, a sharded Vitess
setup becomes a drop-in replacement for an unsharded MySQL/MariaDB setup.


Vtgate API Versions

This driver supports the vtgate API versions v1 and v3, but not v2.

For initial experiments, we recommend to use v1 and then later switch to v3.

The API versions differ as follows:


Vtgate API v1

v1 requires for each query to specify the target shard. vtgate will then
route the query accordingly.

This driver supports the v1 API at the connection level. For example, to open a
Vitess database connection for queries targetting shard "0" in the keyspace
"test_keyspace", call:

  vitessdriver.OpenShard(vtgateAddress, "test_keyspace", "0", 30*time.Second)

This mode is recommended for initial experiments or when migrating from a
pure MySQL/MariaDB setup to an unsharded Vitess setup.


Vtgate API v2

v2 requires for each query to specify the affected sharding keys
("keyspace_id").

Based on the provided sharding key, vtgate will then route the query to the
shard which currently covers that sharding key.

This driver currently does not support the v2 API because it's difficult to
enhance the Go SQL interface with the sharding key information at the
query-level.
Instead, it's recommended to use the v3 API.


Vtgate API v3

v3 automatically infers the sharding key (and therefore the target shard)
from the query itself.

It also maintains secondary indexes across shards and generates values for
AUTO_INCREMENT columns.

Unlike v1, v3 (and also v2) are also agnostic to resharding events.

See the vtgate v3 Features doc for an overview:
https://github.com/youtube/vitess/blob/master/doc/VTGateV3Features.md

To enable vtgate v3, a so-called "VSchema" must be defined. As of 12/2015, this
is not documented yet as we are in the process of simplifing the VSchema
definition and the overall process for creating one. If you want to create your
own VSchema, we recommend to have a look at the VSchema from the vtgate v3 demo:

https://github.com/youtube/vitess/blob/master/examples/demo/schema/vschema.json

(The demo itself is interactive and can be run by executing "./run.py" in the
"examples/demo/" directory.)

The vtgate v3 design doc, which we will also update and simplify in the future,
contains more details on the VSchema:
https://github.com/youtube/vitess/blob/master/doc/VTGateV3Design.md
*/
package vitessdriver
