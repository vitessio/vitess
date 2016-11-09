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
    // Connect to vtgate.
    db, err := vitessdriver.Open("localhost:15991", "keyspace", "master", 30*time.Second)

    // Use "db" via the Golang sql interface.
  }

For a full example, please see: https://github.com/youtube/vitess/blob/master/examples/local/client.go

The full example is based on our tutorial for running Vitess locally: http://vitess.io/getting-started/local-instance.html


Vtgate

This driver connects to a vtgate process. In Vitess, vtgate is a proxy server
that routes queries to the appropriate shards.

By decoupling the routing logic from the application, vtgate enables Vitess
features like consistent, application-transparent resharding:
When you scale up the number of shards, vtgate becomes aware of the new shards.
You do not have to update your application at all.

VTGate is capable of breaking up your query into parts, routing them to the
appropriate shards and combining the results, thereby giving the semblance
of a unified database.

The driver uses the V3 API which doesn't require you to specify routing
information. You just send the query as if Vitess was a regular database.
VTGate analyzes the query and uses additional metadata called VSchema
to perform the necessary routing. See the vtgate v3 Features doc for an overview:
https://github.com/youtube/vitess/blob/master/doc/VTGateV3Features.md

As of 12/2015, the VSchema creation is not documented yet as we are in the
process of simplifying the VSchema definition and the overall process for
creating one.
If you want to create your own VSchema, we recommend to have a
look at the VSchema from the vtgate v3 demo:
https://github.com/youtube/vitess/blob/master/examples/demo/schema

(The demo itself is interactive and can be run by executing "./run.py" in the
"examples/demo/" directory.)

The vtgate v3 design doc, which we will also update and simplify in the future,
contains more details on the VSchema:
https://github.com/youtube/vitess/blob/master/doc/V3VindexDesign.md


Isolation levels

The Vitess isolation model is different from the one exposed by a traditional database.
Isolation levels are controlled by connection parameters instead of Go's IsolationLevel.
You can perform master, replica or rdonly reads. Master reads give you read-after-write
consistency. Replica and rdonly reads give you eventual consistency. Replica reads
are for satisfying OLTP workloads while rdonly is for OLAP.

All transactions must be sent to the master where writes are allowed.
Replica and rdonly reads can only be performed outside of a transaction. So, there is
no concept of a read-only transaction in Vitess.

Consequently, no IsolationLevel must be specified while calling BeginContext. Doing so
will result in an error.


Named arguments

Vitess supports positional or named arguments. However, intermixing is not allowed
within a statement. If using named arguments, the ':' and '@' prefixes are optional.
If they're specified, the driver will strip them off before sending the request over
to VTGate.
*/
package vitessdriver
