You can access your Vitess cluster using a variety of clients and
programming languages. Vitess client libraries help your client
application to more easily talk to your storage system to query data.

Vitess' service is exposed through a
[proto3](https://developers.google.com/protocol-buffers/docs/proto3)
service definition. Vitess supports [gRPC](http://www.grpc.io/),
and you can use the 
[proto compiler](https://developers.google.com/protocol-buffers/docs/proto?hl=en#generating)
to generate stubs that can call the API in any language that the
gRPC framework supports.

This document explains the client library strategy for Vitess.

## Core Principles

Vitess client libraries follow these core principles:

* Libraries fully implement the server API and provide access to all
  API methods, which support the following types of functions:
  * Transactions (begin, commit, rollback)
  * Queries targeted to a specific shards or groups of shards
  * Queries targeted based on sharding key values
  * Queries that monitor sharding configurations  
  * MapReduce operations
* Libraries expose an identical set of well-documented error codes.
* Libraries provide a single-connection abstraction to the
  <code>vtgate</code> service. The connection object should be
  thread-safe, if applicable, and should support multiplexing for
  streaming queries, transactions, and other operations that rely
  on multiple queries.
* A low-level plug-in abstraction enables the transport to be plugged
  in so that the application can use different RPC frameworks. For
  instance, within Google, we use proto2 objects with an internal
  RPC system.<br class="bigbreak">**Note:** If the proto3 library is available in
  your language, we recommend that your API calls use the data types
  generated from our .proto files.
* A high-level object abstraction allows the user to execute transactions
  without having to keep track of the database session.
* Libraries support <code>vtgateclienttest</code>, enabling you to
  fully unit-test all API calls. <code>vtgateclienttest</code> is
  a small server that simulates a real <code>vtgate</code> server
  and returns specific responses to allow for full client feature
  coverage.

## Language-specific considerations
* Each client library should support language-specific, idiomatic
  constructs to simplify application development in that language.
* Client libraries should integrate with the following language-specific
  database drivers. For example, current languages implement the following:
  * Go: [database/sql package](http://golang.org/pkg/database/sql/)
  * Java: [JDBC](https://docs.oracle.com/javase/tutorial/jdbc/index.html)
  * PHP: [PHP Data Objects \(PDO\)](http://php.net/manual/en/intro.pdo.php)
  * Python: [PEP 0249 DB API](https://www.python.org/dev/peps/pep-0249/)
* Libraries provide a thin wrapper around the proto3 service definitions.
  Those wrappers could be extended with adapters to higher level libraries
  like SQLAlchemy (Python) or JDBC (Java), with other object-based helper
  classes in the relevant language, or both.
* If a well-known MapReduce framework exists for the language, the client
  library should provide a data source and a data sink. For example, you
  can read data from Vitess inside a Hadoop MapReduce job and save the
  output into Vitess.

## Available libraries

### Go

The Go client interface is in the
["vtgateconn" package](https://godoc.org/github.com/youtube/vitess/go/vt/vtgate/vtgateconn).

There are multiple implementations available. We recommend to use the
["grpc" implementation](https://godoc.org/github.com/youtube/vitess/go/vt/vtgate/grpcvtgateconn).
Load it by importing its package:

``` go
import "github.com/youtube/vitess/go/vt/vtgate/grpcvtgateconn"
```

When you connect to vtgate, use the
[`DialProtocol` method](https://godoc.org/github.com/youtube/vitess/go/vt/vtgate/vtgateconn#DialProtocol)
and specify "grpc" as protocol.
Alternatively, you can set the
[command line flag "vtgate_protocol"](https://github.com/youtube/vitess/blob/ff800b2a1801f0bb8b0c29a701d9c0988bf827e2/go/vt/vtgate/vtgateconn/vtgateconn.go#L27)
to "grpc".

The Go client interface has multiple `Execute*()` methods for different use-cases
and sharding configurations. When you start off with an unsharded database, we
recommend to use the
[ExecuteShards method](https://godoc.org/github.com/youtube/vitess/go/vt/vtgate/vtgateconn#VTGateConn.ExecuteShards)
and pass "0" as only shard.

For an example how to use the Go client, see the end-to-end test
[local_cluster_test.go](https://github.com/youtube/vitess/blob/master/go/vt/vttest/local_cluster_test.go).
From this test file, you can also reuse the "LaunchVitess" call to
instantiate a minimal Vitess setup (including a MySQL server). This way you can
test your application against an actual instance.

### Java

* [Java client](https://github.com/youtube/vitess/blob/master/java/client/src/main/java/io/vitess/client/VTGateConn.java)

### PHP

* [PHP client](https://github.com/youtube/vitess/tree/master/php)

### Python

* [Python client](https://github.com/youtube/vitess/blob/master/py/vtdb/vtgate_client.py)
