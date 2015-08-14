This document explains the client library strategy for Vitess. A Vitess cluster can be accessed
by a variety of clients, written in different languages. We provide a unified client strategy
for each language we support.

Vitess's service is exposed through a proto3 service definition, and we support gRPC.
So we therefore support all languages the gRPC framework supports. The RPC layer is however
a bit raw to use as is, so we implement a thin layer on the RPC API to the vtgate server pool.
Each client provides a single connection abstraction to the vtgate service.
The RPC API is described in the proto file. (TODO: link to the proto generated doc).

## Core Principles for the Client Libraries

The following principles are followed by each client library:

* Each client should if possible fully implement the server API, and provide access to all methods:
  * Execution of queries targeted to a specific set of shards (the ...Shards methods).
  * Execution of queries targeted by sharding key (the ...KeyspaceIds, ...KeyRanges and ...EntityIds methods).
  * Execution of queries using the v3 API, which chooses a target automatically (Experimental).
  * Transaction methods (Begin, Commit, Rollback).
  * Map-Reduce helper method (SplitQuery).
  * Topology method (GetSrvKeyspace).
* The connection object should be thread-safe if applicable, and allow the multiplexing of multiple queries on the same connection (streaming queries, transactions, ...).
* A lower level plug-in abstraction should be used, so the transport used can be plugged-in, and we can use different RPC frameworks. Note if the proto3 library is available in the language, it is preferable to use the data types generated from our proto files in the API.
* A higher level object abstraction should be available so the user doesn't have to keep track of the Session.
* Language specific idiomatic constructs to provide helpful constructs to application developers. Integration with language database drivers will lower the barrier of entry for application developers. Full integration may require the use of v3 API:
  * For Python, compliance with DB API.
  * For Java, compliance with JDBC.
  * For PHP, compliance with PDO.
  * For Go, the database/sql package.
* If a well-known Map-Reduce framework exists for the language, both a data source and a sink should be provided for that language. For instance, in Java, we provide a Hadoop data source and a sink.
* Higher level libraries can also be provided if they make sense. For instance, we have object-based helper classes in Python (used by YouTube) that we provide.
* Expose the same set of well-documented error codes to the user. (TODO: link to the proto error doc).
* Use vtgateclienttest to fully unit-test all API calls (vtgateclienttest is a small server that can simulate a real server and return specific responses to allow full client feature coverage).

## RPC Frameworks

We started Vitess with BSON-RPC, which is BSON-encoded data structures on top of the Go RPC framework. However, limitations in that framework, and the availability of gRPC, made us switch to [gRPC](https://github.com/grpc).

We now define our services using [proto files](https://github.com/google/protobuf), and the proto3 syntax.

We are not however constrained by it. The plug-in client strategy we follow allows us to easily send the proto3 objects over any transport (for instance, within Google, we use proto2 objects over Stubby).

## Java

TODO: fill in more information, once refactor is done.

## PHP

TODO: fill in more information.

## Go

The Go client exposes the entire API. It also provides an adapter for the go native database/sql library, based on the v3 API.

No map-reduce integration is supported yet.

## Python

TODO: fill in more information.

