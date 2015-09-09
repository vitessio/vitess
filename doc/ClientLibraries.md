**Contents:**

<div id="toc"></div>

## Overview

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
  database drivers, though this support is not yet provided:
  * Go: [database/sql package](http://golang.org/pkg/database/sql/)
  * Java: [JDBC](https://docs.oracle.com/javase/tutorial/jdbc/index.html)
    compliance
  * PHP: [PHP Data Objects \(PDO\)](http://php.net/manual/en/intro.pdo.php)
    compliance
  * Python: [DB API](https://www.python.org/dev/peps/pep-0249/) compliance
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

The Go client exposes the entire API. It also provides an adapter
based on the v3 API for the Go native
[database/sql](http://golang.org/pkg/database/sql/).

The Go client does not yet support MapReduce integration.

### Java

Content to be filled in.

### PHP

Content to be filled in.

### Python

Content to be filled in.
