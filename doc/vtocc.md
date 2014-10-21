# vtocc

vtocc is a smart proxy to mysql/mariadb. It's basically a subset of
vttablet that concerns itself with just serving queries for a
single database. It's a standalone program/server that can be
launched without the need of the rest of the vitess ecosystem.
It can be pointed to an existing database, and you should be able
to send queries through it.

## vtocc features
* Connection pooling.
* SQL parser: Although very close, the vtocc SQL parser is not SQL-92
  compliant. It has left out constructs that are deemed uncommon or
  OLTP-unfriendly. It should, however, allow most queries used by a
  typical web application.
* Query rewrite and sanitation (adding limits, avoiding non-deterministic updates).
* Query de-duping: reuse the results of an in-flight query to any
  subsequent requests that were received while the query was still
  executing.
* Rowcache: the mysql buffer cache is optimized for range scans over
  indices and tables. Unfortunately, it’s not good for random access
  by primary key. The rowcache will instead maintain a row based cache
  (using [memcached](http://memcached.org/) as its backend) and keep it
  consistent by fielding all DMLs that could potentially affect them.
* Query blacklisting: You can specify a set of rules to blacklist queries
  that are potentially problematic.
* Table ACLs: Allow you to specify ACLs for tables based on the connected
  user.
* Update stream: A server that streams the list of rows that are changing
  in the database, which can be used as a mechanism to continuously export
  the data to another data store.
* Query killer for queries that take too long to return data.
* Transaction management: Ability to limit the number of concurrent
  transactions and manage deadlines.
* Streaming queries to serve OLAP workloads.
* A rich set of monitoring features to watch over, diagnose or analyze performance.

## Protocol
vtocc uses the bsonrpc protocol. This means that it uses [bson encoding](http://bsonspec.org)
to receive and send messages. There is currently a [python client](https://github.com/youtube/vitess/blob/master/py/vtdb/tablet.py). A java client is
also getting implemented.

If you are familiar with go, you can actually plug in any protocol you desire, like json,
thrift or protobufs.

## Data types
vtocc has not been well tested with exotic data types. Specifically, we don't know how it
will handle boolean and timestamp columns. Otherwise, we have [tests](https://github.com/youtube/vitess/blob/master/test/test_data/test_schema.sql#L45) for
the commonly used data types.

vtocc can work with latin-1 or utf-8 encoded databases. It's highly recommended that you match
client and server side encoding. vtocc does not try to do any character set conversions.

vtocc will enable rowcache only for tables that have numbers or binary data types as primary
key columns. This is because other column types are not bitwise comparable. For example,
varchar comparison in MySQL is collation dependent. So, those types are not supported.
