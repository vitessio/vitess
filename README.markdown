Vitess
======

Vitess is a set of servers and tools meant to facilitate scaling of MySQL databases for the web.
It's currently used as a fundamental component of YouTube's MySQL infrastructure.

[sougou](https://github.com/sougou) will be presenting Vitess at [Fosdem '14 in the go devroom](https://fosdem.org/2014/schedule/track/go/). Feel free to drop by if you're planning
on attending.

Features
--------

This is an incomplete list of Vitess features.

### Clients

* A Python DBAPI 2.0 compliant client interface.
* A go client interface.
* Simple [BSON](http://http://bsonspec.org/) based protocol with SASL
  authentication.

### vttablet

Smart middleware sitting in front of MySQL and serving clients
requests.

* Connection pooling.
* SQL parser: Although very close, the vtocc SQL parser is not SQL-92
  compliant. It has left out constructs that are deemed uncommon or
  OLTP-unfriendly. It should, however, allow most queries used by a
  well-behaved web application.
* Query rewrite and sanitation (adding limits, avoiding non-deterministic updates).
* Query consolidation: reuse the results of an in-flight query to any
  subsequent requests that were received while the query was still
  executing.
* Rowcache: the mysql buffer cache is optimized for range scans over
  indices and tables. Unfortunately, it’s not good for random access
  by primary key. The rowcache will instead maintain a row based cache
  (using [memcached](http://memcached.org/) as its backend) and keep it
  consistent by fielding all DMLs that could potentially affect them.
* Update stream: A server that streams the list of rows that are changing
  in the database, which can be used as a mechanism to continuously export
  the data to another data store.
* Integrated query killer for queries that take too long to return
  data.
* Discard idle backend connections to avoid offline db errors.
* Transaction management: Ability to limit the number of concurrent
  transactions and manage deadlines.
	  
### Management

* Cluster configuration is stored using pluggable lock service
  backends, with the [ZooKeeper](http://zookeeper.apache.org/) enabled
  by default.
* *vtctl*: command line tool that allows to do most management
  operations, like initializing a server, changing its type, etc.
* Separate replication and serving graph management.
* *vtctld*: HTTP daemon giving an overview of the configuration
   (there's also a JSON API available).
* Fast database snaphsotting and reloading (much faster than mysqldump
  and LOAD DATA INFILE).

### Sharding 

* Keyspaces: All tables in a sharded database need to contain a “key”
  column. Vitess will use these values to decide the target shard for
  such data. All tables that are indexed by a set of keys are known as
  a *keyspace*, which basically represents the logical database that
  combines all the shards that store them.
* Range based sharding: The main advantage of this scheme is that the
  shard map is a simple in-memory lookup. The downside of this scheme
  is that it creates hot-spots for sequentially increasing keys. In
  such cases, we recommend that the application hash the keys so they
  distribute more randomly.

### Replication

* Split replication: replicate only part of the replication stream
  basing on provided criteria (useful for vertical or horizontal
  resharding).

### Tools

* *zkocc*: ZooKeeper connection pooler and cache.
* *zkctl*: manage ZooKeeper instances.
* *mysqlctl*: manage MySQL instances.
* *zk*: command line ZooKeeper client and explorer.

Dependencies
------------

* [Go](http://golang.org)
* [MySQL](http://mysql.com), we are working on
  [MariaDB](https://mariadb.org/) compatibility.
* [ZooKeeper](http://zookeeper.apache.org/) or another lock service implementation.
* [Memcached](http://memcached.org)
* [Python](http://python.org) (for the client and testing).

Development
-----------

[Install Go](http://golang.org/doc/install).

``` sh
cd $WORKSPACE
sudo apt-get install automake libtool flex bison memcached python-dev python-mysqldb libssl-dev g++ mercurial git
go get code.google.com/p/opts-go
git clone git@github.com:youtube/vitess.git src/github.com/youtube/vitess
cd src/github.com/youtube/vitess
./bootstrap.sh
. ./dev.env
```

Optionally:

``` sh
VTDATAROOT=... #  $VTROOT/vt if not set
VTPORTSTART=15000
```

To run the tests:

``` sh
make  # run the tests
```

License
-------

Unless otherwise noted, the vitess source files are distributed
under the BSD-style license found in the LICENSE file.
