# Getting Started
If you run into issues or have questions, you can use our mailing list: vitess@googlegroups.com.

## Dependencies

* [Go](http://golang.org): Needed for building vitess.
* [Google MySQL](https://code.google.com/r/sougou-vitess-mysql/):
  We plan to support [MariaDB](https://mariadb.org/) in the future.
  We currently depend on
  [Google MySQL's group_id](https://code.google.com/p/google-mysql-tools/wiki/GlobalTransactionIds)
  capabilities for some of the maintenance operations like
  reparenting, etc.
* [ZooKeeper](http://zookeeper.apache.org/): By default, Vitess
  uses Zookeeper as the lock service. It is possible to plug in
  something else as long as the new service supports the
  necessary API functions.
* [Memcached](http://memcached.org): Used for the rowcache.
* [Python](http://python.org): For the client and testing.

## Installation

[Install Go](http://golang.org/doc/install).

``` sh
cd $WORKSPACE
sudo apt-get install automake libtool memcached python-dev python-mysqldb libssl-dev g++ mercurial git
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

## Setting up a cluster
TODO: Expand on all sections
### Setup zookeeper
### Start a MySql instance
### Start vttablet
### Start vtgate
### Write a client
### Test
