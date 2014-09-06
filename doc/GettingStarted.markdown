# Getting Started
If you run into issues or have questions, you can use our mailing list: vitess@googlegroups.com.

## Dependencies

* [Go](http://golang.org): Needed for building Vitess.
* [MariaDB](https://mariadb.org/): We currently develop with version 10.0.13.
  Other 10.0.x versions may also work.
* [ZooKeeper](http://zookeeper.apache.org/): By default, Vitess
  uses Zookeeper as the lock service. It is possible to plug in
  something else as long as the new service supports the
  necessary API functions.
* [Memcached](http://memcached.org): Used for the rowcache.
* [Python](http://python.org): For the client and testing.

## Installation

[Install Go](http://golang.org/doc/install).

[Install MariaDB](https://downloads.mariadb.org/).
You can use any installation method (src/bin/rpm/deb),
but be sure to include the client development headers (**libmariadbclient-dev**).

Then download and build Vitess. Note that the value of MYSQL_FLAVOR is case-sensitive.
If the mysql_config command from libmariadbclient-dev is not on the PATH,
you'll need to *export VT_MYSQL_ROOT=/path/to/mariadb* before running bootstrap.sh,
where mysql_config is found at /path/to/mariadb/**bin**/mysql_config.

``` sh
cd $WORKSPACE
sudo apt-get install automake libtool memcached python-dev python-mysqldb libssl-dev g++ mercurial git pkg-config bison curl
git clone https://github.com/youtube/vitess.git src/github.com/youtube/vitess
cd src/github.com/youtube/vitess
export MYSQL_FLAVOR=MariaDB
./bootstrap.sh
. ./dev.env
make build
```

To run the tests:

``` sh
make  # run the tests
```

Note: If you see failing tests, it may be that your disk is too slow for the testsuite, leading to timeouts. You might try [testing against a ramdisk](TestingOnARamDisk.markdown).

## Setting up a cluster
TODO: Expand on all sections
### Setup zookeeper
### Start a MySql instance
### Start vttablet
### Start vtgate
### Write a client
### Test
