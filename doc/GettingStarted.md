# Getting Started
If you run into issues or have questions, you can use our mailing list: vitess@googlegroups.com.

## Dependencies

* We currently develop on Ubuntu 14.04 (Trusty) and Debian 7.0 (Wheezy).
* You'll need some kind of Java Runtime if you use ZooKeeper.
  We use OpenJDK (*sudo apt-get install openjdk-7-jre*).
* [Go](http://golang.org) 1.3+: Needed for building Vitess.
* [MariaDB](https://mariadb.org/): We currently develop with version 10.0.14.
  Other 10.0.x versions may also work.
* [ZooKeeper](http://zookeeper.apache.org/)
  or [etcd](https://github.com/coreos/etcd) 0.4.6:
  By default, Vitess uses ZooKeeper as the lock service.
  We also have a plugin for etcd. See the Building section below.
  It is possible to plug in something else as long as the new service supports
  the necessary API functions.
* [Memcached](http://memcached.org): Used for the rowcache.
* [Python](http://python.org) 2.7: For the client and testing.

## Building

[Install Go](http://golang.org/doc/install).

[Install MariaDB](https://downloads.mariadb.org/).
You can use any installation method (src/bin/rpm/deb),
but be sure to include the client development headers (**libmariadbclient-dev**).

ZooKeeper 3.3.5 is included by default. If you plan to use it, you don't need
to install anything else.

If you want to use etcd instead, install
[etcd 0.4.6](https://github.com/coreos/etcd/releases/tag/v0.4.6)
and make sure the "etcd" command is on your path.

Then download and build Vitess. Note that the value of MYSQL_FLAVOR is case-sensitive.
If the mysql_config command from libmariadbclient-dev is not on the PATH,
you'll need to *export VT_MYSQL_ROOT=/path/to/mariadb* before running bootstrap.sh,
where mysql_config is found at /path/to/mariadb/**bin**/mysql_config.

``` sh
cd $WORKSPACE
sudo apt-get install make automake libtool memcached python-dev python-mysqldb libssl-dev g++ mercurial git pkg-config bison curl
git clone https://github.com/youtube/vitess.git src/github.com/youtube/vitess
cd src/github.com/youtube/vitess
export MYSQL_FLAVOR=MariaDB
./bootstrap.sh
. ./dev.env
make build
```

## Testing

If you want to use etcd, set the following environment variable:

``` sh
export VT_TEST_FLAGS='--topo-server-flavor=etcd'
```

The full set of tests included in the default _make_ and _make test_ targets
is intended for use by Vitess developers to verify code changes.
These tests simulate a small cluster by launching many servers on the local
machine, so they require a lot of resources (minimum 8GB RAM and SSD recommended).

If you are only interested in checking that Vitess is working in your
environment, you can run a set of lighter tests:

``` sh
make site_test
```

### Common Test Issues

Many common failures come from running the full developer test suite
(_make_ or _make test_) on an underpowered machine. If you still get
these errors with the lighter set of site tests (*make site_test*),
please let us know on the mailing list.

#### Node already exists, port in use, etc.

Sometimes a failed test may leave behind orphaned processes.
If you use the default settings, you can find these by looking for
*vtdataroot* in the command line, since every process is told to put
its files there with a command line flag. For example:

``` sh
pgrep -f -l '(vtdataroot|VTDATAROOT)' # list Vitess processes
pkill -f '(vtdataroot|VTDATAROOT)' # kill Vitess processes
```

#### Too many connections to MySQL, or other timeouts

This often means your disk is too slow. If you don't have access to an SSD,
you can try [testing against a ramdisk](TestingOnARamDisk.md).

#### Connection refused to tablet, MySQL socket not found, etc.

This could mean you ran out of RAM and a server crashed when it tried to allocate more.
Some of the heavier tests currently require up to 8GB RAM.

#### Connection refused in zkctl test

This could indicate that no Java Runtime is installed.

#### Running out of disk space

Some of the larger tests use up to 4GB of temporary space on disk.

## Setting up a cluster

Once you have a successful `make build`, you can proceed to start up a
[local cluster](https://github.com/youtube/vitess/tree/master/examples/local)
for testing.
