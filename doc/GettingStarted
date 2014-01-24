# Getting Started
## Dependencies

* [Go](http://golang.org)
* [Google MySQL](https://code.google.com/r/sougou-vitess-mysql/), we are working on
  [MariaDB](https://mariadb.org/) compatibility.
* [ZooKeeper](http://zookeeper.apache.org/) or another lock service implementation.
* [Memcached](http://memcached.org)
* [Python](http://python.org) (for the client and testing).

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
