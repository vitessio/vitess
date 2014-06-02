# Vitess

Vitess is a set of servers and tools meant to facilitate scaling of MySQL databases for the web.
It's currently used as a fundamental component of YouTube's MySQL infrastructure.

[sougou](https://github.com/sougou) presented Vitess at Fosdem '14 in the go devroom. [Here are the slides](https://github.com/youtube/vitess/blob/master/doc/Vitess2014.pdf?raw=true), and here is the [video](http://youtu.be/qATTTSg6zXk).

## Curent State of the Project
- Makes MySQL scalable for real.
- YouTube's production-hardened codebase.
- Functionality and APIs are under active development.
- There is not much of a documentation except the source code itself.
- Not yet ready for unsupervised usage.

But if you feel adventurous, you're welcome to try it. (And by the way, it's awesome!)
If you run into issues, please subscribe and post to [our mailing list](https://groups.google.com/forum/#!forum/vitess).
We'll do our best to help you.

## How can I Try It out?
If you run into issues or have questions, you can use our mailing list: vitess@googlegroups.com

> We assume that you use Ubuntu. If not then you're smart enough to figure it out yourself.

- Install dependencies first ():
  - [Go language](http://golang.org/): 
    `sudo apt-get install golang`
  - Build dependencies:
    `sudo apt-get install automake libtool bison memcached  python-mysqldb`
    - We expect that these dependencies are already installed: 
      `sudo apt-get git mercurial g++ libssl-dev pkg-config python-dev`
- Now it's time to check out code and build it!
  - Choose the directory where your vitess files would live and `cd` there
  - Download vitess source code: `git clone https://github.com/youtube/vitess.git src/github.com/youtube/vitess`
  - Go into vitess source folder: `cd src/github.com/youtube/vitess`
  - Time to build it! `./bootstrap.sh`
    - Btw, Ubuntu Trusty is known to brake MySQL build, use this workaround: 
      `export MYSQL_FLAVOR=MariaDB`
    - This process takes its time, it builds MySQL and ZooKeeper from source.
    - This should completely configure your environment to use vitess.
  - You'll need to have your environment variables for development: `. ./dev.env`
    - `$VTTOP` points to vitess source code.
    - `$VTROOT` points to root directory where all the files are installed.
- You can run tests now: `make`
- *TODO: Add information on how to start test instance and play with it*

## List of External Dependencies

* [Go](http://golang.org): Needed for building vitess.
* [Google MySQL](https://code.google.com/r/sougou-vitess-mysql/):
  * We plan to support [MariaDB](https://mariadb.org/) in the future.
  * We currently depend on
  [Google MySQL's group_id](https://code.google.com/p/google-mysql-tools/wiki/GlobalTransactionIds)
  capabilities for some of the maintenance operations like
  reparenting, etc.
* [ZooKeeper](http://zookeeper.apache.org/): By default, Vitess
  uses Zookeeper as the lock service. It is possible to plug in
  something else as long as the new service supports the
  necessary API functions.
* [Memcached](http://memcached.org): Used for the rowcache.
* [Python](http://python.org): For the client and testing.

<!-- 

Optionally:

``` sh
VTDATAROOT=... #  $VTROOT/vt if not set
VTPORTSTART=15000
```

## Setting up a cluster
TODO: Expand on all sections
### Setup zookeeper
### Start a MySql instance
### Start vttablet
### Start vtgate
### Write a client
### Test
-->

## More Documentation to Read

* [Vision](https://github.com/youtube/vitess/blob/master/doc/Vision.markdown)
* Concepts 
 * [General Concepts](https://github.com/youtube/vitess/blob/master/doc/Concepts.markdown)
 * [Replication Graph](https://github.com/youtube/vitess/blob/master/doc/ReplicationGraph.markdown)
 * [Serving graph](https://github.com/youtube/vitess/blob/master/doc/ServingGraph.markdown)
 * [What is VtTablet?](https://github.com/youtube/vitess/blob/master/doc/VtTablet.markdown)
* [Tools](https://github.com/youtube/vitess/blob/master/doc/Tools.markdown)
* Operations
 * [Preparing for production](https://github.com/youtube/vitess/blob/master/doc/Production.markdown)
 * [Reparenting](https://github.com/youtube/vitess/blob/master/doc/Reparenting.markdown)
 * [Resharding](https://github.com/youtube/vitess/blob/master/doc/Resharding.markdown)
 * [Schema management](https://github.com/youtube/vitess/blob/master/doc/SchemaManagement.markdown)
 * [Zookeeper data](https://github.com/youtube/vitess/blob/master/doc/ZookeeperData.markdown)

# License

Unless otherwise noted, the vitess source files are distributed
under the BSD-style license found in the LICENSE file.
