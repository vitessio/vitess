# Vitess

Vitess is a set of servers and tools meant to facilitate scaling of MySQL
databases for the web. It's been developed since 2011, and is currently used as
a fundamental component of YouTube's MySQL infrastructure, serving thousands of
QPS (per server). If you want to find out whether Vitess is a good fit for your
project, please read our [helicopter
overview](https://github.com/youtube/vitess/blob/master/doc/HelicopterOverview.markdown).

## Overview

![Overview](https://raw.githubusercontent.com/youtube/vitess/master/doc/VitessOverview.png)

Vitess consists of a number servers, command line utilities, and a consistent
metadata store. Taken together, they allow you to serve more database traffic,
and add features like sharding, which normally you would have to implement in your
application.

**vttablet** is a server that sits in front of a MySQL database, making it more
robust and available in the face of high traffic. Among other things, it adds a
connection pool, has a row based cache, and it rewrites SQL queries to be safer
and nicer to the underlying database.

**vtgate** is a very light proxy that routes database traffic from your app to the
right vttablet, basing on the sharding scheme, latency required, and health of
the vttablets. This allows the client to be very simple, as all it needs to be
concerned about is finding the closest vtgate.

The **topology** is a metadata store that contains information about running
servers, the sharding scheme, and replication graph. It is backed by a
consistent data store, like [Apache ZooKeeper](http://zookeeper.apache.org/).
The topology backends are plugin based, allowing you to write your own if
ZooKeeper doesn't fit your needs. You can explore the topology through
**vtctld**, a webserver (not shown in the diagram).

**vtctl** is a command line utility that allows a human or a script to easily
interact with the system.

All components communicate using a lightweight RPC system based on
[BSON](http://bsonspec.org/). The RPC system is plugin based, so you can easily
write your own backend (at Google we use a Protocol Buffers based protocol). We
provide a client implementation for three languages: Python, Go, and Java.
Writing a client for your language should not be difficult, as it's a matter of
implementing only a few API calls (please send us a pull request if you do!).

To learn more, please click on the documentation links below.
You can also watch a [9 minute introduction](https://www.youtube.com/watch?v=midJ6b1LkA0)
to Vitess [sougou](https://github.com/sougou) prepared for Google I/O 2014.
There's also a longer presentation from the Fosdem '14 go devroom
([slides](https://github.com/youtube/vitess/blob/master/doc/Vitess2014.pdf?raw=true),
[video](http://youtu.be/qATTTSg6zXk)).

## Trying it out

Vitess is not entirely ready for unsupervised use yet. Some functionality is
still under development, APIs may change, and parts of the code are
undocumented. However, if you feel adventurous, you're more than welcome to try
it. We know that there are some rough edges, so please don't hesitate to reach out
to us through [our mailing list](https://groups.google.com/forum/#!forum/vitess)
if you run into any issues. Warnings aside, please take a look at our [Getting
Started](https://github.com/youtube/vitess/blob/master/doc/GettingStarted.markdown)
guide.

## Documentation

### Intro
 * [Helicopter overview](https://github.com/youtube/vitess/blob/master/doc/HelicopterOverview.markdown):
     high level overview of Vitess that should tell you whether Vitess is for you.
 * [Frequently Asked Questions](https://github.com/youtube/vitess/blob/master/doc/FAQ.markdown).
 * [Vision](https://github.com/youtube/vitess/blob/master/doc/Vision.markdown):
     principles guiding the design of Vitess.

### Using Vitess

 * [Getting Started](https://github.com/youtube/vitess/blob/master/doc/GettingStarted.markdown):
     how to set your environment to work with Vitess.
 * [Tools](https://github.com/youtube/vitess/blob/master/doc/Tools.markdown):
     all Vitess tools and servers.
 * [Vttablet](https://github.com/youtube/vitess/blob/master/doc/Vttablet.markdown):
     information about the most important Vitess server.
 * [Reparenting](https://github.com/youtube/vitess/blob/master/doc/Reparenting.markdown):
     performing master failover.
 * [Resharding](https://github.com/youtube/vitess/blob/master/doc/Resharding.markdown):
     adding more shards to your cluster.
 * [Preparing for production](https://github.com/youtube/vitess/blob/master/doc/Production.markdown) (wip).
 * [Schema management](https://github.com/youtube/vitess/blob/master/doc/SchemaManagement.markdown):
     managing your database schema using Vitess.

### Reference

 * [General Concepts](https://github.com/youtube/vitess/blob/master/doc/Concepts.markdown)
 * [Zookeeper data](https://github.com/youtube/vitess/blob/master/doc/ZookeeperData.markdown)
 * [Serving graph](https://github.com/youtube/vitess/blob/master/doc/ServingGraph.markdown)
 * [Replication Graph](https://github.com/youtube/vitess/blob/master/doc/ReplicationGraph.markdown)

## License

Unless otherwise noted, the vitess source files are distributed
under the BSD-style license found in the LICENSE file.
