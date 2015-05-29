# Vitess <p align="right">[![Build Status](https://travis-ci.org/youtube/vitess.svg?branch=master)](https://travis-ci.org/youtube/vitess/builds) [![Coverage Status](https://coveralls.io/repos/youtube/vitess/badge.png)](https://coveralls.io/r/youtube/vitess)</p>

Vitess is a storage platform for scaling MySQL.
It is optimized to run as effectively in cloud architectures as it does on dedicated hardware.
It combines many important features of MySQL with the scalability of a NoSQL database.

It's been actively developed since 2011, and is currently used as
a fundamental component of YouTube's MySQL infrastructure, serving thousands of
QPS per server. If you want to find out whether Vitess is a good fit for your
project, please visit [vitess.io](http://vitess.io).

There are a couple of videos from [sougou](https://github.com/sougou) that you can watch:
a [short intro](http://youtu.be/midJ6b1LkA0) prepared for Google I/O 2014
and a more [detailed presentation from @Scale '14](http://youtu.be/5yDO-tmIoXY).

## Documentation

### Intro

 * [Helicopter overview](http://vitess.io):
     high level overview of Vitess that should tell you whether Vitess is for you.
 * [Sharding in Vitess](http://vitess.io/doc/Sharding)
 * [Frequently Asked Questions](http://vitess.io/doc/FAQ).

### Using Vitess

 * Getting Started
  * [On Kubernetes](http://vitess.io/getting-started/).
  * [From the ground up](http://vitess.io/doc/GettingStarted).
 * [Tools](http://vitess.io/doc/Tools):
     all Vitess tools and servers.
 * [vttablet/vtocc](http://vitess.io/doc/vtocc):
     information about the most important Vitess server.
 * [Reparenting](http://vitess.io/doc/Reparenting):
     performing master failover.
 * [Resharding](http://vitess.io/doc/Resharding):
     adding more shards to your cluster.
 * [Schema management](http://vitess.io/doc/SchemaManagement):
     managing your database schema using Vitess.

### Reference

 * [General Concepts](http://vitess.io/doc/Concepts)
 * [Topology Service](http://vitess.io/doc/TopologyService)
 * [V3 design](http://vitess.io/doc/VTGateV3)

## Contact

Ask questions in the
[vitess@googlegroups.com](https://groups.google.com/forum/#!forum/vitess)
discussion forum.

Subscribe to
[vitess-announce@googlegroups.com](https://groups.google.com/forum/#!forum/vitess-announce)
for low-frequency updates like new features and releases.

## License

Unless otherwise noted, the vitess source files are distributed
under the BSD-style license found in the LICENSE file.
