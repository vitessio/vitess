[![Build Status](https://travis-ci.org/youtube/vitess.svg?branch=master)](https://travis-ci.org/youtube/vitess/builds)
[![codebeat](https://codebeat.co/badges/0d74f7ec-17d8-4b5a-b00e-cf8194cab5b5)](https://codebeat.co/projects/github-com-youtube-vitess)
[![Go Report Card](https://goreportcard.com/report/github.com/youtube/vitess)](https://goreportcard.com/report/github.com/youtube/vitess)



# Vitess 

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
 * [Sharding in Vitess](http://vitess.io/user-guide/sharding.html)

### Using Vitess

 * Getting Started
  * [On Kubernetes](http://vitess.io/getting-started/).
  * [From the ground up](http://vitess.io/getting-started/local-instance.html).
 * [Architecture](http://vitess.io/overview/#architecture):
     all Vitess tools and servers.
 * [Reparenting](http://vitess.io/doc/Reparenting):
     performing master failover.
 * [Resharding](http://vitess.io/user-guide/sharding.html#resharding):
     adding more shards to your cluster.
 * [Schema management](http://vitess.io/doc/SchemaManagement):
     managing your database schema using Vitess.

### Reference

 * [General Concepts](http://vitess.io/overview/concepts.html)
 * [Topology Service](http://vitess.io/doc/TopologyService)
 * [VTGate V3](http://vitess.io/doc/VTGateV3Features/)

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
