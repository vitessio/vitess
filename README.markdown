# Vitess

Vitess is a set of servers and tools meant to facilitate scaling of MySQL
databases for the web. It's currently used as a fundamental component of
YouTube's MySQL infrastructure, serving thousands of QPS (per server).

[sougou](https://github.com/sougou) presented Vitess at Fosdem '14 in the go
devroom. [Here are the
slides](https://github.com/youtube/vitess/blob/master/doc/Vitess2014.pdf?raw=true),
and here is the [video](http://youtu.be/qATTTSg6zXk).

## Trying it out

Vitess is not entirely ready for unsupervised use yet. Some functionality is
still under development, APIs may change, and parts of the code are
undocumented. However, if you feel adventurous, you're more than welcome to try
it. We know that there are some rough edges, so please don't hesitate to reach
to us through [our mailing list](https://groups.google.com/forum/#!forum/vitess)
if you run into any issues. Warnings aside, please take a look at our [getting
started
doc](https://github.com/youtube/vitess/blob/master/doc/GettingStarted.markdown).

## Documentation

 * [Vision](https://github.com/youtube/vitess/blob/master/doc/Vision.markdown)
 * [General Concepts](https://github.com/youtube/vitess/blob/master/doc/Concepts.markdown)
 * [Replication Graph](https://github.com/youtube/vitess/blob/master/doc/ReplicationGraph.markdown)
 * [Serving graph](https://github.com/youtube/vitess/blob/master/doc/ServingGraph.markdown)
 * [Tools](https://github.com/youtube/vitess/blob/master/doc/Tools.markdown)
 * [Getting Started](https://github.com/youtube/vitess/blob/master/doc/GettingStarted.markdown)
 * [Preparing for production](https://github.com/youtube/vitess/blob/master/doc/Production.markdown)
 * [Reparenting](https://github.com/youtube/vitess/blob/master/doc/Reparenting.markdown)
 * [Resharding](https://github.com/youtube/vitess/blob/master/doc/Resharding.markdown)
 * [Schema management](https://github.com/youtube/vitess/blob/master/doc/SchemaManagement.markdown)
 * [Zookeeper data](https://github.com/youtube/vitess/blob/master/doc/ZookeeperData.markdown)

## License

Unless otherwise noted, the vitess source files are distributed
under the BSD-style license found in the LICENSE file.
