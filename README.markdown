# Vitess

Vitess is a set of servers and tools meant to facilitate scaling of MySQL databases for the web.
It's currently used as a fundamental component of YouTube's MySQL infrastructure.

[sougou](https://github.com/sougou) presented Vitess at Fosdem '14 in the go devroom. [Here are the slides](https://github.com/youtube/vitess/blob/master/doc/Vitess2014.pdf?raw=true). We'll share the video links when they become available.

* [Vision](https://github.com/youtube/vitess/blob/master/doc/Vision.markdown)
* Concepts 
 * [General Concepts](https://github.com/youtube/vitess/blob/master/doc/Concepts.markdown)
 * [Replication Graph](https://github.com/youtube/vitess/blob/master/doc/ReplicationGraph.markdown)
 * [Serving graph](https://github.com/youtube/vitess/blob/master/doc/ServingGraph.markdown)
* [Tools](https://github.com/youtube/vitess/blob/master/doc/Tools.markdown)
* [Getting Started](https://github.com/youtube/vitess/blob/master/doc/GettingStarted.markdown)
* Operations
 * [Preparing for production](https://github.com/youtube/vitess/blob/master/doc/Production.markdown)
 * [Reparenting](https://github.com/youtube/vitess/blob/master/doc/Reparenting.markdown)
 * [Resharding](https://github.com/youtube/vitess/blob/master/doc/Resharding.markdown)
 * [Schema management](https://github.com/youtube/vitess/blob/master/doc/SchemaManagement.markdown)
 * [Zookeeper data](https://github.com/youtube/vitess/blob/master/doc/ZookeeperData.markdown)

### vttablet
TODO: Move this content to its own markdown.

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

## License

Unless otherwise noted, the vitess source files are distributed
under the BSD-style license found in the LICENSE file.
