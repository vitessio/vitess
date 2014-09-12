# vttablet

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
