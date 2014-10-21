# vtocc

vtocc is a smart proxy to mysql/mariadb. It's basically a subset of
vttablet that concerns itself with just serving queries for a
single database. It's a standalone program/server that can be
launched without the need of the rest of the vitess ecosystem.
It can be pointed to an existing database, and you should be able
to send queries through it.

## vtocc features
* Connection pooling.
* SQL parser: Although very close, the vtocc SQL parser is not SQL-92
  compliant. It has left out constructs that are deemed uncommon or
  OLTP-unfriendly. It should, however, allow most queries used by a
  typical web application.
* Query rewrite and sanitation (adding limits, avoiding non-deterministic updates).
* Query de-duping: reuse the results of an in-flight query to any
  subsequent requests that were received while the query was still
  executing.
* Rowcache: the mysql buffer cache is optimized for range scans over
  indices and tables. Unfortunately, it’s not good for random access
  by primary key. The rowcache will instead maintain a row based cache
  (using [memcached](http://memcached.org/) as its backend) and keep it
  consistent by fielding all DMLs that could potentially affect them.
* Query blacklisting: You can specify a set of rules to blacklist queries
  that are potentially problematic.
* Table ACLs: Allow you to specify ACLs for tables based on the connected
  user.
* Update stream: A server that streams the list of rows that are changing
  in the database, which can be used as a mechanism to continuously export
  the data to another data store.
* Query killer for queries that take too long to return data.
* Transaction management: Ability to limit the number of concurrent
  transactions and manage deadlines.
* Streaming queries to serve OLAP workloads.
* A rich set of monitoring features to watch over, diagnose or analyze performance.

## Protocol
vtocc uses the bsonrpc protocol. This means that it uses [bson encoding](http://bsonspec.org)
to receive and send messages. There is currently a [python client](https://github.com/youtube/vitess/blob/master/py/vtdb/tablet.py). A java client is
also getting implemented.

If you are familiar with go, you can actually plug in any protocol you desire, like json,
thrift or protobufs.

## Data types
vtocc has not been well tested with exotic data types. Specifically, we don't know how it
will handle boolean and timestamp columns. Otherwise, we have [tests](https://github.com/youtube/vitess/blob/master/test/test_data/test_schema.sql#L45) for
the commonly used data types.

vtocc can work with latin-1 or utf-8 encoded databases. It's highly recommended that you match
client and server side encoding. vtocc does not try to do any character set conversions.

vtocc will enable rowcache only for tables that have numbers or binary data types as primary
key columns. This is because other column types are not bitwise comparable. For example,
varchar comparison in MySQL is collation dependent. So, those types are not supported.

## Bind variables
One major differentiator with vtocc is its use of bind variables. When you send a query,
you build it like this:

    query = "select a, b, c from t where id1 = :id1 and id2 = :id2"
    bindVars = {"id1": 1, "id2": 2}

vtocc parses the query string and caches it against an execution plan. Subsequent requests
with the same query string will cause vtocc to reuse the cached query plan to efficiently
handle the request.

vtocc also accepts old-style positional variables. In such cases the bind vars are to be
named as v1, v2, etc:

    query = "select a, b, c from t where id1 = ? and id2 = ?"
    bindVars = {"v1": 1, "v2": 2}

vtocc also accepts list bind variables (TO BE IMPLEMENTED):

    query = "select a, b, c from t where id1 in ::list"
    bindVars = {"list": [1, 2, 3]}

Additionally, vtocc tracks statistics grouped by these query strings, which are
useful for analysis and trouble-shooting.

## Execute functions
There are three Execute functions:
* **Execute**: This is an OLTP execute function that is expected to return a limited set
  of rows. If the number of rows exceeds the max allowed limit, an error is returned.
* **BatchExecute** executes a set of OLTP statements as a single round-trip request. If you
  are affected by network latency, this function can be used to group your requests.
  You can call this function from within a transaction, or you can begin and commit a
  transaction from within a batch.
* **StreamExecute**: This function is used for returning large result sets that could
  potentially require full table scans.

## Command line arguments
'vtocc -h' should print the full set of command line arguments. Here is an explanation
of what they mean:
#### DB connection parameters
There are four types of db-config parameters. db-config-app-* specify the connection parameters
that will be used to serve the app. The 'repl' parameters will be used by the rowcache to connect
to the server as a replica to fetch binlog events for invalidation. The 'dba' and 'filtered'
parameters are only used when running as vttablet.
* **db-config-app-charset="utf8"**: Only utf8 or latin1 are currently supported.
* **db-config-app-dbname=""**: Name of the MySQL database to serve queries for.
* **db-config-app-keyspace=""**: It’s recommended that this value be set to the same as dbname. Clients connecting to vtocc will need to specify the keyspace name, which will be used as sanity check.
* **db-config-app-uname="vt_app"**: Set this to the username vtocc should connect as.
* **db-config-app-unixsocket=""**: Socket file name. This is the recommended mode of connection (vs host-port).
* **db-credentials-server="file"**: db credentials server type (use 'file' for the file implementation).
* **db-credentials-file**: Specifies the file where db credentials are stored.

TODO: Document the rest of the flags.

#### Query server parameters
All timeout related parameters below are specified in seconds. A value of zero means never.
* **port=0**: Server port.
* **queryserver-config-idle-timeout=1800**: vtocc has many connection pools to connect to mysql. If any connection in the pool has been idle for longer than the specified time, then vtocc discards the connection and creates a new one instead. This value should be less than the MySQL idle timeout.
* **queryserver-config-max-result-size=10000**: vtocc adds a limit clause to all unbounded queries. If the result returned exceeds this number, it returns an error instead.
* **queryserver-config-pool-size=16**: This is the generic read pool. This pool gets used if you issue read queries outside of transactions.
* **queryserver-config-query-cache-size=5000**: This is the number of unique query strings that vtocc caches. You can start off with the default value and adjust up or down based on what you see.
* **queryserver-config-query-timeout=0**: This timeout specifies how long a query is allowed to run before it’s killed.
* **queryserver-config-schema-reload-time=1800**: This specifies how often the schema is reloaded by vtocc. If you rollout schema changes to a live serving database, you can be sure that it has been read by vtocc after the reload time has elapsed. If needed, there are ways to make vtocc reload the schema immediately.
* **queryserver-config-stream-buffer-size=32768**: This is the buffer size for streaming queries.
* **queryserver-config-stream-pool-size=750**: Connection pool size for streaming queries.
* **queryserver-config-strict-mode=true**: If this is turned off, vtocc allows all DMLs and does not enforce MySQL's `STRICT_TRANS_TABLES`. This setting can be tuned off for migration purposes if the database is not already configured with these settings.
* **queryserver-config-transaction-cap=20**: This value limits the number of allowed concurrent transactions.
* **queryserver-config-transaction-timeout=30**: The amount of time to allow a transaction to complete before killing it.
