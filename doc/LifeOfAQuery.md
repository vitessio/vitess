Life of A Query
=====================

* [From Client to VtGate](#from-client-to-vtgate)
* [From VtGate to VtTablet](#from-vtgate-to-vttablet)
* [From VtTablet to MySQL](#from-vttablet-to-mysql)
* [Putting it all together](#putting-it-all-together)
* [TopoServer](#toposerver)
* [Streaming Query](#streaming-query)
* [Scatter Query](#scatter-query)

A query means a request for information from database and it involves four components in the case of Vitess, including the client application, VtGate, VtTablet and MySQL instance. This doc explains the interaction which happens between and within components.

![](https://raw.githubusercontent.com/youtube/vitess/master/doc/life_of_a_query.png)

At a very high level, as the graph shows, first the client sends a query to VtGate. VtGate then resolves the query and routes it to the right VtTablets. For each VtTablet that receives the query, it does necessary validations and passes the query to the underlying MySQL instance. After gathering results from MySQL, VtTablet sends the response back to VtGate. Once VtGate receives responses from all VtTablets, it sends the combined result to the client. In the presence of VtTablet errors, VtGate will retry the query if errors are recoverable and it only fails the query if either errors are unrecoverable or the maximum number of retries has been reached.

## From Client to VtGate

A client application first sends an rpc with an embedded sql query to VtGate. VtGate's rpc server unmarshals this rpc request, calls the appropriate VtGate method and return its result back to client.

![](https://raw.githubusercontent.com/youtube/vitess/master/doc/life_of_a_query_client_to_vtgate.png)

VtGate keeps an in-memory table that stores all available rpc methods for each service, e.g. VtGate uses "VTGate" as its service name and most of its methods defined in [go/vt/vtgate/vtgate.go](../go/vt/vtgate/vtgate.go) are used to serve rpc request.

## From VtGate to VtTablet

![](https://raw.githubusercontent.com/youtube/vitess/master/doc/life_of_a_query_vtgate_to_vttablet.png)

After receiving an rpc call from the client and one of its Execute* method being invoked, VtGate needs to figure out which shards should receive the query and send it to each of them. In addition, VtGate talks to the topo server to get necessary information to create a VtTablet connection for each shard. At this point, VtGate is able to send the query to the right VtTablets in parallel. VtGate also does retry if timeout happens or some VtTablets return recoverable errors.

Internally, VtGate has a ScatterConn instance and uses it to execute queries across multiple ShardConn connections. A ScatterConn performs the query on selected shards in parallel. It first obtains a ShardConn connection for every shard and sends the query using the ShardConn's execute method. If the requested session is in a transaction, it will open a new transaction on the connection, and update the Session with the transaction id. If the session already contains a transaction id for the shard, it reuses it. If there are any unrecoverable errors during a transaction, it rolls back the transaction for all shards.

A ShardConn object represents a load balanced connection to a group of VtTablets that belong to the same shard. ShardConn can be concurrently used across goroutines.

## From VtTablet to MySQL

![](https://raw.githubusercontent.com/youtube/vitess/master/doc/life_of_a_query_vttablet_to_mysql.png)

Once VtTablet received an rpc call from VtGate, it does a few checks before passing the query to MySQL. First, it validates the current VtTablet state including the session id, then generates a query plan and applies predefined query rules and does ACL checks. It also checks whether the query hits the row cache and returns the result immediately if so. In addition, VtTablet consolidates duplicate queries from executing simultaneously and shares results between them. At this point, VtTablet has no way but pass the query down to MySQL layer and wait for the result.

## Putting it all together

![](https://raw.githubusercontent.com/youtube/vitess/master/doc/life_of_a_query_all.png)

## TopoServer

A topo server stores information to help VtGate navigate the query to the right VtTablets. It contains keyspace to shards mappings, keyspace id to shards mappings and ports that a VtTablet listens to (EndPoint). VtGate caches this information in memory and periodically updates it if changes have happened in the topo server.

## Streaming Query

Generally speaking, a streaming query means query results will be returned as a stream. In Vitess' case, both VtGate and VtTablet will send results back as soon as it is available. VtTablet by default will collect a fixed number of rows returned from MySQL, send them back to VtGate and repeats the above step until all rows have been returned.

## Scatter Query

A scatter query, as its name indicates, will hit multiple shards. In Vitess, a scatter query is recognized once VtGate determines a query needs to hit multiple VtTablets. VtGate then sends the query to these VtTablets, assembles the result after receiving all responses and returns the combined result to the client.
