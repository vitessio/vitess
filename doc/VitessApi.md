This document describes Vitess API methods that enable your client application to more easily talk to your storage system to query data. API methods are grouped into the following categories:

* [Range-based Sharding](#range-based-sharding)
* [Transactions](#transactions)
* [Custom Sharding](#custom-sharding)
* [Map Reduce](#map-reduce)
* [Topology](#topology)
* [v3 API (alpha)](#v3-api-(alpha))


The following table lists the methods in each group and links to more detail about each method:

<table id="api-method-summary">
<tr><td class="api-method-summary-group" colspan="2">Range-based Sharding</td></tr>
<tr>
<td><code><a href="#executebatchkeyspaceids">ExecuteBatchKeyspaceIds</a></code></td>
<td>ExecuteBatchKeyspaceIds executes the list of queries based on the specified keyspace ids.</td>
</tr>
<tr>
<td><code><a href="#executeentityids">ExecuteEntityIds</a></code></td>
<td>ExecuteEntityIds executes the query based on the specified external id to keyspace id map.</td>
</tr>
<tr>
<td><code><a href="#executekeyranges">ExecuteKeyRanges</a></code></td>
<td>ExecuteKeyRanges executes the query based on the specified key ranges.</td>
</tr>
<tr>
<td><code><a href="#executekeyspaceids">ExecuteKeyspaceIds</a></code></td>
<td>ExecuteKeyspaceIds executes the query based on the specified keyspace ids.</td>
</tr>
<tr>
<td><code><a href="#streamexecutekeyranges">StreamExecuteKeyRanges</a></code></td>
<td>StreamExecuteKeyRanges executes a streaming query based on key ranges. Use this method if the query returns a large number of rows.</td>
</tr>
<tr>
<td><code><a href="#streamexecutekeyspaceids">StreamExecuteKeyspaceIds</a></code></td>
<td>StreamExecuteKeyspaceIds executes a streaming query based on keyspace ids. Use this method if the query returns a large number of rows.</td>
</tr>
<tr><td class="api-method-summary-group" colspan="2">Transactions</td></tr>
<tr>
<td><code><a href="#begin">Begin</a></code></td>
<td>Begin a transaction.</td>
</tr>
<tr>
<td><code><a href="#commit">Commit</a></code></td>
<td>Commit a transaction.</td>
</tr>
<tr>
<td><code><a href="#rollback">Rollback</a></code></td>
<td>Rollback a transaction.</td>
</tr>
<tr><td class="api-method-summary-group" colspan="2">Custom Sharding</td></tr>
<tr>
<td><code><a href="#executebatchshards">ExecuteBatchShards</a></code></td>
<td>ExecuteBatchShards executes the list of queries on the specified shards.</td>
</tr>
<tr>
<td><code><a href="#executeshards">ExecuteShards</a></code></td>
<td>ExecuteShards executes the query on the specified shards.</td>
</tr>
<tr>
<td><code><a href="#streamexecuteshards">StreamExecuteShards</a></code></td>
<td>StreamExecuteShards executes a streaming query based on shards. Use this method if the query returns a large number of rows.</td>
</tr>
<tr><td class="api-method-summary-group" colspan="2">Map Reduce</td></tr>
<tr>
<td><code><a href="#splitquery">SplitQuery</a></code></td>
<td>Split a query into non-overlapping sub queries</td>
</tr>
<tr><td class="api-method-summary-group" colspan="2">Topology</td></tr>
<tr>
<td><code><a href="#getsrvkeyspace">GetSrvKeyspace</a></code></td>
<td>GetSrvKeyspace returns a SrvKeyspace object (as seen by this vtgate). This method is provided as a convenient way for clients to take a look at the sharding configuration for a Keyspace. Looking at the sharding information should not be used for routing queries (as the information may change, use the Execute calls for that). It is convenient for monitoring applications for instance, or if using custom sharding.</td>
</tr>
<tr><td class="api-method-summary-group" colspan="2">v3 API (alpha)</td></tr>
<tr>
<td><code><a href="#execute">Execute</a></code></td>
<td>Execute tries to route the query to the right shard. It depends on the query and bind variables to provide enough information in conjonction with the vindexes to route the query.</td>
</tr>
<tr>
<td><code><a href="#streamexecute">StreamExecute</a></code></td>
<td>StreamExecute executes a streaming query based on shards. It depends on the query and bind variables to provide enough information in conjonction with the vindexes to route the query. Use this method if the query returns a large number of rows.</td>
</tr>
</table>
##Range-based Sharding
### ExecuteBatchKeyspaceIds

ExecuteBatchKeyspaceIds executes the list of queries based on the specified keyspace ids.

#### Request

 ExecuteBatchKeyspaceIdsRequest is the payload to ExecuteBatchKeyspaceId

##### Parameters

| Name |Description |
| :-------- | :-------- 
| <code>caller_id</code> <br>[vtrpc.CallerID](#vtrpc.callerid)| CallerID is passed along RPCs to identify the originating client for a request. It is not meant to be secure, but only informational.  The client can put whatever info they want in these fields, and they will be trusted by the servers. The fields will just be used for logging purposes, and to easily find a client. VtGate propagates it to VtTablet, and VtTablet may use this information for monitoring purposes, to display on dashboards, or for blacklisting purposes. |
| <code>session</code> <br>[Session](#session)| Session objects are session cookies and are invalidated on use. Query results will contain updated session values. Their content should be opaque to the user. |
| <code>queries</code> <br>list &lt;[BoundKeyspaceIdQuery](#boundkeyspaceidquery)&gt;| BoundKeyspaceIdQuery represents a single query request for the specified list of keyspace ids. This is used in a list for ExecuteBatchKeyspaceIdsRequest. |
| <code>tablet_type</code> <br>[topodata.TabletType](#topodata.tablettype)| TabletType represents the type of a given tablet. |
| <code>as_transaction</code> <br>bool| |

#### Response

 ExecuteBatchKeyspaceIdsResponse is the returned value from ExecuteBatchKeyspaceId

##### Properties

| Name |Description |
| :-------- | :-------- 
| <code>error</code> <br>[vtrpc.RPCError](#vtrpc.rpcerror)| RPCError is an application-level error structure returned by VtTablet (and passed along by VtGate if appropriate). We use this so the clients don't have to parse the error messages, but instead can depend on the value of the code. |
| <code>session</code> <br>[Session](#session)| Session objects are session cookies and are invalidated on use. Query results will contain updated session values. Their content should be opaque to the user. |
| <code>results</code> <br>list &lt;[query.QueryResult](#query.queryresult)&gt;| QueryResult is returned by Execute and ExecuteStream.  As returned by Execute, len(fields) is always equal to len(row) (for each row in rows).  As returned by StreamExecute, the first QueryResult has the fields set, and subsequent QueryResult have rows set. And as Execute, len(QueryResult[0].fields) is always equal to len(row) (for each row in rows for each QueryResult in QueryResult[1:]). |

### ExecuteEntityIds

ExecuteEntityIds executes the query based on the specified external id to keyspace id map.

#### Request

 ExecuteEntityIdsRequest is the payload to ExecuteEntityIds

##### Parameters

| Name |Description |
| :-------- | :-------- 
| <code>caller_id</code> <br>[vtrpc.CallerID](#vtrpc.callerid)| CallerID is passed along RPCs to identify the originating client for a request. It is not meant to be secure, but only informational.  The client can put whatever info they want in these fields, and they will be trusted by the servers. The fields will just be used for logging purposes, and to easily find a client. VtGate propagates it to VtTablet, and VtTablet may use this information for monitoring purposes, to display on dashboards, or for blacklisting purposes. |
| <code>session</code> <br>[Session](#session)| Session objects are session cookies and are invalidated on use. Query results will contain updated session values. Their content should be opaque to the user. |
| <code>query</code> <br>[query.BoundQuery](#query.boundquery)| BoundQuery is a query with its bind variables |
| <code>keyspace</code> <br>string| |
| <code>entity_column_name</code> <br>string| |
| <code>entity_keyspace_ids</code> <br>list &lt;[EntityId](#executeentityidsrequest.entityid)&gt;| |
| <code>tablet_type</code> <br>[topodata.TabletType](#topodata.tablettype)| TabletType represents the type of a given tablet. |
| <code>not_in_transaction</code> <br>bool| |

#### Messages

##### ExecuteEntityIdsRequest.EntityId

<em>Properties</em>

| Name |Description |
| :-------- | :-------- 
| <code>xid_type</code> <br>[Type](#executeentityidsrequest.entityid.type)| |
| <code>xid_bytes</code> <br>bytes| |
| <code>xid_int</code> <br>int64| |
| <code>xid_uint</code> <br>uint64| |
| <code>xid_float</code> <br>double| |
| <code>keyspace_id</code> <br>bytes| |

#### Enums

##### ExecuteEntityIdsRequest.EntityId.Type

| Name |Value |Description |
| :-------- | :-------- | :-------- 
| <code>TYPE_NULL</code> | <code>0</code> |  |
| <code>TYPE_BYTES</code> | <code>1</code> |  |
| <code>TYPE_INT</code> | <code>2</code> |  |
| <code>TYPE_UINT</code> | <code>3</code> |  |
| <code>TYPE_FLOAT</code> | <code>4</code> |  |

#### Response

 ExecuteEntityIdsResponse is the returned value from ExecuteEntityIds

##### Properties

| Name |Description |
| :-------- | :-------- 
| <code>error</code> <br>[vtrpc.RPCError](#vtrpc.rpcerror)| RPCError is an application-level error structure returned by VtTablet (and passed along by VtGate if appropriate). We use this so the clients don't have to parse the error messages, but instead can depend on the value of the code. |
| <code>session</code> <br>[Session](#session)| Session objects are session cookies and are invalidated on use. Query results will contain updated session values. Their content should be opaque to the user. |
| <code>result</code> <br>[query.QueryResult](#query.queryresult)| QueryResult is returned by Execute and ExecuteStream.  As returned by Execute, len(fields) is always equal to len(row) (for each row in rows).  As returned by StreamExecute, the first QueryResult has the fields set, and subsequent QueryResult have rows set. And as Execute, len(QueryResult[0].fields) is always equal to len(row) (for each row in rows for each QueryResult in QueryResult[1:]). |

### ExecuteKeyRanges

ExecuteKeyRanges executes the query based on the specified key ranges.

#### Request

 ExecuteKeyRangesRequest is the payload to ExecuteKeyRanges

##### Parameters

| Name |Description |
| :-------- | :-------- 
| <code>caller_id</code> <br>[vtrpc.CallerID](#vtrpc.callerid)| CallerID is passed along RPCs to identify the originating client for a request. It is not meant to be secure, but only informational.  The client can put whatever info they want in these fields, and they will be trusted by the servers. The fields will just be used for logging purposes, and to easily find a client. VtGate propagates it to VtTablet, and VtTablet may use this information for monitoring purposes, to display on dashboards, or for blacklisting purposes. |
| <code>session</code> <br>[Session](#session)| Session objects are session cookies and are invalidated on use. Query results will contain updated session values. Their content should be opaque to the user. |
| <code>query</code> <br>[query.BoundQuery](#query.boundquery)| BoundQuery is a query with its bind variables |
| <code>keyspace</code> <br>string| |
| <code>key_ranges</code> <br>list &lt;[topodata.KeyRange](#topodata.keyrange)&gt;| KeyRange describes a range of sharding keys, when range-based sharding is used. |
| <code>tablet_type</code> <br>[topodata.TabletType](#topodata.tablettype)| TabletType represents the type of a given tablet. |
| <code>not_in_transaction</code> <br>bool| |

#### Response

 ExecuteKeyRangesResponse is the returned value from ExecuteKeyRanges

##### Properties

| Name |Description |
| :-------- | :-------- 
| <code>error</code> <br>[vtrpc.RPCError](#vtrpc.rpcerror)| RPCError is an application-level error structure returned by VtTablet (and passed along by VtGate if appropriate). We use this so the clients don't have to parse the error messages, but instead can depend on the value of the code. |
| <code>session</code> <br>[Session](#session)| Session objects are session cookies and are invalidated on use. Query results will contain updated session values. Their content should be opaque to the user. |
| <code>result</code> <br>[query.QueryResult](#query.queryresult)| QueryResult is returned by Execute and ExecuteStream.  As returned by Execute, len(fields) is always equal to len(row) (for each row in rows).  As returned by StreamExecute, the first QueryResult has the fields set, and subsequent QueryResult have rows set. And as Execute, len(QueryResult[0].fields) is always equal to len(row) (for each row in rows for each QueryResult in QueryResult[1:]). |

### ExecuteKeyspaceIds

ExecuteKeyspaceIds executes the query based on the specified keyspace ids.

#### Request

 ExecuteKeyspaceIdsRequest is the payload to ExecuteKeyspaceIds

##### Parameters

| Name |Description |
| :-------- | :-------- 
| <code>caller_id</code> <br>[vtrpc.CallerID](#vtrpc.callerid)| CallerID is passed along RPCs to identify the originating client for a request. It is not meant to be secure, but only informational.  The client can put whatever info they want in these fields, and they will be trusted by the servers. The fields will just be used for logging purposes, and to easily find a client. VtGate propagates it to VtTablet, and VtTablet may use this information for monitoring purposes, to display on dashboards, or for blacklisting purposes. |
| <code>session</code> <br>[Session](#session)| Session objects are session cookies and are invalidated on use. Query results will contain updated session values. Their content should be opaque to the user. |
| <code>query</code> <br>[query.BoundQuery](#query.boundquery)| BoundQuery is a query with its bind variables |
| <code>keyspace</code> <br>string| |
| <code>keyspace_ids</code> <br>list &lt;bytes&gt;| |
| <code>tablet_type</code> <br>[topodata.TabletType](#topodata.tablettype)| TabletType represents the type of a given tablet. |
| <code>not_in_transaction</code> <br>bool| |

#### Response

 ExecuteKeyspaceIdsResponse is the returned value from ExecuteKeyspaceIds

##### Properties

| Name |Description |
| :-------- | :-------- 
| <code>error</code> <br>[vtrpc.RPCError](#vtrpc.rpcerror)| RPCError is an application-level error structure returned by VtTablet (and passed along by VtGate if appropriate). We use this so the clients don't have to parse the error messages, but instead can depend on the value of the code. |
| <code>session</code> <br>[Session](#session)| Session objects are session cookies and are invalidated on use. Query results will contain updated session values. Their content should be opaque to the user. |
| <code>result</code> <br>[query.QueryResult](#query.queryresult)| QueryResult is returned by Execute and ExecuteStream.  As returned by Execute, len(fields) is always equal to len(row) (for each row in rows).  As returned by StreamExecute, the first QueryResult has the fields set, and subsequent QueryResult have rows set. And as Execute, len(QueryResult[0].fields) is always equal to len(row) (for each row in rows for each QueryResult in QueryResult[1:]). |

### StreamExecuteKeyRanges

StreamExecuteKeyRanges executes a streaming query based on key ranges. Use this method if the query returns a large number of rows.

#### Request

 StreamExecuteKeyRangesRequest is the payload to StreamExecuteKeyRanges

##### Parameters

| Name |Description |
| :-------- | :-------- 
| <code>caller_id</code> <br>[vtrpc.CallerID](#vtrpc.callerid)| CallerID is passed along RPCs to identify the originating client for a request. It is not meant to be secure, but only informational.  The client can put whatever info they want in these fields, and they will be trusted by the servers. The fields will just be used for logging purposes, and to easily find a client. VtGate propagates it to VtTablet, and VtTablet may use this information for monitoring purposes, to display on dashboards, or for blacklisting purposes. |
| <code>query</code> <br>[query.BoundQuery](#query.boundquery)| BoundQuery is a query with its bind variables |
| <code>keyspace</code> <br>string| |
| <code>key_ranges</code> <br>list &lt;[topodata.KeyRange](#topodata.keyrange)&gt;| KeyRange describes a range of sharding keys, when range-based sharding is used. |
| <code>tablet_type</code> <br>[topodata.TabletType](#topodata.tablettype)| TabletType represents the type of a given tablet. |

#### Response

 StreamExecuteKeyRangesResponse is the returned value from StreamExecuteKeyRanges

##### Properties

| Name |Description |
| :-------- | :-------- 
| <code>error</code> <br>[vtrpc.RPCError](#vtrpc.rpcerror)| RPCError is an application-level error structure returned by VtTablet (and passed along by VtGate if appropriate). We use this so the clients don't have to parse the error messages, but instead can depend on the value of the code. |
| <code>result</code> <br>[query.QueryResult](#query.queryresult)| QueryResult is returned by Execute and ExecuteStream.  As returned by Execute, len(fields) is always equal to len(row) (for each row in rows).  As returned by StreamExecute, the first QueryResult has the fields set, and subsequent QueryResult have rows set. And as Execute, len(QueryResult[0].fields) is always equal to len(row) (for each row in rows for each QueryResult in QueryResult[1:]). |

### StreamExecuteKeyspaceIds

StreamExecuteKeyspaceIds executes a streaming query based on keyspace ids. Use this method if the query returns a large number of rows.

#### Request

 StreamExecuteKeyspaceIdsRequest is the payload to StreamExecuteKeyspaceIds

##### Parameters

| Name |Description |
| :-------- | :-------- 
| <code>caller_id</code> <br>[vtrpc.CallerID](#vtrpc.callerid)| CallerID is passed along RPCs to identify the originating client for a request. It is not meant to be secure, but only informational.  The client can put whatever info they want in these fields, and they will be trusted by the servers. The fields will just be used for logging purposes, and to easily find a client. VtGate propagates it to VtTablet, and VtTablet may use this information for monitoring purposes, to display on dashboards, or for blacklisting purposes. |
| <code>query</code> <br>[query.BoundQuery](#query.boundquery)| BoundQuery is a query with its bind variables |
| <code>keyspace</code> <br>string| |
| <code>keyspace_ids</code> <br>list &lt;bytes&gt;| |
| <code>tablet_type</code> <br>[topodata.TabletType](#topodata.tablettype)| TabletType represents the type of a given tablet. |

#### Response

 StreamExecuteKeyspaceIdsResponse is the returned value from StreamExecuteKeyspaceIds

##### Properties

| Name |Description |
| :-------- | :-------- 
| <code>error</code> <br>[vtrpc.RPCError](#vtrpc.rpcerror)| RPCError is an application-level error structure returned by VtTablet (and passed along by VtGate if appropriate). We use this so the clients don't have to parse the error messages, but instead can depend on the value of the code. |
| <code>result</code> <br>[query.QueryResult](#query.queryresult)| QueryResult is returned by Execute and ExecuteStream.  As returned by Execute, len(fields) is always equal to len(row) (for each row in rows).  As returned by StreamExecute, the first QueryResult has the fields set, and subsequent QueryResult have rows set. And as Execute, len(QueryResult[0].fields) is always equal to len(row) (for each row in rows for each QueryResult in QueryResult[1:]). |

##Transactions
### Begin

Begin a transaction.

#### Request

 BeginRequest is the payload to Begin

##### Parameters

| Name |Description |
| :-------- | :-------- 
| <code>caller_id</code> <br>[vtrpc.CallerID](#vtrpc.callerid)| CallerID is passed along RPCs to identify the originating client for a request. It is not meant to be secure, but only informational.  The client can put whatever info they want in these fields, and they will be trusted by the servers. The fields will just be used for logging purposes, and to easily find a client. VtGate propagates it to VtTablet, and VtTablet may use this information for monitoring purposes, to display on dashboards, or for blacklisting purposes. |

#### Response

 BeginResponse is the returned value from Begin

##### Properties

| Name |Description |
| :-------- | :-------- 
| <code>error</code> <br>[vtrpc.RPCError](#vtrpc.rpcerror)| RPCError is an application-level error structure returned by VtTablet (and passed along by VtGate if appropriate). We use this so the clients don't have to parse the error messages, but instead can depend on the value of the code. |
| <code>session</code> <br>[Session](#session)| Session objects are session cookies and are invalidated on use. Query results will contain updated session values. Their content should be opaque to the user. |

### Commit

Commit a transaction.

#### Request

 CommitRequest is the payload to Commit

##### Parameters

| Name |Description |
| :-------- | :-------- 
| <code>caller_id</code> <br>[vtrpc.CallerID](#vtrpc.callerid)| CallerID is passed along RPCs to identify the originating client for a request. It is not meant to be secure, but only informational.  The client can put whatever info they want in these fields, and they will be trusted by the servers. The fields will just be used for logging purposes, and to easily find a client. VtGate propagates it to VtTablet, and VtTablet may use this information for monitoring purposes, to display on dashboards, or for blacklisting purposes. |
| <code>session</code> <br>[Session](#session)| Session objects are session cookies and are invalidated on use. Query results will contain updated session values. Their content should be opaque to the user. |

#### Response

 CommitResponse is the returned value from Commit

##### Properties

| Name |Description |
| :-------- | :-------- 
| <code>error</code> <br>[vtrpc.RPCError](#vtrpc.rpcerror)| RPCError is an application-level error structure returned by VtTablet (and passed along by VtGate if appropriate). We use this so the clients don't have to parse the error messages, but instead can depend on the value of the code. |

### Rollback

Rollback a transaction.

#### Request

 RollbackRequest is the payload to Rollback

##### Parameters

| Name |Description |
| :-------- | :-------- 
| <code>caller_id</code> <br>[vtrpc.CallerID](#vtrpc.callerid)| CallerID is passed along RPCs to identify the originating client for a request. It is not meant to be secure, but only informational.  The client can put whatever info they want in these fields, and they will be trusted by the servers. The fields will just be used for logging purposes, and to easily find a client. VtGate propagates it to VtTablet, and VtTablet may use this information for monitoring purposes, to display on dashboards, or for blacklisting purposes. |
| <code>session</code> <br>[Session](#session)| Session objects are session cookies and are invalidated on use. Query results will contain updated session values. Their content should be opaque to the user. |

#### Response

 RollbackResponse is the returned value from Rollback

##### Properties

| Name |Description |
| :-------- | :-------- 
| <code>error</code> <br>[vtrpc.RPCError](#vtrpc.rpcerror)| RPCError is an application-level error structure returned by VtTablet (and passed along by VtGate if appropriate). We use this so the clients don't have to parse the error messages, but instead can depend on the value of the code. |

##Custom Sharding
### ExecuteBatchShards

ExecuteBatchShards executes the list of queries on the specified shards.

#### Request

 ExecuteBatchShardsRequest is the payload to ExecuteBatchShards

##### Parameters

| Name |Description |
| :-------- | :-------- 
| <code>caller_id</code> <br>[vtrpc.CallerID](#vtrpc.callerid)| CallerID is passed along RPCs to identify the originating client for a request. It is not meant to be secure, but only informational.  The client can put whatever info they want in these fields, and they will be trusted by the servers. The fields will just be used for logging purposes, and to easily find a client. VtGate propagates it to VtTablet, and VtTablet may use this information for monitoring purposes, to display on dashboards, or for blacklisting purposes. |
| <code>session</code> <br>[Session](#session)| Session objects are session cookies and are invalidated on use. Query results will contain updated session values. Their content should be opaque to the user. |
| <code>queries</code> <br>list &lt;[BoundShardQuery](#boundshardquery)&gt;| BoundShardQuery represents a single query request for the specified list of shards. This is used in a list for ExecuteBatchShardsRequest. |
| <code>tablet_type</code> <br>[topodata.TabletType](#topodata.tablettype)| TabletType represents the type of a given tablet. |
| <code>as_transaction</code> <br>bool| |

#### Response

 ExecuteBatchShardsResponse is the returned value from ExecuteBatchShards

##### Properties

| Name |Description |
| :-------- | :-------- 
| <code>error</code> <br>[vtrpc.RPCError](#vtrpc.rpcerror)| RPCError is an application-level error structure returned by VtTablet (and passed along by VtGate if appropriate). We use this so the clients don't have to parse the error messages, but instead can depend on the value of the code. |
| <code>session</code> <br>[Session](#session)| Session objects are session cookies and are invalidated on use. Query results will contain updated session values. Their content should be opaque to the user. |
| <code>results</code> <br>list &lt;[query.QueryResult](#query.queryresult)&gt;| QueryResult is returned by Execute and ExecuteStream.  As returned by Execute, len(fields) is always equal to len(row) (for each row in rows).  As returned by StreamExecute, the first QueryResult has the fields set, and subsequent QueryResult have rows set. And as Execute, len(QueryResult[0].fields) is always equal to len(row) (for each row in rows for each QueryResult in QueryResult[1:]). |

### ExecuteShards

ExecuteShards executes the query on the specified shards.

#### Request

 ExecuteShardsRequest is the payload to ExecuteShards

##### Parameters

| Name |Description |
| :-------- | :-------- 
| <code>caller_id</code> <br>[vtrpc.CallerID](#vtrpc.callerid)| CallerID is passed along RPCs to identify the originating client for a request. It is not meant to be secure, but only informational.  The client can put whatever info they want in these fields, and they will be trusted by the servers. The fields will just be used for logging purposes, and to easily find a client. VtGate propagates it to VtTablet, and VtTablet may use this information for monitoring purposes, to display on dashboards, or for blacklisting purposes. |
| <code>session</code> <br>[Session](#session)| Session objects are session cookies and are invalidated on use. Query results will contain updated session values. Their content should be opaque to the user. |
| <code>query</code> <br>[query.BoundQuery](#query.boundquery)| BoundQuery is a query with its bind variables |
| <code>keyspace</code> <br>string| |
| <code>shards</code> <br>list &lt;string&gt;| |
| <code>tablet_type</code> <br>[topodata.TabletType](#topodata.tablettype)| TabletType represents the type of a given tablet. |
| <code>not_in_transaction</code> <br>bool| |

#### Response

 ExecuteShardsResponse is the returned value from ExecuteShards

##### Properties

| Name |Description |
| :-------- | :-------- 
| <code>error</code> <br>[vtrpc.RPCError](#vtrpc.rpcerror)| RPCError is an application-level error structure returned by VtTablet (and passed along by VtGate if appropriate). We use this so the clients don't have to parse the error messages, but instead can depend on the value of the code. |
| <code>session</code> <br>[Session](#session)| Session objects are session cookies and are invalidated on use. Query results will contain updated session values. Their content should be opaque to the user. |
| <code>result</code> <br>[query.QueryResult](#query.queryresult)| QueryResult is returned by Execute and ExecuteStream.  As returned by Execute, len(fields) is always equal to len(row) (for each row in rows).  As returned by StreamExecute, the first QueryResult has the fields set, and subsequent QueryResult have rows set. And as Execute, len(QueryResult[0].fields) is always equal to len(row) (for each row in rows for each QueryResult in QueryResult[1:]). |

### StreamExecuteShards

StreamExecuteShards executes a streaming query based on shards. Use this method if the query returns a large number of rows.

#### Request

 StreamExecuteShardsRequest is the payload to StreamExecuteShards

##### Parameters

| Name |Description |
| :-------- | :-------- 
| <code>caller_id</code> <br>[vtrpc.CallerID](#vtrpc.callerid)| CallerID is passed along RPCs to identify the originating client for a request. It is not meant to be secure, but only informational.  The client can put whatever info they want in these fields, and they will be trusted by the servers. The fields will just be used for logging purposes, and to easily find a client. VtGate propagates it to VtTablet, and VtTablet may use this information for monitoring purposes, to display on dashboards, or for blacklisting purposes. |
| <code>query</code> <br>[query.BoundQuery](#query.boundquery)| BoundQuery is a query with its bind variables |
| <code>keyspace</code> <br>string| |
| <code>shards</code> <br>list &lt;string&gt;| |
| <code>tablet_type</code> <br>[topodata.TabletType](#topodata.tablettype)| TabletType represents the type of a given tablet. |

#### Response

 StreamExecuteShardsResponse is the returned value from StreamExecuteShards

##### Properties

| Name |Description |
| :-------- | :-------- 
| <code>error</code> <br>[vtrpc.RPCError](#vtrpc.rpcerror)| RPCError is an application-level error structure returned by VtTablet (and passed along by VtGate if appropriate). We use this so the clients don't have to parse the error messages, but instead can depend on the value of the code. |
| <code>result</code> <br>[query.QueryResult](#query.queryresult)| QueryResult is returned by Execute and ExecuteStream.  As returned by Execute, len(fields) is always equal to len(row) (for each row in rows).  As returned by StreamExecute, the first QueryResult has the fields set, and subsequent QueryResult have rows set. And as Execute, len(QueryResult[0].fields) is always equal to len(row) (for each row in rows for each QueryResult in QueryResult[1:]). |

##Map Reduce
### SplitQuery

Split a query into non-overlapping sub queries

#### Request

 SplitQueryRequest is the payload to SplitQuery

##### Parameters

| Name |Description |
| :-------- | :-------- 
| <code>caller_id</code> <br>[vtrpc.CallerID](#vtrpc.callerid)| CallerID is passed along RPCs to identify the originating client for a request. It is not meant to be secure, but only informational.  The client can put whatever info they want in these fields, and they will be trusted by the servers. The fields will just be used for logging purposes, and to easily find a client. VtGate propagates it to VtTablet, and VtTablet may use this information for monitoring purposes, to display on dashboards, or for blacklisting purposes. |
| <code>keyspace</code> <br>string| |
| <code>query</code> <br>[query.BoundQuery](#query.boundquery)| BoundQuery is a query with its bind variables |
| <code>split_column</code> <br>string| |
| <code>split_count</code> <br>int64| |

#### Response

 SplitQueryResponse is the returned value from SplitQuery

##### Properties

| Name |Description |
| :-------- | :-------- 
| <code>splits</code> <br>list &lt;[Part](#splitqueryresponse.part)&gt;| |

#### Messages

##### SplitQueryResponse.KeyRangePart

<em>Properties</em>

| Name |Description |
| :-------- | :-------- 
| <code>keyspace</code> <br>string| |
| <code>key_ranges</code> <br>list &lt;[topodata.KeyRange](#topodata.keyrange)&gt;| KeyRange describes a range of sharding keys, when range-based sharding is used. |

##### SplitQueryResponse.Part

<em>Properties</em>

| Name |Description |
| :-------- | :-------- 
| <code>query</code> <br>[query.BoundQuery](#query.boundquery)| BoundQuery is a query with its bind variables |
| <code>key_range_part</code> <br>[KeyRangePart](#splitqueryresponse.keyrangepart)| |
| <code>shard_part</code> <br>[ShardPart](#splitqueryresponse.shardpart)| |
| <code>size</code> <br>int64| |

##### SplitQueryResponse.ShardPart

<em>Properties</em>

| Name |Description |
| :-------- | :-------- 
| <code>keyspace</code> <br>string| |
| <code>shards</code> <br>list &lt;string&gt;| |

##Topology
### GetSrvKeyspace

GetSrvKeyspace returns a SrvKeyspace object (as seen by this vtgate). This method is provided as a convenient way for clients to take a look at the sharding configuration for a Keyspace. Looking at the sharding information should not be used for routing queries (as the information may change, use the Execute calls for that). It is convenient for monitoring applications for instance, or if using custom sharding.

#### Request

 GetSrvKeyspaceRequest is the payload to GetSrvKeyspace

##### Parameters

| Name |Description |
| :-------- | :-------- 
| <code>keyspace</code> <br>string| |

#### Response

 GetSrvKeyspaceResponse is the returned value from GetSrvKeyspace

##### Properties

| Name |Description |
| :-------- | :-------- 
| <code>srv_keyspace</code> <br>[topodata.SrvKeyspace](#topodata.srvkeyspace)| SrvKeyspace is a rollup node for the keyspace itself. |

##v3 API (alpha)
### Execute

Execute tries to route the query to the right shard. It depends on the query and bind variables to provide enough information in conjonction with the vindexes to route the query.

#### Request

 ExecuteRequest is the payload to Execute

##### Parameters

| Name |Description |
| :-------- | :-------- 
| <code>caller_id</code> <br>[vtrpc.CallerID](#vtrpc.callerid)| CallerID is passed along RPCs to identify the originating client for a request. It is not meant to be secure, but only informational.  The client can put whatever info they want in these fields, and they will be trusted by the servers. The fields will just be used for logging purposes, and to easily find a client. VtGate propagates it to VtTablet, and VtTablet may use this information for monitoring purposes, to display on dashboards, or for blacklisting purposes. |
| <code>session</code> <br>[Session](#session)| Session objects are session cookies and are invalidated on use. Query results will contain updated session values. Their content should be opaque to the user. |
| <code>query</code> <br>[query.BoundQuery](#query.boundquery)| BoundQuery is a query with its bind variables |
| <code>tablet_type</code> <br>[topodata.TabletType](#topodata.tablettype)| TabletType represents the type of a given tablet. |
| <code>not_in_transaction</code> <br>bool| |

#### Response

 ExecuteResponse is the returned value from Execute

##### Properties

| Name |Description |
| :-------- | :-------- 
| <code>error</code> <br>[vtrpc.RPCError](#vtrpc.rpcerror)| RPCError is an application-level error structure returned by VtTablet (and passed along by VtGate if appropriate). We use this so the clients don't have to parse the error messages, but instead can depend on the value of the code. |
| <code>session</code> <br>[Session](#session)| Session objects are session cookies and are invalidated on use. Query results will contain updated session values. Their content should be opaque to the user. |
| <code>result</code> <br>[query.QueryResult](#query.queryresult)| QueryResult is returned by Execute and ExecuteStream.  As returned by Execute, len(fields) is always equal to len(row) (for each row in rows).  As returned by StreamExecute, the first QueryResult has the fields set, and subsequent QueryResult have rows set. And as Execute, len(QueryResult[0].fields) is always equal to len(row) (for each row in rows for each QueryResult in QueryResult[1:]). |

### StreamExecute

StreamExecute executes a streaming query based on shards. It depends on the query and bind variables to provide enough information in conjonction with the vindexes to route the query. Use this method if the query returns a large number of rows.

#### Request

 StreamExecuteRequest is the payload to StreamExecute

##### Parameters

| Name |Description |
| :-------- | :-------- 
| <code>caller_id</code> <br>[vtrpc.CallerID](#vtrpc.callerid)| CallerID is passed along RPCs to identify the originating client for a request. It is not meant to be secure, but only informational.  The client can put whatever info they want in these fields, and they will be trusted by the servers. The fields will just be used for logging purposes, and to easily find a client. VtGate propagates it to VtTablet, and VtTablet may use this information for monitoring purposes, to display on dashboards, or for blacklisting purposes. |
| <code>query</code> <br>[query.BoundQuery](#query.boundquery)| BoundQuery is a query with its bind variables |
| <code>tablet_type</code> <br>[topodata.TabletType](#topodata.tablettype)| TabletType represents the type of a given tablet. |

#### Response

 StreamExecuteResponse is the returned value from StreamExecute

##### Properties

| Name |Description |
| :-------- | :-------- 
| <code>error</code> <br>[vtrpc.RPCError](#vtrpc.rpcerror)| RPCError is an application-level error structure returned by VtTablet (and passed along by VtGate if appropriate). We use this so the clients don't have to parse the error messages, but instead can depend on the value of the code. |
| <code>result</code> <br>[query.QueryResult](#query.queryresult)| QueryResult is returned by Execute and ExecuteStream.  As returned by Execute, len(fields) is always equal to len(row) (for each row in rows).  As returned by StreamExecute, the first QueryResult has the fields set, and subsequent QueryResult have rows set. And as Execute, len(QueryResult[0].fields) is always equal to len(row) (for each row in rows for each QueryResult in QueryResult[1:]). |

## Enums

### topodata.KeyspaceIdType

 KeyspaceIdType describes the type of the sharding key for a range-based sharded keyspace.

| Name |Value |Description |
| :-------- | :-------- | :-------- 
| <code>UNSET</code> | <code>0</code> | UNSET is the default value, when range-based sharding is not used.  |
| <code>UINT64</code> | <code>1</code> | UINT64 is when uint64 value is used. This is represented as 'unsigned bigint' in mysql  |
| <code>BYTES</code> | <code>2</code> | BYTES is when an array of bytes is used. This is represented as 'varbinary' in mysql  |

### topodata.TabletType

 TabletType represents the type of a given tablet.

| Name |Value |Description |
| :-------- | :-------- | :-------- 
| <code>UNKNOWN</code> | <code>0</code> |  |
| <code>IDLE</code> | <code>1</code> |  |
| <code>MASTER</code> | <code>2</code> |  |
| <code>REPLICA</code> | <code>3</code> |  |
| <code>RDONLY</code> | <code>4</code> |  |
| <code>BATCH</code> | <code>4</code> |  |
| <code>SPARE</code> | <code>5</code> |  |
| <code>EXPERIMENTAL</code> | <code>6</code> |  |
| <code>SCHEMA_UPGRADE</code> | <code>7</code> |  |
| <code>BACKUP</code> | <code>8</code> |  |
| <code>RESTORE</code> | <code>9</code> |  |
| <code>WORKER</code> | <code>10</code> |  |
| <code>SCRAP</code> | <code>11</code> |  |

### vtrpc.ErrorCode

 ErrorCode is the enum values for Errors. Internally, errors should be created with one of these codes. These will then be translated over the wire by various RPC frameworks.

| Name |Value |Description |
| :-------- | :-------- | :-------- 
| <code>SUCCESS</code> | <code>0</code> | SUCCESS is returned from a successful call  |
| <code>CANCELLED</code> | <code>1</code> | CANCELLED means that the context was cancelled (and noticed in the app layer, as opposed to the RPC layer)  |
| <code>UNKNOWN_ERROR</code> | <code>2</code> | UNKNOWN_ERROR includes: 1. MySQL error codes that we don't explicitly handle. 2. MySQL response that wasn't as expected. For example, we might expect a MySQL timestamp to be returned in a particular way, but it wasn't. 3. Anything else that doesn't fall into a different bucket.  |
| <code>BAD_INPUT</code> | <code>3</code> | BAD_INPUT is returned when an end-user either sends SQL that couldn't be parsed correctly, or tries a query that isn't supported by Vitess.  |
| <code>DEADLINE_EXCEEDED</code> | <code>4</code> | DEADLINE_EXCEEDED is returned when an action is taking longer than a given timeout.  |
| <code>INTEGRITY_ERROR</code> | <code>5</code> | INTEGRITY_ERROR is returned on integrity error from MySQL, usually due to duplicate primary keys  |
| <code>PERMISSION_DENIED</code> | <code>6</code> | PERMISSION_DENIED errors are returned when a user requests access to something that they don't have permissions for.  |
| <code>RESOURCE_EXHAUSTED</code> | <code>7</code> | RESOURCE_EXHAUSTED is returned when a query exceeds its quota in some dimension and can't be completed due to that. Queries that return RESOURCE_EXHAUSTED should not be retried, as it could be detrimental to the server's health. Examples of errors that will cause the RESOURCE_EXHAUSTED code: 1. TxPoolFull: this is retried server-side, and is only returned as an error if the server-side retries failed. 2. Query is killed due to it taking too long.  |
| <code>QUERY_NOT_SERVED</code> | <code>8</code> | QUERY_NOT_SERVED means that a query could not be served right now. Client can interpret it as: "the tablet that you sent this query to cannot serve the query right now, try a different tablet or try again later." This could be due to various reasons: QueryService is not serving, should not be serving, wrong shard, wrong tablet type, blacklisted table, etc. Clients that receive this error should usually retry the query, but after taking the appropriate steps to make sure that the query will get sent to the correct tablet.  |
| <code>NOT_IN_TX</code> | <code>9</code> | NOT_IN_TX means that we're not currently in a transaction, but we should be.  |
| <code>INTERNAL_ERROR</code> | <code>10</code> | INTERNAL_ERRORs are problems that only the server can fix, not the client. These errors are not due to a query itself, but rather due to the state of the system. Generally, we don't expect the errors to go away by themselves, but they may go away after human intervention. Examples of scenarios where INTERNAL_ERROR is returned: 1. Something is not configured correctly internally. 2. A necessary resource is not available, and we don't expect it to become available by itself. 3. A sanity check fails 4. Some other internal error occurs Clients should not retry immediately, as there is little chance of success. However, it's acceptable for retries to happen internally, for example to multiple backends, in case only a subset of backend are not functional.  |
| <code>TRANSIENT_ERROR</code> | <code>11</code> | TRANSIENT_ERROR is used for when there is some error that we expect we can recover from automatically - often due to a resource limit temporarily being reached. Retrying this error, with an exponential backoff, should succeed. Clients should be able to successfully retry the query on the same backends. Examples of things that can trigger this error: 1. Query has been throttled 2. VtGate could have request backlog  |
| <code>UNAUTHENTICATED</code> | <code>12</code> | UNAUTHENTICATED errors are returned when a user requests access to something, and we're unable to verify the user's authentication.  |

## Messages

### BoundKeyspaceIdQuery

BoundKeyspaceIdQuery represents a single query request for the specified list of keyspace ids. This is used in a list for ExecuteBatchKeyspaceIdsRequest.

#### Properties

| Name |Description |
| :-------- | :-------- 
| <code>query</code> <br>[query.BoundQuery](#query.boundquery)| BoundQuery is a query with its bind variables |
| <code>keyspace</code> <br>string| |
| <code>keyspace_ids</code> <br>list &lt;bytes&gt;| |

### BoundShardQuery

BoundShardQuery represents a single query request for the specified list of shards. This is used in a list for ExecuteBatchShardsRequest.

#### Properties

| Name |Description |
| :-------- | :-------- 
| <code>query</code> <br>[query.BoundQuery](#query.boundquery)| BoundQuery is a query with its bind variables |
| <code>keyspace</code> <br>string| |
| <code>shards</code> <br>list &lt;string&gt;| |

### Session

Session objects are session cookies and are invalidated on use. Query results will contain updated session values. Their content should be opaque to the user.

#### Properties

| Name |Description |
| :-------- | :-------- 
| <code>in_transaction</code> <br>bool| |
| <code>shard_sessions</code> <br>list &lt;[ShardSession](#session.shardsession)&gt;| |

#### Messages

##### Session.ShardSession

<em>Properties</em>

| Name |Description |
| :-------- | :-------- 
| <code>target</code> <br>[query.Target](#query.target)| Target describes what the client expects the tablet is. If the tablet does not match, an error is returned. |
| <code>transaction_id</code> <br>int64| |

### query.BindVariable

BindVariable represents a single bind variable in a Query

#### Properties

| Name |Description |
| :-------- | :-------- 
| <code>type</code> <br>[Type](#bindvariable.type)| |
| <code>value_bytes</code> <br>bytes| Depending on type, only one value below is set. |
| <code>value_int</code> <br>int64| |
| <code>value_uint</code> <br>uint64| |
| <code>value_float</code> <br>double| |
| <code>value_bytes_list</code> <br>list &lt;bytes&gt;| |
| <code>value_int_list</code> <br>list &lt;int64&gt;| |
| <code>value_uint_list</code> <br>list &lt;uint64&gt;| |
| <code>value_float_list</code> <br>list &lt;double&gt;| |

#### Enums

##### BindVariable.Type

| Name |Value |Description |
| :-------- | :-------- | :-------- 
| <code>TYPE_NULL</code> | <code>0</code> |  |
| <code>TYPE_BYTES</code> | <code>1</code> |  |
| <code>TYPE_INT</code> | <code>2</code> |  |
| <code>TYPE_UINT</code> | <code>3</code> |  |
| <code>TYPE_FLOAT</code> | <code>4</code> |  |
| <code>TYPE_BYTES_LIST</code> | <code>5</code> |  |
| <code>TYPE_INT_LIST</code> | <code>6</code> |  |
| <code>TYPE_UINT_LIST</code> | <code>7</code> |  |
| <code>TYPE_FLOAT_LIST</code> | <code>8</code> |  |

### query.BoundQuery

BoundQuery is a query with its bind variables

#### Properties

| Name |Description |
| :-------- | :-------- 
| <code>sql</code> <br>bytes| |
| <code>bind_variables</code> <br>map &lt;string, [BindVariable](#query.bindvariable)&gt;| |

### query.Field

Field describes a single column returned by a query

#### Properties

| Name |Description |
| :-------- | :-------- 
| <code>name</code> <br>string| name of the field as returned by mysql C API |
| <code>type</code> <br>[Type](#field.type)| |
| <code>flags</code> <br>int64| flags is essentially a bitset<Flag>. |

#### Enums

##### Field.Flag

 Flag contains the MySQL field flags bitset values e.g. to distinguish between signed and unsigned integer.  These numbers should exactly match values defined in dist/mysql-5.1.52/include/mysql_com.h

| Name |Value |Description |
| :-------- | :-------- | :-------- 
| <code>VT_ZEROVALUE_FLAG</code> | <code>0</code> | ZEROVALUE_FLAG is not part of the MySQL specification and only used in unit tests.  |
| <code>VT_NOT_NULL_FLAG</code> | <code>1</code> |  |
| <code>VT_PRI_KEY_FLAG</code> | <code>2</code> |  |
| <code>VT_UNIQUE_KEY_FLAG</code> | <code>4</code> |  |
| <code>VT_MULTIPLE_KEY_FLAG</code> | <code>8</code> |  |
| <code>VT_BLOB_FLAG</code> | <code>16</code> |  |
| <code>VT_UNSIGNED_FLAG</code> | <code>32</code> |  |
| <code>VT_ZEROFILL_FLAG</code> | <code>64</code> |  |
| <code>VT_BINARY_FLAG</code> | <code>128</code> |  |
| <code>VT_ENUM_FLAG</code> | <code>256</code> |  |
| <code>VT_AUTO_INCREMENT_FLAG</code> | <code>512</code> |  |
| <code>VT_TIMESTAMP_FLAG</code> | <code>1024</code> |  |
| <code>VT_SET_FLAG</code> | <code>2048</code> |  |
| <code>VT_NO_DEFAULT_VALUE_FLAG</code> | <code>4096</code> |  |
| <code>VT_ON_UPDATE_NOW_FLAG</code> | <code>8192</code> |  |
| <code>VT_NUM_FLAG</code> | <code>32768</code> |  |

##### Field.Type

 Type follows enum_field_types from mysql.h.

| Name |Value |Description |
| :-------- | :-------- | :-------- 
| <code>TYPE_DECIMAL</code> | <code>0</code> |  |
| <code>TYPE_TINY</code> | <code>1</code> |  |
| <code>TYPE_SHORT</code> | <code>2</code> |  |
| <code>TYPE_LONG</code> | <code>3</code> |  |
| <code>TYPE_FLOAT</code> | <code>4</code> |  |
| <code>TYPE_DOUBLE</code> | <code>5</code> |  |
| <code>TYPE_NULL</code> | <code>6</code> |  |
| <code>TYPE_TIMESTAMP</code> | <code>7</code> |  |
| <code>TYPE_LONGLONG</code> | <code>8</code> |  |
| <code>TYPE_INT24</code> | <code>9</code> |  |
| <code>TYPE_DATE</code> | <code>10</code> |  |
| <code>TYPE_TIME</code> | <code>11</code> |  |
| <code>TYPE_DATETIME</code> | <code>12</code> |  |
| <code>TYPE_YEAR</code> | <code>13</code> |  |
| <code>TYPE_NEWDATE</code> | <code>14</code> |  |
| <code>TYPE_VARCHAR</code> | <code>15</code> |  |
| <code>TYPE_BIT</code> | <code>16</code> |  |
| <code>TYPE_NEWDECIMAL</code> | <code>246</code> |  |
| <code>TYPE_ENUM</code> | <code>247</code> |  |
| <code>TYPE_SET</code> | <code>248</code> |  |
| <code>TYPE_TINY_BLOB</code> | <code>249</code> |  |
| <code>TYPE_MEDIUM_BLOB</code> | <code>250</code> |  |
| <code>TYPE_LONG_BLOB</code> | <code>251</code> |  |
| <code>TYPE_BLOB</code> | <code>252</code> |  |
| <code>TYPE_VAR_STRING</code> | <code>253</code> |  |
| <code>TYPE_STRING</code> | <code>254</code> |  |
| <code>TYPE_GEOMETRY</code> | <code>255</code> |  |

### query.QueryResult

QueryResult is returned by Execute and ExecuteStream.  As returned by Execute, len(fields) is always equal to len(row) (for each row in rows).  As returned by StreamExecute, the first QueryResult has the fields set, and subsequent QueryResult have rows set. And as Execute, len(QueryResult[0].fields) is always equal to len(row) (for each row in rows for each QueryResult in QueryResult[1:]).

#### Properties

| Name |Description |
| :-------- | :-------- 
| <code>fields</code> <br>list &lt;[Field](#query.field)&gt;| Field describes a single column returned by a query |
| <code>rows_affected</code> <br>uint64| |
| <code>insert_id</code> <br>uint64| |
| <code>rows</code> <br>list &lt;[Row](#query.row)&gt;| Row is a database row. |

### query.Row

Row is a database row.

#### Properties

| Name |Description |
| :-------- | :-------- 
| <code>values</code> <br>list &lt;bytes&gt;| |

### query.Target

Target describes what the client expects the tablet is. If the tablet does not match, an error is returned.

#### Properties

| Name |Description |
| :-------- | :-------- 
| <code>keyspace</code> <br>string| |
| <code>shard</code> <br>string| |
| <code>tablet_type</code> <br>[topodata.TabletType](#topodata.tablettype)| TabletType represents the type of a given tablet. |

### topodata.KeyRange

KeyRange describes a range of sharding keys, when range-based sharding is used.

#### Properties

| Name |Description |
| :-------- | :-------- 
| <code>start</code> <br>bytes| |
| <code>end</code> <br>bytes| |

### topodata.ShardReference

ShardReference is used as a pointer from a SrvKeyspace to a SrvShard

#### Properties

| Name |Description |
| :-------- | :-------- 
| <code>name</code> <br>string| Copied from Shard. |
| <code>key_range</code> <br>[KeyRange](#topodata.keyrange)| KeyRange describes a range of sharding keys, when range-based sharding is used. |

### topodata.SrvKeyspace

SrvKeyspace is a rollup node for the keyspace itself.

#### Properties

| Name |Description |
| :-------- | :-------- 
| <code>partitions</code> <br>list &lt;[KeyspacePartition](#srvkeyspace.keyspacepartition)&gt;| The partitions this keyspace is serving, per tablet type. |
| <code>sharding_column_name</code> <br>string| copied from Keyspace |
| <code>sharding_column_type</code> <br>[KeyspaceIdType](#topodata.keyspaceidtype)| |
| <code>served_from</code> <br>list &lt;[ServedFrom](#srvkeyspace.servedfrom)&gt;| |
| <code>split_shard_count</code> <br>int32| |

#### Messages

##### SrvKeyspace.KeyspacePartition

<em>Properties</em>

| Name |Description |
| :-------- | :-------- 
| <code>served_type</code> <br>[TabletType](#topodata.tablettype)| The type this partition applies to. |
| <code>shard_references</code> <br>list &lt;[ShardReference](#topodata.shardreference)&gt;| ShardReference is used as a pointer from a SrvKeyspace to a SrvShard |

##### SrvKeyspace.ServedFrom

ServedFrom indicates a relationship between a TabletType and the keyspace name that's serving it.

<em>Properties</em>

| Name |Description |
| :-------- | :-------- 
| <code>tablet_type</code> <br>[TabletType](#topodata.tablettype)| ServedFrom indicates a relationship between a TabletType and the keyspace name that's serving it. the tablet type |
| <code>keyspace</code> <br>string| the keyspace name that's serving it |

### vtrpc.CallerID

CallerID is passed along RPCs to identify the originating client for a request. It is not meant to be secure, but only informational.  The client can put whatever info they want in these fields, and they will be trusted by the servers. The fields will just be used for logging purposes, and to easily find a client. VtGate propagates it to VtTablet, and VtTablet may use this information for monitoring purposes, to display on dashboards, or for blacklisting purposes.

#### Properties

| Name |Description |
| :-------- | :-------- 
| <code>principal</code> <br>string| principal is the effective user identifier. It is usually filled in with whoever made the request to the appserver, if the request came from an automated job or another system component. If the request comes directly from the Internet, or if the Vitess client takes action on its own accord, it is okay for this field to be absent. |
| <code>component</code> <br>string| component describes the running process of the effective caller. It can for instance be the hostname:port of the servlet initiating the database call, or the container engine ID used by the servlet. |
| <code>subcomponent</code> <br>string| subcomponent describes a component inisde the immediate caller which is responsible for generating is request. Suggested values are a servlet name or an API endpoint name. |

### vtrpc.RPCError

RPCError is an application-level error structure returned by VtTablet (and passed along by VtGate if appropriate). We use this so the clients don't have to parse the error messages, but instead can depend on the value of the code.

#### Properties

| Name |Description |
| :-------- | :-------- 
| <code>code</code> <br>[ErrorCode](#vtrpc.errorcode)| |
| <code>message</code> <br>string| |

