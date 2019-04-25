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
<td><code><a href="#resolvetransaction">ResolveTransaction</a></code></td>
<td>ResolveTransaction resolves a transaction.</td>
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
<td>Execute tries to route the query to the right shard. It depends on the query and bind variables to provide enough information in conjunction with the vindexes to route the query.</td>
</tr>
<tr>
<td><code><a href="#streamexecute">StreamExecute</a></code></td>
<td>StreamExecute executes a streaming query based on shards. It depends on the query and bind variables to provide enough information in conjunction with the vindexes to route the query. Use this method if the query returns a large number of rows.</td>
</tr>
</table>
##Range-based Sharding
### ExecuteBatchKeyspaceIds

ExecuteBatchKeyspaceIds executes the list of queries based on the specified keyspace ids.

#### Request

 ExecuteBatchKeyspaceIdsRequest is the payload to ExecuteBatchKeyspaceId.

##### Parameters

| Name |Description |
| :-------- | :-------- 
| <code>caller_id</code> <br>[vtrpc.CallerID](#vtrpc.callerid)| CallerID is passed along RPCs to identify the originating client for a request. It is not meant to be secure, but only informational.  The client can put whatever info they want in these fields, and they will be trusted by the servers. The fields will just be used for logging purposes, and to easily find a client. VtGate propagates it to VtTablet, and VtTablet may use this information for monitoring purposes, to display on dashboards, or for blacklisting purposes. |
| <code>session</code> <br>[Session](#session)| Session objects are session cookies and are invalidated on use. Query results will contain updated session values. Their content should be opaque to the user. |
| <code>queries</code> <br>list &lt;[BoundKeyspaceIdQuery](#boundkeyspaceidquery)&gt;| BoundKeyspaceIdQuery represents a single query request for the specified list of keyspace ids. This is used in a list for ExecuteBatchKeyspaceIdsRequest. |
| <code>tablet_type</code> <br>[topodata.TabletType](#topodata.tablettype)| TabletType represents the type of a given tablet. |
| <code>as_transaction</code> <br>bool| as_transaction will execute the queries in this batch in a single transaction per shard, created for this purpose. (this can be seen as adding a 'begin' before and 'commit' after the queries). Only makes sense if tablet_type is master. If set, the Session is ignored. |
| <code>options</code> <br>[query.ExecuteOptions](#query.executeoptions)| ExecuteOptions is passed around for all Execute calls. |

#### Response

 ExecuteBatchKeyspaceIdsResponse is the returned value from ExecuteBatchKeyspaceId.

##### Properties

| Name |Description |
| :-------- | :-------- 
| <code>error</code> <br>[vtrpc.RPCError](#vtrpc.rpcerror)| RPCError is an application-level error structure returned by VtTablet (and passed along by VtGate if appropriate). We use this so the clients don't have to parse the error messages, but instead can depend on the value of the code. |
| <code>session</code> <br>[Session](#session)| Session objects are session cookies and are invalidated on use. Query results will contain updated session values. Their content should be opaque to the user. |
| <code>results</code> <br>list &lt;[query.QueryResult](#query.queryresult)&gt;| QueryResult is returned by Execute and ExecuteStream.  As returned by Execute, len(fields) is always equal to len(row) (for each row in rows).  As returned by StreamExecute, the first QueryResult has the fields set, and subsequent QueryResult have rows set. And as Execute, len(QueryResult[0].fields) is always equal to len(row) (for each row in rows for each QueryResult in QueryResult[1:]). |

### ExecuteEntityIds

ExecuteEntityIds executes the query based on the specified external id to keyspace id map.

#### Request

 ExecuteEntityIdsRequest is the payload to ExecuteEntityIds.

##### Parameters

| Name |Description |
| :-------- | :-------- 
| <code>caller_id</code> <br>[vtrpc.CallerID](#vtrpc.callerid)| CallerID is passed along RPCs to identify the originating client for a request. It is not meant to be secure, but only informational.  The client can put whatever info they want in these fields, and they will be trusted by the servers. The fields will just be used for logging purposes, and to easily find a client. VtGate propagates it to VtTablet, and VtTablet may use this information for monitoring purposes, to display on dashboards, or for blacklisting purposes. |
| <code>session</code> <br>[Session](#session)| Session objects are session cookies and are invalidated on use. Query results will contain updated session values. Their content should be opaque to the user. |
| <code>query</code> <br>[query.BoundQuery](#query.boundquery)| BoundQuery is a query with its bind variables |
| <code>keyspace</code> <br>string| keyspace to target the query to. |
| <code>entity_column_name</code> <br>string| entity_column_name is the column name to use. |
| <code>entity_keyspace_ids</code> <br>list &lt;[EntityId](#executeentityidsrequest.entityid)&gt;| entity_keyspace_ids are pairs of entity_column_name values associated with its corresponding keyspace_id. |
| <code>tablet_type</code> <br>[topodata.TabletType](#topodata.tablettype)| TabletType represents the type of a given tablet. |
| <code>not_in_transaction</code> <br>bool| not_in_transaction is deprecated and should not be used. |
| <code>options</code> <br>[query.ExecuteOptions](#query.executeoptions)| ExecuteOptions is passed around for all Execute calls. |

#### Messages

##### ExecuteEntityIdsRequest.EntityId

<em>Properties</em>

| Name |Description |
| :-------- | :-------- 
| <code>type</code> <br>[query.Type](#query.type)| Type defines the various supported data types in bind vars and query results. |
| <code>value</code> <br>bytes| value is the value for the entity. Not set if type is NULL_TYPE. |
| <code>keyspace_id</code> <br>bytes| keyspace_id is the associated keyspace_id for the entity. |

#### Response

 ExecuteEntityIdsResponse is the returned value from ExecuteEntityIds.

##### Properties

| Name |Description |
| :-------- | :-------- 
| <code>error</code> <br>[vtrpc.RPCError](#vtrpc.rpcerror)| RPCError is an application-level error structure returned by VtTablet (and passed along by VtGate if appropriate). We use this so the clients don't have to parse the error messages, but instead can depend on the value of the code. |
| <code>session</code> <br>[Session](#session)| Session objects are session cookies and are invalidated on use. Query results will contain updated session values. Their content should be opaque to the user. |
| <code>result</code> <br>[query.QueryResult](#query.queryresult)| QueryResult is returned by Execute and ExecuteStream.  As returned by Execute, len(fields) is always equal to len(row) (for each row in rows).  As returned by StreamExecute, the first QueryResult has the fields set, and subsequent QueryResult have rows set. And as Execute, len(QueryResult[0].fields) is always equal to len(row) (for each row in rows for each QueryResult in QueryResult[1:]). |

### ExecuteKeyRanges

ExecuteKeyRanges executes the query based on the specified key ranges.

#### Request

 ExecuteKeyRangesRequest is the payload to ExecuteKeyRanges.

##### Parameters

| Name |Description |
| :-------- | :-------- 
| <code>caller_id</code> <br>[vtrpc.CallerID](#vtrpc.callerid)| CallerID is passed along RPCs to identify the originating client for a request. It is not meant to be secure, but only informational.  The client can put whatever info they want in these fields, and they will be trusted by the servers. The fields will just be used for logging purposes, and to easily find a client. VtGate propagates it to VtTablet, and VtTablet may use this information for monitoring purposes, to display on dashboards, or for blacklisting purposes. |
| <code>session</code> <br>[Session](#session)| Session objects are session cookies and are invalidated on use. Query results will contain updated session values. Their content should be opaque to the user. |
| <code>query</code> <br>[query.BoundQuery](#query.boundquery)| BoundQuery is a query with its bind variables |
| <code>keyspace</code> <br>string| keyspace to target the query to |
| <code>key_ranges</code> <br>list &lt;[topodata.KeyRange](#topodata.keyrange)&gt;| KeyRange describes a range of sharding keys, when range-based sharding is used. |
| <code>tablet_type</code> <br>[topodata.TabletType](#topodata.tablettype)| TabletType represents the type of a given tablet. |
| <code>not_in_transaction</code> <br>bool| not_in_transaction is deprecated and should not be used. |
| <code>options</code> <br>[query.ExecuteOptions](#query.executeoptions)| ExecuteOptions is passed around for all Execute calls. |

#### Response

 ExecuteKeyRangesResponse is the returned value from ExecuteKeyRanges.

##### Properties

| Name |Description |
| :-------- | :-------- 
| <code>error</code> <br>[vtrpc.RPCError](#vtrpc.rpcerror)| RPCError is an application-level error structure returned by VtTablet (and passed along by VtGate if appropriate). We use this so the clients don't have to parse the error messages, but instead can depend on the value of the code. |
| <code>session</code> <br>[Session](#session)| Session objects are session cookies and are invalidated on use. Query results will contain updated session values. Their content should be opaque to the user. |
| <code>result</code> <br>[query.QueryResult](#query.queryresult)| QueryResult is returned by Execute and ExecuteStream.  As returned by Execute, len(fields) is always equal to len(row) (for each row in rows).  As returned by StreamExecute, the first QueryResult has the fields set, and subsequent QueryResult have rows set. And as Execute, len(QueryResult[0].fields) is always equal to len(row) (for each row in rows for each QueryResult in QueryResult[1:]). |

### ExecuteKeyspaceIds

ExecuteKeyspaceIds executes the query based on the specified keyspace ids.

#### Request

 ExecuteKeyspaceIdsRequest is the payload to ExecuteKeyspaceIds.

##### Parameters

| Name |Description |
| :-------- | :-------- 
| <code>caller_id</code> <br>[vtrpc.CallerID](#vtrpc.callerid)| CallerID is passed along RPCs to identify the originating client for a request. It is not meant to be secure, but only informational.  The client can put whatever info they want in these fields, and they will be trusted by the servers. The fields will just be used for logging purposes, and to easily find a client. VtGate propagates it to VtTablet, and VtTablet may use this information for monitoring purposes, to display on dashboards, or for blacklisting purposes. |
| <code>session</code> <br>[Session](#session)| Session objects are session cookies and are invalidated on use. Query results will contain updated session values. Their content should be opaque to the user. |
| <code>query</code> <br>[query.BoundQuery](#query.boundquery)| BoundQuery is a query with its bind variables |
| <code>keyspace</code> <br>string| keyspace to target the query to. |
| <code>keyspace_ids</code> <br>list &lt;bytes&gt;| keyspace_ids contains the list of keyspace_ids affected by this query. Will be used to find the shards to send the query to. |
| <code>tablet_type</code> <br>[topodata.TabletType](#topodata.tablettype)| TabletType represents the type of a given tablet. |
| <code>not_in_transaction</code> <br>bool| not_in_transaction is deprecated and should not be used. |
| <code>options</code> <br>[query.ExecuteOptions](#query.executeoptions)| ExecuteOptions is passed around for all Execute calls. |

#### Response

 ExecuteKeyspaceIdsResponse is the returned value from ExecuteKeyspaceIds.

##### Properties

| Name |Description |
| :-------- | :-------- 
| <code>error</code> <br>[vtrpc.RPCError](#vtrpc.rpcerror)| RPCError is an application-level error structure returned by VtTablet (and passed along by VtGate if appropriate). We use this so the clients don't have to parse the error messages, but instead can depend on the value of the code. |
| <code>session</code> <br>[Session](#session)| Session objects are session cookies and are invalidated on use. Query results will contain updated session values. Their content should be opaque to the user. |
| <code>result</code> <br>[query.QueryResult](#query.queryresult)| QueryResult is returned by Execute and ExecuteStream.  As returned by Execute, len(fields) is always equal to len(row) (for each row in rows).  As returned by StreamExecute, the first QueryResult has the fields set, and subsequent QueryResult have rows set. And as Execute, len(QueryResult[0].fields) is always equal to len(row) (for each row in rows for each QueryResult in QueryResult[1:]). |

### StreamExecuteKeyRanges

StreamExecuteKeyRanges executes a streaming query based on key ranges. Use this method if the query returns a large number of rows.

#### Request

 StreamExecuteKeyRangesRequest is the payload to StreamExecuteKeyRanges.

##### Parameters

| Name |Description |
| :-------- | :-------- 
| <code>caller_id</code> <br>[vtrpc.CallerID](#vtrpc.callerid)| CallerID is passed along RPCs to identify the originating client for a request. It is not meant to be secure, but only informational.  The client can put whatever info they want in these fields, and they will be trusted by the servers. The fields will just be used for logging purposes, and to easily find a client. VtGate propagates it to VtTablet, and VtTablet may use this information for monitoring purposes, to display on dashboards, or for blacklisting purposes. |
| <code>query</code> <br>[query.BoundQuery](#query.boundquery)| BoundQuery is a query with its bind variables |
| <code>keyspace</code> <br>string| keyspace to target the query to. |
| <code>key_ranges</code> <br>list &lt;[topodata.KeyRange](#topodata.keyrange)&gt;| KeyRange describes a range of sharding keys, when range-based sharding is used. |
| <code>tablet_type</code> <br>[topodata.TabletType](#topodata.tablettype)| TabletType represents the type of a given tablet. |
| <code>options</code> <br>[query.ExecuteOptions](#query.executeoptions)| ExecuteOptions is passed around for all Execute calls. |

#### Response

 StreamExecuteKeyRangesResponse is the returned value from StreamExecuteKeyRanges.

##### Properties

| Name |Description |
| :-------- | :-------- 
| <code>result</code> <br>[query.QueryResult](#query.queryresult)| QueryResult is returned by Execute and ExecuteStream.  As returned by Execute, len(fields) is always equal to len(row) (for each row in rows).  As returned by StreamExecute, the first QueryResult has the fields set, and subsequent QueryResult have rows set. And as Execute, len(QueryResult[0].fields) is always equal to len(row) (for each row in rows for each QueryResult in QueryResult[1:]). |

### StreamExecuteKeyspaceIds

StreamExecuteKeyspaceIds executes a streaming query based on keyspace ids. Use this method if the query returns a large number of rows.

#### Request

 StreamExecuteKeyspaceIdsRequest is the payload to StreamExecuteKeyspaceIds.

##### Parameters

| Name |Description |
| :-------- | :-------- 
| <code>caller_id</code> <br>[vtrpc.CallerID](#vtrpc.callerid)| CallerID is passed along RPCs to identify the originating client for a request. It is not meant to be secure, but only informational.  The client can put whatever info they want in these fields, and they will be trusted by the servers. The fields will just be used for logging purposes, and to easily find a client. VtGate propagates it to VtTablet, and VtTablet may use this information for monitoring purposes, to display on dashboards, or for blacklisting purposes. |
| <code>query</code> <br>[query.BoundQuery](#query.boundquery)| BoundQuery is a query with its bind variables |
| <code>keyspace</code> <br>string| keyspace to target the query to. |
| <code>keyspace_ids</code> <br>list &lt;bytes&gt;| keyspace_ids contains the list of keyspace_ids affected by this query. Will be used to find the shards to send the query to. |
| <code>tablet_type</code> <br>[topodata.TabletType](#topodata.tablettype)| TabletType represents the type of a given tablet. |
| <code>options</code> <br>[query.ExecuteOptions](#query.executeoptions)| ExecuteOptions is passed around for all Execute calls. |

#### Response

 StreamExecuteKeyspaceIdsResponse is the returned value from StreamExecuteKeyspaceIds.

##### Properties

| Name |Description |
| :-------- | :-------- 
| <code>result</code> <br>[query.QueryResult](#query.queryresult)| QueryResult is returned by Execute and ExecuteStream.  As returned by Execute, len(fields) is always equal to len(row) (for each row in rows).  As returned by StreamExecute, the first QueryResult has the fields set, and subsequent QueryResult have rows set. And as Execute, len(QueryResult[0].fields) is always equal to len(row) (for each row in rows for each QueryResult in QueryResult[1:]). |

##Transactions
### Begin

Begin a transaction.

#### Request

 BeginRequest is the payload to Begin.

##### Parameters

| Name |Description |
| :-------- | :-------- 
| <code>caller_id</code> <br>[vtrpc.CallerID](#vtrpc.callerid)| CallerID is passed along RPCs to identify the originating client for a request. It is not meant to be secure, but only informational.  The client can put whatever info they want in these fields, and they will be trusted by the servers. The fields will just be used for logging purposes, and to easily find a client. VtGate propagates it to VtTablet, and VtTablet may use this information for monitoring purposes, to display on dashboards, or for blacklisting purposes. |
| <code>single_db</code> <br>bool| single_db specifies if the transaction should be restricted to a single database. |

#### Response

 BeginResponse is the returned value from Begin.

##### Properties

| Name |Description |
| :-------- | :-------- 
| <code>session</code> <br>[Session](#session)| Session objects are session cookies and are invalidated on use. Query results will contain updated session values. Their content should be opaque to the user. |

### Commit

Commit a transaction.

#### Request

 CommitRequest is the payload to Commit.

##### Parameters

| Name |Description |
| :-------- | :-------- 
| <code>caller_id</code> <br>[vtrpc.CallerID](#vtrpc.callerid)| CallerID is passed along RPCs to identify the originating client for a request. It is not meant to be secure, but only informational.  The client can put whatever info they want in these fields, and they will be trusted by the servers. The fields will just be used for logging purposes, and to easily find a client. VtGate propagates it to VtTablet, and VtTablet may use this information for monitoring purposes, to display on dashboards, or for blacklisting purposes. |
| <code>session</code> <br>[Session](#session)| Session objects are session cookies and are invalidated on use. Query results will contain updated session values. Their content should be opaque to the user. |
| <code>atomic</code> <br>bool| atomic specifies if the commit should go through the 2PC workflow to ensure atomicity. |

#### Response

 CommitResponse is the returned value from Commit.

##### Properties

| Name |Description |
| :-------- | :-------- 

### ResolveTransaction

ResolveTransaction resolves a transaction.

#### Request

 ResolveTransactionRequest is the payload to ResolveTransaction.

##### Parameters

| Name |Description |
| :-------- | :-------- 
| <code>caller_id</code> <br>[vtrpc.CallerID](#vtrpc.callerid)| CallerID is passed along RPCs to identify the originating client for a request. It is not meant to be secure, but only informational.  The client can put whatever info they want in these fields, and they will be trusted by the servers. The fields will just be used for logging purposes, and to easily find a client. VtGate propagates it to VtTablet, and VtTablet may use this information for monitoring purposes, to display on dashboards, or for blacklisting purposes. |
| <code>dtid</code> <br>string| dtid is the dtid of the transaction to be resolved. |

#### Response

 ResolveTransactionResponse is the returned value from Rollback.

##### Properties

| Name |Description |
| :-------- | :-------- 

### Rollback

Rollback a transaction.

#### Request

 RollbackRequest is the payload to Rollback.

##### Parameters

| Name |Description |
| :-------- | :-------- 
| <code>caller_id</code> <br>[vtrpc.CallerID](#vtrpc.callerid)| CallerID is passed along RPCs to identify the originating client for a request. It is not meant to be secure, but only informational.  The client can put whatever info they want in these fields, and they will be trusted by the servers. The fields will just be used for logging purposes, and to easily find a client. VtGate propagates it to VtTablet, and VtTablet may use this information for monitoring purposes, to display on dashboards, or for blacklisting purposes. |
| <code>session</code> <br>[Session](#session)| Session objects are session cookies and are invalidated on use. Query results will contain updated session values. Their content should be opaque to the user. |

#### Response

 RollbackResponse is the returned value from Rollback.

##### Properties

| Name |Description |
| :-------- | :-------- 

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
| <code>as_transaction</code> <br>bool| as_transaction will execute the queries in this batch in a single transaction per shard, created for this purpose. (this can be seen as adding a 'begin' before and 'commit' after the queries). Only makes sense if tablet_type is master. If set, the Session is ignored. |
| <code>options</code> <br>[query.ExecuteOptions](#query.executeoptions)| ExecuteOptions is passed around for all Execute calls. |

#### Response

 ExecuteBatchShardsResponse is the returned value from ExecuteBatchShards.

##### Properties

| Name |Description |
| :-------- | :-------- 
| <code>error</code> <br>[vtrpc.RPCError](#vtrpc.rpcerror)| RPCError is an application-level error structure returned by VtTablet (and passed along by VtGate if appropriate). We use this so the clients don't have to parse the error messages, but instead can depend on the value of the code. |
| <code>session</code> <br>[Session](#session)| Session objects are session cookies and are invalidated on use. Query results will contain updated session values. Their content should be opaque to the user. |
| <code>results</code> <br>list &lt;[query.QueryResult](#query.queryresult)&gt;| QueryResult is returned by Execute and ExecuteStream.  As returned by Execute, len(fields) is always equal to len(row) (for each row in rows).  As returned by StreamExecute, the first QueryResult has the fields set, and subsequent QueryResult have rows set. And as Execute, len(QueryResult[0].fields) is always equal to len(row) (for each row in rows for each QueryResult in QueryResult[1:]). |

### ExecuteShards

ExecuteShards executes the query on the specified shards.

#### Request

 ExecuteShardsRequest is the payload to ExecuteShards.

##### Parameters

| Name |Description |
| :-------- | :-------- 
| <code>caller_id</code> <br>[vtrpc.CallerID](#vtrpc.callerid)| CallerID is passed along RPCs to identify the originating client for a request. It is not meant to be secure, but only informational.  The client can put whatever info they want in these fields, and they will be trusted by the servers. The fields will just be used for logging purposes, and to easily find a client. VtGate propagates it to VtTablet, and VtTablet may use this information for monitoring purposes, to display on dashboards, or for blacklisting purposes. |
| <code>session</code> <br>[Session](#session)| Session objects are session cookies and are invalidated on use. Query results will contain updated session values. Their content should be opaque to the user. |
| <code>query</code> <br>[query.BoundQuery](#query.boundquery)| BoundQuery is a query with its bind variables |
| <code>keyspace</code> <br>string| keyspace to target the query to. |
| <code>shards</code> <br>list &lt;string&gt;| shards to target the query to. A DML can only target one shard. |
| <code>tablet_type</code> <br>[topodata.TabletType](#topodata.tablettype)| TabletType represents the type of a given tablet. |
| <code>not_in_transaction</code> <br>bool| not_in_transaction is deprecated and should not be used. |
| <code>options</code> <br>[query.ExecuteOptions](#query.executeoptions)| ExecuteOptions is passed around for all Execute calls. |

#### Response

 ExecuteShardsResponse is the returned value from ExecuteShards.

##### Properties

| Name |Description |
| :-------- | :-------- 
| <code>error</code> <br>[vtrpc.RPCError](#vtrpc.rpcerror)| RPCError is an application-level error structure returned by VtTablet (and passed along by VtGate if appropriate). We use this so the clients don't have to parse the error messages, but instead can depend on the value of the code. |
| <code>session</code> <br>[Session](#session)| Session objects are session cookies and are invalidated on use. Query results will contain updated session values. Their content should be opaque to the user. |
| <code>result</code> <br>[query.QueryResult](#query.queryresult)| QueryResult is returned by Execute and ExecuteStream.  As returned by Execute, len(fields) is always equal to len(row) (for each row in rows).  As returned by StreamExecute, the first QueryResult has the fields set, and subsequent QueryResult have rows set. And as Execute, len(QueryResult[0].fields) is always equal to len(row) (for each row in rows for each QueryResult in QueryResult[1:]). |

### StreamExecuteShards

StreamExecuteShards executes a streaming query based on shards. Use this method if the query returns a large number of rows.

#### Request

 StreamExecuteShardsRequest is the payload to StreamExecuteShards.

##### Parameters

| Name |Description |
| :-------- | :-------- 
| <code>caller_id</code> <br>[vtrpc.CallerID](#vtrpc.callerid)| CallerID is passed along RPCs to identify the originating client for a request. It is not meant to be secure, but only informational.  The client can put whatever info they want in these fields, and they will be trusted by the servers. The fields will just be used for logging purposes, and to easily find a client. VtGate propagates it to VtTablet, and VtTablet may use this information for monitoring purposes, to display on dashboards, or for blacklisting purposes. |
| <code>query</code> <br>[query.BoundQuery](#query.boundquery)| BoundQuery is a query with its bind variables |
| <code>keyspace</code> <br>string| keyspace to target the query to. |
| <code>shards</code> <br>list &lt;string&gt;| shards to target the query to. |
| <code>tablet_type</code> <br>[topodata.TabletType](#topodata.tablettype)| TabletType represents the type of a given tablet. |
| <code>options</code> <br>[query.ExecuteOptions](#query.executeoptions)| ExecuteOptions is passed around for all Execute calls. |

#### Response

 StreamExecuteShardsResponse is the returned value from StreamExecuteShards.

##### Properties

| Name |Description |
| :-------- | :-------- 
| <code>result</code> <br>[query.QueryResult](#query.queryresult)| QueryResult is returned by Execute and ExecuteStream.  As returned by Execute, len(fields) is always equal to len(row) (for each row in rows).  As returned by StreamExecute, the first QueryResult has the fields set, and subsequent QueryResult have rows set. And as Execute, len(QueryResult[0].fields) is always equal to len(row) (for each row in rows for each QueryResult in QueryResult[1:]). |

##Map Reduce
### SplitQuery

Split a query into non-overlapping sub queries

#### Request

 SplitQueryRequest is the payload to SplitQuery.  SplitQuery takes a "SELECT" query and generates a list of queries called "query-parts". Each query-part consists of the original query with an added WHERE clause that restricts the query-part to operate only on rows whose values in the columns listed in the "split_column" field of the request (see below) are in a particular range.  It is guaranteed that the set of rows obtained from executing each query-part on a database snapshot and merging (without deduping) the results is equal to the set of rows obtained from executing the original query on the same snapshot with the rows containing NULL values in any of the split_column's excluded.  This is typically called by the MapReduce master when reading from Vitess. There it's desirable that the sets of rows returned by the query-parts have roughly the same size.

##### Parameters

| Name |Description |
| :-------- | :-------- 
| <code>caller_id</code> <br>[vtrpc.CallerID](#vtrpc.callerid)| CallerID is passed along RPCs to identify the originating client for a request. It is not meant to be secure, but only informational.  The client can put whatever info they want in these fields, and they will be trusted by the servers. The fields will just be used for logging purposes, and to easily find a client. VtGate propagates it to VtTablet, and VtTablet may use this information for monitoring purposes, to display on dashboards, or for blacklisting purposes. |
| <code>keyspace</code> <br>string| keyspace to target the query to. |
| <code>query</code> <br>[query.BoundQuery](#query.boundquery)| BoundQuery is a query with its bind variables |
| <code>split_column</code> <br>list &lt;string&gt;| Each generated query-part will be restricted to rows whose values in the columns listed in this field are in a particular range. The list of columns named here must be a prefix of the list of columns defining some index or primary key of the table referenced in 'query'. For many tables using the primary key columns (in order) is sufficient and this is the default if this field is omitted. See the comment on the 'algorithm' field for more restrictions and information. |
| <code>split_count</code> <br>int64| You can specify either an estimate of the number of query-parts to generate or an estimate of the number of rows each query-part should return. Thus, exactly one of split_count or num_rows_per_query_part should be nonzero. The non-given parameter is calculated from the given parameter using the formula: split_count * num_rows_per_query_pary = table_size, where table_size is an approximation of the number of rows in the table. Note that if "split_count" is given it is regarded as an estimate. The number of query-parts returned may differ slightly (in particular, if it's not a whole multiple of the number of vitess shards). |
| <code>num_rows_per_query_part</code> <br>int64| |
| <code>algorithm</code> <br>query.SplitQueryRequest.Algorithm| The algorithm to use to split the query. The split algorithm is performed on each database shard in parallel. The lists of query-parts generated by the shards are merged and returned to the caller. Two algorithms are supported: EQUAL_SPLITS If this algorithm is selected then only the first 'split_column' given is used (or the first primary key column if the 'split_column' field is empty). In the rest of this algorithm's description, we refer to this column as "the split column". The split column must have numeric type (integral or floating point). The algorithm works by taking the interval [min, max], where min and max are the minimum and maximum values of the split column in the table-shard, respectively, and partitioning it into 'split_count' sub-intervals of equal size. The added WHERE clause of each query-part restricts that part to rows whose value in the split column belongs to a particular sub-interval. This is fast, but requires that the distribution of values of the split column be uniform in [min, max] for the number of rows returned by each query part to be roughly the same. FULL_SCAN If this algorithm is used then the split_column must be the primary key columns (in order). This algorithm performs a full-scan of the table-shard referenced in 'query' to get "boundary" rows that are num_rows_per_query_part apart when the table is ordered by the columns listed in 'split_column'. It then restricts each query-part to the rows located between two successive boundary rows. This algorithm supports multiple split_column's of any type, but is slower than EQUAL_SPLITS. |
| <code>use_split_query_v2</code> <br>bool| Remove this field after this new server code is released to prod. We must keep it for now, so that clients can still send it to the old server code currently in production. |

#### Response

 SplitQueryResponse is the returned value from SplitQuery.

##### Properties

| Name |Description |
| :-------- | :-------- 
| <code>splits</code> <br>list &lt;[Part](#splitqueryresponse.part)&gt;| splits contains the queries to run to fetch the entire data set. |

#### Messages

##### SplitQueryResponse.KeyRangePart

<em>Properties</em>

| Name |Description |
| :-------- | :-------- 
| <code>keyspace</code> <br>string| keyspace to target the query to. |
| <code>key_ranges</code> <br>list &lt;[topodata.KeyRange](#topodata.keyrange)&gt;| KeyRange describes a range of sharding keys, when range-based sharding is used. |

##### SplitQueryResponse.Part

<em>Properties</em>

| Name |Description |
| :-------- | :-------- 
| <code>query</code> <br>[query.BoundQuery](#query.boundquery)| BoundQuery is a query with its bind variables |
| <code>key_range_part</code> <br>[KeyRangePart](#splitqueryresponse.keyrangepart)| key_range_part is set if the query should be executed by ExecuteKeyRanges. |
| <code>shard_part</code> <br>[ShardPart](#splitqueryresponse.shardpart)| shard_part is set if the query should be executed by ExecuteShards. |
| <code>size</code> <br>int64| size is the approximate number of rows this query will return. |

##### SplitQueryResponse.ShardPart

<em>Properties</em>

| Name |Description |
| :-------- | :-------- 
| <code>keyspace</code> <br>string| keyspace to target the query to. |
| <code>shards</code> <br>list &lt;string&gt;| shards to target the query to. |

##Topology
### GetSrvKeyspace

GetSrvKeyspace returns a SrvKeyspace object (as seen by this vtgate). This method is provided as a convenient way for clients to take a look at the sharding configuration for a Keyspace. Looking at the sharding information should not be used for routing queries (as the information may change, use the Execute calls for that). It is convenient for monitoring applications for instance, or if using custom sharding.

#### Request

 GetSrvKeyspaceRequest is the payload to GetSrvKeyspace.

##### Parameters

| Name |Description |
| :-------- | :-------- 
| <code>keyspace</code> <br>string| keyspace name to fetch. |

#### Response

 GetSrvKeyspaceResponse is the returned value from GetSrvKeyspace.

##### Properties

| Name |Description |
| :-------- | :-------- 
| <code>srv_keyspace</code> <br>[topodata.SrvKeyspace](#topodata.srvkeyspace)| SrvKeyspace is a rollup node for the keyspace itself. |

##v3 API (alpha)
### Execute

Execute tries to route the query to the right shard. It depends on the query and bind variables to provide enough information in conjunction with the vindexes to route the query.

#### Request

 ExecuteRequest is the payload to Execute.

##### Parameters

| Name |Description |
| :-------- | :-------- 
| <code>caller_id</code> <br>[vtrpc.CallerID](#vtrpc.callerid)| CallerID is passed along RPCs to identify the originating client for a request. It is not meant to be secure, but only informational.  The client can put whatever info they want in these fields, and they will be trusted by the servers. The fields will just be used for logging purposes, and to easily find a client. VtGate propagates it to VtTablet, and VtTablet may use this information for monitoring purposes, to display on dashboards, or for blacklisting purposes. |
| <code>session</code> <br>[Session](#session)| Session objects are session cookies and are invalidated on use. Query results will contain updated session values. Their content should be opaque to the user. |
| <code>query</code> <br>[query.BoundQuery](#query.boundquery)| BoundQuery is a query with its bind variables |
| <code>tablet_type</code> <br>[topodata.TabletType](#topodata.tablettype)| TabletType represents the type of a given tablet. |
| <code>not_in_transaction</code> <br>bool| not_in_transaction is deprecated and should not be used. |
| <code>keyspace</code> <br>string| keyspace to target the query to. |
| <code>options</code> <br>[query.ExecuteOptions](#query.executeoptions)| ExecuteOptions is passed around for all Execute calls. |

#### Response

 ExecuteResponse is the returned value from Execute.

##### Properties

| Name |Description |
| :-------- | :-------- 
| <code>error</code> <br>[vtrpc.RPCError](#vtrpc.rpcerror)| RPCError is an application-level error structure returned by VtTablet (and passed along by VtGate if appropriate). We use this so the clients don't have to parse the error messages, but instead can depend on the value of the code. |
| <code>session</code> <br>[Session](#session)| Session objects are session cookies and are invalidated on use. Query results will contain updated session values. Their content should be opaque to the user. |
| <code>result</code> <br>[query.QueryResult](#query.queryresult)| QueryResult is returned by Execute and ExecuteStream.  As returned by Execute, len(fields) is always equal to len(row) (for each row in rows).  As returned by StreamExecute, the first QueryResult has the fields set, and subsequent QueryResult have rows set. And as Execute, len(QueryResult[0].fields) is always equal to len(row) (for each row in rows for each QueryResult in QueryResult[1:]). |

### StreamExecute

StreamExecute executes a streaming query based on shards. It depends on the query and bind variables to provide enough information in conjunction with the vindexes to route the query. Use this method if the query returns a large number of rows.

#### Request

 StreamExecuteRequest is the payload to StreamExecute.

##### Parameters

| Name |Description |
| :-------- | :-------- 
| <code>caller_id</code> <br>[vtrpc.CallerID](#vtrpc.callerid)| CallerID is passed along RPCs to identify the originating client for a request. It is not meant to be secure, but only informational.  The client can put whatever info they want in these fields, and they will be trusted by the servers. The fields will just be used for logging purposes, and to easily find a client. VtGate propagates it to VtTablet, and VtTablet may use this information for monitoring purposes, to display on dashboards, or for blacklisting purposes. |
| <code>query</code> <br>[query.BoundQuery](#query.boundquery)| BoundQuery is a query with its bind variables |
| <code>tablet_type</code> <br>[topodata.TabletType](#topodata.tablettype)| TabletType represents the type of a given tablet. |
| <code>keyspace</code> <br>string| keyspace to target the query to. |
| <code>options</code> <br>[query.ExecuteOptions](#query.executeoptions)| ExecuteOptions is passed around for all Execute calls. |

#### Response

 StreamExecuteResponse is the returned value from StreamExecute.

##### Properties

| Name |Description |
| :-------- | :-------- 
| <code>result</code> <br>[query.QueryResult](#query.queryresult)| QueryResult is returned by Execute and ExecuteStream.  As returned by Execute, len(fields) is always equal to len(row) (for each row in rows).  As returned by StreamExecute, the first QueryResult has the fields set, and subsequent QueryResult have rows set. And as Execute, len(QueryResult[0].fields) is always equal to len(row) (for each row in rows for each QueryResult in QueryResult[1:]). |

## Enums

### query.Type

 Type defines the various supported data types in bind vars and query results.

| Name |Value |Description |
| :-------- | :-------- | :-------- 
| <code>NULL_TYPE</code> | <code>0</code> | NULL_TYPE specifies a NULL type.  |
| <code>INT8</code> | <code>257</code> | INT8 specifies a TINYINT type. Properties: 1, IsNumber.  |
| <code>UINT8</code> | <code>770</code> | UINT8 specifies a TINYINT UNSIGNED type. Properties: 2, IsNumber, IsUnsigned.  |
| <code>INT16</code> | <code>259</code> | INT16 specifies a SMALLINT type. Properties: 3, IsNumber.  |
| <code>UINT16</code> | <code>772</code> | UINT16 specifies a SMALLINT UNSIGNED type. Properties: 4, IsNumber, IsUnsigned.  |
| <code>INT24</code> | <code>261</code> | INT24 specifies a MEDIUMINT type. Properties: 5, IsNumber.  |
| <code>UINT24</code> | <code>774</code> | UINT24 specifies a MEDIUMINT UNSIGNED type. Properties: 6, IsNumber, IsUnsigned.  |
| <code>INT32</code> | <code>263</code> | INT32 specifies a INTEGER type. Properties: 7, IsNumber.  |
| <code>UINT32</code> | <code>776</code> | UINT32 specifies a INTEGER UNSIGNED type. Properties: 8, IsNumber, IsUnsigned.  |
| <code>INT64</code> | <code>265</code> | INT64 specifies a BIGINT type. Properties: 9, IsNumber.  |
| <code>UINT64</code> | <code>778</code> | UINT64 specifies a BIGINT UNSIGNED type. Properties: 10, IsNumber, IsUnsigned.  |
| <code>FLOAT32</code> | <code>1035</code> | FLOAT32 specifies a FLOAT type. Properties: 11, IsFloat.  |
| <code>FLOAT64</code> | <code>1036</code> | FLOAT64 specifies a DOUBLE or REAL type. Properties: 12, IsFloat.  |
| <code>TIMESTAMP</code> | <code>2061</code> | TIMESTAMP specifies a TIMESTAMP type. Properties: 13, IsQuoted.  |
| <code>DATE</code> | <code>2062</code> | DATE specifies a DATE type. Properties: 14, IsQuoted.  |
| <code>TIME</code> | <code>2063</code> | TIME specifies a TIME type. Properties: 15, IsQuoted.  |
| <code>DATETIME</code> | <code>2064</code> | DATETIME specifies a DATETIME type. Properties: 16, IsQuoted.  |
| <code>YEAR</code> | <code>785</code> | YEAR specifies a YEAR type. Properties: 17, IsNumber, IsUnsigned.  |
| <code>DECIMAL</code> | <code>18</code> | DECIMAL specifies a DECIMAL or NUMERIC type. Properties: 18, None.  |
| <code>TEXT</code> | <code>6163</code> | TEXT specifies a TEXT type. Properties: 19, IsQuoted, IsText.  |
| <code>BLOB</code> | <code>10260</code> | BLOB specifies a BLOB type. Properties: 20, IsQuoted, IsBinary.  |
| <code>VARCHAR</code> | <code>6165</code> | VARCHAR specifies a VARCHAR type. Properties: 21, IsQuoted, IsText.  |
| <code>VARBINARY</code> | <code>10262</code> | VARBINARY specifies a VARBINARY type. Properties: 22, IsQuoted, IsBinary.  |
| <code>CHAR</code> | <code>6167</code> | CHAR specifies a CHAR type. Properties: 23, IsQuoted, IsText.  |
| <code>BINARY</code> | <code>10264</code> | BINARY specifies a BINARY type. Properties: 24, IsQuoted, IsBinary.  |
| <code>BIT</code> | <code>2073</code> | BIT specifies a BIT type. Properties: 25, IsQuoted.  |
| <code>ENUM</code> | <code>2074</code> | ENUM specifies an ENUM type. Properties: 26, IsQuoted.  |
| <code>SET</code> | <code>2075</code> | SET specifies a SET type. Properties: 27, IsQuoted.  |
| <code>TUPLE</code> | <code>28</code> | TUPLE specifies a tuple. This cannot be returned in a QueryResult, but it can be sent as a bind var. Properties: 28, None.  |
| <code>GEOMETRY</code> | <code>2077</code> | GEOMETRY specifies a GEOMETRY type. Properties: 29, IsQuoted.  |
| <code>JSON</code> | <code>2078</code> | JSON specified a JSON type. Properties: 30, IsQuoted.  |

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
| <code>UNKNOWN</code> | <code>0</code> | UNKNOWN is not a valid value.  |
| <code>MASTER</code> | <code>1</code> | MASTER is the master server for the shard. Only MASTER allows DMLs.  |
| <code>REPLICA</code> | <code>2</code> | REPLICA is a slave type. It is used to serve live traffic. A REPLICA can be promoted to MASTER. A demoted MASTER will go to REPLICA.  |
| <code>RDONLY</code> | <code>3</code> | RDONLY (old name) / BATCH (new name) is used to serve traffic for long-running jobs. It is a separate type from REPLICA so long-running queries don't affect web-like traffic.  |
| <code>BATCH</code> | <code>3</code> |  |
| <code>SPARE</code> | <code>4</code> | SPARE is a type of servers that cannot serve queries, but is available in case an extra server is needed.  |
| <code>EXPERIMENTAL</code> | <code>5</code> | EXPERIMENTAL is like SPARE, except it can serve queries. This type can be used for usages not planned by Vitess, like online export to another storage engine.  |
| <code>BACKUP</code> | <code>6</code> | BACKUP is the type a server goes to when taking a backup. No queries can be served in BACKUP mode.  |
| <code>RESTORE</code> | <code>7</code> | RESTORE is the type a server uses when restoring a backup, at startup time.  No queries can be served in RESTORE mode.  |
| <code>DRAINED</code> | <code>8</code> | DRAINED is the type a server goes into when used by Vitess tools to perform an offline action. It is a serving type (as the tools processes may need to run queries), but it's not used to route queries from Vitess users. In this state, this tablet is dedicated to the process that uses it.  |

### vtrpc.ErrorCode

 ErrorCode is the enum values for Errors. Internally, errors should be created with one of these codes. These will then be translated over the wire by various RPC frameworks.

| Name |Value |Description |
| :-------- | :-------- | :-------- 
| <code>SUCCESS</code> | <code>0</code> | SUCCESS is returned from a successful call.  |
| <code>CANCELLED</code> | <code>1</code> | CANCELLED means that the context was cancelled (and noticed in the app layer, as opposed to the RPC layer).  |
| <code>UNKNOWN_ERROR</code> | <code>2</code> | UNKNOWN_ERROR includes: 1. MySQL error codes that we don't explicitly handle. 2. MySQL response that wasn't as expected. For example, we might expect a MySQL timestamp to be returned in a particular way, but it wasn't. 3. Anything else that doesn't fall into a different bucket.  |
| <code>BAD_INPUT</code> | <code>3</code> | BAD_INPUT is returned when an end-user either sends SQL that couldn't be parsed correctly, or tries a query that isn't supported by Vitess.  |
| <code>DEADLINE_EXCEEDED</code> | <code>4</code> | DEADLINE_EXCEEDED is returned when an action is taking longer than a given timeout.  |
| <code>INTEGRITY_ERROR</code> | <code>5</code> | INTEGRITY_ERROR is returned on integrity error from MySQL, usually due to duplicate primary keys.  |
| <code>PERMISSION_DENIED</code> | <code>6</code> | PERMISSION_DENIED errors are returned when a user requests access to something that they don't have permissions for.  |
| <code>RESOURCE_EXHAUSTED</code> | <code>7</code> | RESOURCE_EXHAUSTED is returned when a query exceeds its quota in some dimension and can't be completed due to that. Queries that return RESOURCE_EXHAUSTED should not be retried, as it could be detrimental to the server's health. Examples of errors that will cause the RESOURCE_EXHAUSTED code: 1. TxPoolFull: this is retried server-side, and is only returned as an error if the server-side retries failed. 2. Query is killed due to it taking too long.  |
| <code>QUERY_NOT_SERVED</code> | <code>8</code> | QUERY_NOT_SERVED means that a query could not be served right now. Client can interpret it as: "the tablet that you sent this query to cannot serve the query right now, try a different tablet or try again later." This could be due to various reasons: QueryService is not serving, should not be serving, wrong shard, wrong tablet type, blacklisted table, etc. Clients that receive this error should usually retry the query, but after taking the appropriate steps to make sure that the query will get sent to the correct tablet.  |
| <code>NOT_IN_TX</code> | <code>9</code> | NOT_IN_TX means that we're not currently in a transaction, but we should be.  |
| <code>INTERNAL_ERROR</code> | <code>10</code> | INTERNAL_ERRORs are problems that only the server can fix, not the client. These errors are not due to a query itself, but rather due to the state of the system. Generally, we don't expect the errors to go away by themselves, but they may go away after human intervention. Examples of scenarios where INTERNAL_ERROR is returned: 1. Something is not configured correctly internally. 2. A necessary resource is not available, and we don't expect it to become available by itself. 3. A sanity check fails. 4. Some other internal error occurs. Clients should not retry immediately, as there is little chance of success. However, it's acceptable for retries to happen internally, for example to multiple backends, in case only a subset of backend are not functional.  |
| <code>TRANSIENT_ERROR</code> | <code>11</code> | TRANSIENT_ERROR is used for when there is some error that we expect we can recover from automatically - often due to a resource limit temporarily being reached. Retrying this error, with an exponential backoff, should succeed. Clients should be able to successfully retry the query on the same backends. Examples of things that can trigger this error: 1. Query has been throttled 2. VtGate could have request backlog  |
| <code>UNAUTHENTICATED</code> | <code>12</code> | UNAUTHENTICATED errors are returned when a user requests access to something, and we're unable to verify the user's authentication.  |

## Messages

### BoundKeyspaceIdQuery

BoundKeyspaceIdQuery represents a single query request for the specified list of keyspace ids. This is used in a list for ExecuteBatchKeyspaceIdsRequest.

#### Properties

| Name |Description |
| :-------- | :-------- 
| <code>query</code> <br>[query.BoundQuery](#query.boundquery)| BoundQuery is a query with its bind variables |
| <code>keyspace</code> <br>string| keyspace to target the query to. |
| <code>keyspace_ids</code> <br>list &lt;bytes&gt;| keyspace_ids contains the list of keyspace_ids affected by this query. Will be used to find the shards to send the query to. |

### BoundShardQuery

BoundShardQuery represents a single query request for the specified list of shards. This is used in a list for ExecuteBatchShardsRequest.

#### Properties

| Name |Description |
| :-------- | :-------- 
| <code>query</code> <br>[query.BoundQuery](#query.boundquery)| BoundQuery is a query with its bind variables |
| <code>keyspace</code> <br>string| keyspace to target the query to. |
| <code>shards</code> <br>list &lt;string&gt;| shards to target the query to. A DML can only target one shard. |

### Session

Session objects are session cookies and are invalidated on use. Query results will contain updated session values. Their content should be opaque to the user.

#### Properties

| Name |Description |
| :-------- | :-------- 
| <code>in_transaction</code> <br>bool| |
| <code>shard_sessions</code> <br>list &lt;[ShardSession](#session.shardsession)&gt;| |
| <code>single_db</code> <br>bool| single_db specifies if the transaction should be restricted to a single database. |

#### Messages

##### Session.ShardSession

<em>Properties</em>

| Name |Description |
| :-------- | :-------- 
| <code>target</code> <br>[query.Target](#query.target)| Target describes what the client expects the tablet is. If the tablet does not match, an error is returned. |
| <code>transaction_id</code> <br>int64| |

### query.BindVariable

BindVariable represents a single bind variable in a Query.

#### Properties

| Name |Description |
| :-------- | :-------- 
| <code>type</code> <br>[Type](#query.type)| |
| <code>value</code> <br>bytes| |
| <code>values</code> <br>list &lt;[Value](#query.value)&gt;| Value represents a typed value. |

### query.BoundQuery

BoundQuery is a query with its bind variables

#### Properties

| Name |Description |
| :-------- | :-------- 
| <code>sql</code> <br>string| sql is the SQL query to execute |
| <code>bind_variables</code> <br>map &lt;string, [BindVariable](#query.bindvariable)&gt;| bind_variables is a map of all bind variables to expand in the query |

### query.EventToken

EventToken is a structure that describes a point in time in a replication stream on one shard. The most recent known replication position can be retrieved from vttablet when executing a query. It is also sent with the replication streams from the binlog service.

#### Properties

| Name |Description |
| :-------- | :-------- 
| <code>timestamp</code> <br>int64| timestamp is the MySQL timestamp of the statements. Seconds since Epoch. |
| <code>shard</code> <br>string| The shard name that applied the statements. Note this is not set when streaming from a vttablet. It is only used on the client -> vtgate link. |
| <code>position</code> <br>string| The position on the replication stream after this statement was applied. It is not the transaction ID / GTID, but the position / GTIDSet. |

### query.ExecuteOptions

ExecuteOptions is passed around for all Execute calls.

#### Properties

| Name |Description |
| :-------- | :-------- 
| <code>include_event_token</code> <br>bool| This used to be exclude_field_names, which was replaced by IncludedFields enum below If set, we will try to include an EventToken with the responses. |
| <code>compare_event_token</code> <br>[EventToken](#query.eventtoken)| EventToken is a structure that describes a point in time in a replication stream on one shard. The most recent known replication position can be retrieved from vttablet when executing a query. It is also sent with the replication streams from the binlog service. |
| <code>included_fields</code> <br>[IncludedFields](#executeoptions.includedfields)| Controls what fields are returned in Field message responses from mysql, i.e. field name, table name, etc. This is an optimization for high-QPS queries where the client knows what it's getting |

#### Enums

##### ExecuteOptions.IncludedFields

| Name |Value |Description |
| :-------- | :-------- | :-------- 
| <code>TYPE_AND_NAME</code> | <code>0</code> |  |
| <code>TYPE_ONLY</code> | <code>1</code> |  |
| <code>ALL</code> | <code>2</code> |  |

### query.Field

Field describes a single column returned by a query

#### Properties

| Name |Description |
| :-------- | :-------- 
| <code>name</code> <br>string| name of the field as returned by mysql C API |
| <code>type</code> <br>[Type](#query.type)| vitess-defined type. Conversion function is in sqltypes package. |
| <code>table</code> <br>string| Remaining fields from mysql C API. These fields are only populated when ExecuteOptions.included_fields is set to IncludedFields.ALL. |
| <code>org_table</code> <br>string| |
| <code>database</code> <br>string| |
| <code>org_name</code> <br>string| |
| <code>column_length</code> <br>uint32| column_length is really a uint32. All 32 bits can be used. |
| <code>charset</code> <br>uint32| charset is actually a uint16. Only the lower 16 bits are used. |
| <code>decimals</code> <br>uint32| decimals is actually a uint8. Only the lower 8 bits are used. |
| <code>flags</code> <br>uint32| flags is actually a uint16. Only the lower 16 bits are used. |

### query.QueryResult

QueryResult is returned by Execute and ExecuteStream.  As returned by Execute, len(fields) is always equal to len(row) (for each row in rows).  As returned by StreamExecute, the first QueryResult has the fields set, and subsequent QueryResult have rows set. And as Execute, len(QueryResult[0].fields) is always equal to len(row) (for each row in rows for each QueryResult in QueryResult[1:]).

#### Properties

| Name |Description |
| :-------- | :-------- 
| <code>fields</code> <br>list &lt;[Field](#query.field)&gt;| Field describes a single column returned by a query |
| <code>rows_affected</code> <br>uint64| |
| <code>insert_id</code> <br>uint64| |
| <code>rows</code> <br>list &lt;[Row](#query.row)&gt;| Row is a database row. |
| <code>extras</code> <br>[ResultExtras](#query.resultextras)| ResultExtras contains optional out-of-band information. Usually the extras are requested by adding ExecuteOptions flags. |

### query.ResultExtras

ResultExtras contains optional out-of-band information. Usually the extras are requested by adding ExecuteOptions flags.

#### Properties

| Name |Description |
| :-------- | :-------- 
| <code>event_token</code> <br>[EventToken](#query.eventtoken)| EventToken is a structure that describes a point in time in a replication stream on one shard. The most recent known replication position can be retrieved from vttablet when executing a query. It is also sent with the replication streams from the binlog service. |
| <code>fresher</code> <br>bool| If set, it means the data returned with this result is fresher than the compare_token passed in the ExecuteOptions. |

### query.ResultWithError

ResultWithError represents a query response in the form of result or error but not both.

#### Properties

| Name |Description |
| :-------- | :-------- 
| <code>error</code> <br>[vtrpc.RPCError](#vtrpc.rpcerror)| RPCError is an application-level error structure returned by VtTablet (and passed along by VtGate if appropriate). We use this so the clients don't have to parse the error messages, but instead can depend on the value of the code. |
| <code>result</code> <br>[query.QueryResult](#query.queryresult)| QueryResult is returned by Execute and ExecuteStream.  As returned by Execute, len(fields) is always equal to len(row) (for each row in rows).  As returned by StreamExecute, the first QueryResult has the fields set, and subsequent QueryResult have rows set. And as Execute, len(QueryResult[0].fields) is always equal to len(row) (for each row in rows for each QueryResult in QueryResult[1:]). |

### query.Row

Row is a database row.

#### Properties

| Name |Description |
| :-------- | :-------- 
| <code>lengths</code> <br>list &lt;sint64&gt;| lengths contains the length of each value in values. A length of -1 means that the field is NULL. While reading values, you have to accummulate the length to know the offset where the next value begins in values. |
| <code>values</code> <br>bytes| values contains a concatenation of all values in the row. |

### query.StreamEvent

StreamEvent describes a set of transformations that happened as a single transactional unit on a server. It is streamed back by the Update Stream calls.

#### Properties

| Name |Description |
| :-------- | :-------- 
| <code>statements</code> <br>list &lt;[Statement](#streamevent.statement)&gt;| The statements in this transaction. |
| <code>event_token</code> <br>[EventToken](#query.eventtoken)| EventToken is a structure that describes a point in time in a replication stream on one shard. The most recent known replication position can be retrieved from vttablet when executing a query. It is also sent with the replication streams from the binlog service. |

#### Messages

##### StreamEvent.Statement

One individual Statement in a transaction.

<em>Properties</em>

| Name |Description |
| :-------- | :-------- 
| <code>category</code> <br>[Category](#streamevent.statement.category)| |
| <code>table_name</code> <br>string| table_name, primary_key_fields and primary_key_values are set for DML. |
| <code>primary_key_fields</code> <br>list &lt;[Field](#query.field)&gt;| Field describes a single column returned by a query |
| <code>primary_key_values</code> <br>list &lt;[Row](#query.row)&gt;| Row is a database row. |
| <code>sql</code> <br>bytes| sql is set for all queries. FIXME(alainjobart) we may not need it for DMLs. |

#### Enums

##### StreamEvent.Statement.Category

 One individual Statement in a transaction. The category of one statement.

| Name |Value |Description |
| :-------- | :-------- | :-------- 
| <code>Error</code> | <code>0</code> |  |
| <code>DML</code> | <code>1</code> |  |
| <code>DDL</code> | <code>2</code> |  |

### query.Target

Target describes what the client expects the tablet is. If the tablet does not match, an error is returned.

#### Properties

| Name |Description |
| :-------- | :-------- 
| <code>keyspace</code> <br>string| |
| <code>shard</code> <br>string| |
| <code>tablet_type</code> <br>[topodata.TabletType](#topodata.tablettype)| TabletType represents the type of a given tablet. |

### query.Value

Value represents a typed value.

#### Properties

| Name |Description |
| :-------- | :-------- 
| <code>type</code> <br>[Type](#query.type)| |
| <code>value</code> <br>bytes| |

### topodata.KeyRange

KeyRange describes a range of sharding keys, when range-based sharding is used.

#### Properties

| Name |Description |
| :-------- | :-------- 
| <code>start</code> <br>bytes| |
| <code>end</code> <br>bytes| |

### topodata.ShardReference

ShardReference is used as a pointer from a SrvKeyspace to a Shard

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

#### Messages

##### SrvKeyspace.KeyspacePartition

<em>Properties</em>

| Name |Description |
| :-------- | :-------- 
| <code>served_type</code> <br>[TabletType](#topodata.tablettype)| The type this partition applies to. |
| <code>shard_references</code> <br>list &lt;[ShardReference](#topodata.shardreference)&gt;| ShardReference is used as a pointer from a SrvKeyspace to a Shard |

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

