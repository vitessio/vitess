package com.youtube.vitess.client;

import com.google.common.collect.Iterables;

import com.youtube.vitess.proto.Query.QueryResult;
import com.youtube.vitess.proto.Topodata.KeyRange;
import com.youtube.vitess.proto.Topodata.TabletType;
import com.youtube.vitess.proto.Vtgate.BoundKeyspaceIdQuery;
import com.youtube.vitess.proto.Vtgate.BoundShardQuery;
import com.youtube.vitess.proto.Vtgate.CommitRequest;
import com.youtube.vitess.proto.Vtgate.ExecuteBatchKeyspaceIdsRequest;
import com.youtube.vitess.proto.Vtgate.ExecuteBatchKeyspaceIdsResponse;
import com.youtube.vitess.proto.Vtgate.ExecuteBatchShardsRequest;
import com.youtube.vitess.proto.Vtgate.ExecuteBatchShardsResponse;
import com.youtube.vitess.proto.Vtgate.ExecuteEntityIdsRequest;
import com.youtube.vitess.proto.Vtgate.ExecuteEntityIdsResponse;
import com.youtube.vitess.proto.Vtgate.ExecuteKeyRangesRequest;
import com.youtube.vitess.proto.Vtgate.ExecuteKeyRangesResponse;
import com.youtube.vitess.proto.Vtgate.ExecuteKeyspaceIdsRequest;
import com.youtube.vitess.proto.Vtgate.ExecuteKeyspaceIdsResponse;
import com.youtube.vitess.proto.Vtgate.ExecuteRequest;
import com.youtube.vitess.proto.Vtgate.ExecuteResponse;
import com.youtube.vitess.proto.Vtgate.ExecuteShardsRequest;
import com.youtube.vitess.proto.Vtgate.ExecuteShardsResponse;
import com.youtube.vitess.proto.Vtgate.RollbackRequest;
import com.youtube.vitess.proto.Vtgate.Session;

import java.util.List;
import java.util.Map;

/**
 * VTGateTx manages a pending transaction.
 */
public class VTGateTx {
  private RpcClient client;
  private Session session;

  private VTGateTx(RpcClient client, Session session) {
    this.client = client;
    this.session = session;
  }

  public static VTGateTx withRpcClientAndSession(RpcClient client, Session session) {
    return new VTGateTx(client, session);
  }

  public QueryResult execute(
      Context ctx, String query, Map<String, ?> bindVars, TabletType tabletType)
      throws VitessException, VitessRpcException, VitessNotInTransactionException {
    if (this.session == null) {
      throw new VitessNotInTransactionException("execute: not in transaction");
    }
    ExecuteRequest request =
        ExecuteRequest.newBuilder()
            .setCallerId(ctx.getCallerId())
            .setQuery(Proto.bindQuery(query, bindVars))
            .setTabletType(tabletType)
            .setSession(this.session)
            .build();
    ExecuteResponse response = this.client.execute(ctx, request);
    this.session = response.getSession();
    Proto.checkError(response.getError());
    return response.getResult();
  }

  public QueryResult executeShards(Context ctx, String query, String keyspace,
      Iterable<String> shards, Map<String, ?> bindVars, TabletType tabletType)
      throws VitessException, VitessRpcException, VitessNotInTransactionException {
    if (this.session == null) {
      throw new VitessNotInTransactionException("executeShards: not in transaction");
    }
    ExecuteShardsRequest request =
        ExecuteShardsRequest.newBuilder()
            .setCallerId(ctx.getCallerId())
            .setQuery(Proto.bindQuery(query, bindVars))
            .setKeyspace(keyspace)
            .addAllShards(shards)
            .setTabletType(tabletType)
            .setSession(this.session)
            .build();
    ExecuteShardsResponse response = this.client.executeShards(ctx, request);
    this.session = response.getSession();
    Proto.checkError(response.getError());
    return response.getResult();
  }

  public QueryResult executeKeyspaceIds(Context ctx, String query, String keyspace,
      Iterable<byte[]> keyspaceIds, Map<String, ?> bindVars, TabletType tabletType)
      throws VitessException, VitessRpcException, VitessNotInTransactionException {
    if (this.session == null) {
      throw new VitessNotInTransactionException("executeKeyspaceIds: not in transaction");
    }
    ExecuteKeyspaceIdsRequest request =
        ExecuteKeyspaceIdsRequest.newBuilder()
            .setCallerId(ctx.getCallerId())
            .setQuery(Proto.bindQuery(query, bindVars))
            .setKeyspace(keyspace)
            .addAllKeyspaceIds(Iterables.transform(keyspaceIds, Proto.BYTE_ARRAY_TO_BYTE_STRING))
            .setTabletType(tabletType)
            .setSession(this.session)
            .build();
    ExecuteKeyspaceIdsResponse response = this.client.executeKeyspaceIds(ctx, request);
    this.session = response.getSession();
    Proto.checkError(response.getError());
    return response.getResult();
  }

  public QueryResult executeKeyRanges(Context ctx, String query, String keyspace,
      Iterable<? extends KeyRange> keyRanges, Map<String, ?> bindVars, TabletType tabletType)
      throws VitessException, VitessRpcException, VitessNotInTransactionException {
    if (this.session == null) {
      throw new VitessNotInTransactionException("executeKeyRanges: not in transaction");
    }
    ExecuteKeyRangesRequest request =
        ExecuteKeyRangesRequest.newBuilder()
            .setCallerId(ctx.getCallerId())
            .setQuery(Proto.bindQuery(query, bindVars))
            .setKeyspace(keyspace)
            .addAllKeyRanges(keyRanges)
            .setTabletType(tabletType)
            .setSession(this.session)
            .build();
    ExecuteKeyRangesResponse response = this.client.executeKeyRanges(ctx, request);
    this.session = response.getSession();
    Proto.checkError(response.getError());
    return response.getResult();
  }

  public QueryResult executeEntityIds(Context ctx, String query, String keyspace,
      String entityColumnName, Iterable<?> entityIds, Map<String, ?> bindVars,
      TabletType tabletType)
      throws VitessException, VitessRpcException, VitessNotInTransactionException {
    if (this.session == null) {
      throw new VitessNotInTransactionException("executeEntityIds: not in transaction");
    }
    ExecuteEntityIdsRequest request =
        ExecuteEntityIdsRequest.newBuilder()
            .setCallerId(ctx.getCallerId())
            .setQuery(Proto.bindQuery(query, bindVars))
            .setKeyspace(keyspace)
            .setEntityColumnName(entityColumnName)
            .addAllEntityKeyspaceIds(Iterables.transform(entityIds, Proto.OBJECT_TO_ENTITY_ID))
            .setTabletType(tabletType)
            .setSession(this.session)
            .build();
    ExecuteEntityIdsResponse response = this.client.executeEntityIds(ctx, request);
    this.session = response.getSession();
    Proto.checkError(response.getError());
    return response.getResult();
  }

  public List<QueryResult> executeBatchShards(Context ctx,
      Iterable<? extends BoundShardQuery> queries, TabletType tabletType, boolean asTransaction)
      throws VitessException, VitessRpcException, VitessNotInTransactionException {
    if (this.session == null) {
      throw new VitessNotInTransactionException("executeBatchShards: not in transaction");
    }
    ExecuteBatchShardsRequest request =
        ExecuteBatchShardsRequest.newBuilder()
            .setCallerId(ctx.getCallerId())
            .addAllQueries(queries)
            .setTabletType(tabletType)
            .setAsTransaction(asTransaction)
            .setSession(this.session)
            .build();
    ExecuteBatchShardsResponse response = this.client.executeBatchShards(ctx, request);
    this.session = response.getSession();
    Proto.checkError(response.getError());
    return response.getResultsList();
  }

  public List<QueryResult> executeBatchKeyspaceIds(Context ctx,
      Iterable<? extends BoundKeyspaceIdQuery> queries, TabletType tabletType,
      boolean asTransaction)
      throws VitessException, VitessRpcException, VitessNotInTransactionException {
    if (this.session == null) {
      throw new VitessNotInTransactionException("executeBatchKeyspaceIds: not in transaction");
    }
    ExecuteBatchKeyspaceIdsRequest request =
        ExecuteBatchKeyspaceIdsRequest.newBuilder()
            .setCallerId(ctx.getCallerId())
            .addAllQueries(queries)
            .setTabletType(tabletType)
            .setAsTransaction(asTransaction)
            .setSession(this.session)
            .build();
    ExecuteBatchKeyspaceIdsResponse response = this.client.executeBatchKeyspaceIds(ctx, request);
    this.session = response.getSession();
    Proto.checkError(response.getError());
    return response.getResultsList();
  }

  public void commit(Context ctx)
      throws VitessException, VitessRpcException, VitessNotInTransactionException {
    if (this.session == null) {
      throw new VitessNotInTransactionException("commit: not in transaction");
    }
    CommitRequest request =
        CommitRequest.newBuilder().setCallerId(ctx.getCallerId()).setSession(this.session).build();
    this.client.commit(ctx, request);
    this.session = null;
  }

  public void rollback(Context ctx)
      throws VitessException, VitessRpcException, VitessNotInTransactionException {
    if (this.session == null) {
      throw new VitessNotInTransactionException("rollback: not in transaction");
    }
    RollbackRequest request =
        RollbackRequest.newBuilder()
            .setCallerId(ctx.getCallerId())
            .setSession(this.session)
            .build();
    this.client.rollback(ctx, request);
    this.session = null;
  }
}
