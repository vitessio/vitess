package com.youtube.vitess.client;

import com.youtube.vitess.proto.Query.QueryResult;
import com.youtube.vitess.proto.Vtgate.BeginRequest;
import com.youtube.vitess.proto.Vtgate.BeginResponse;
import com.youtube.vitess.proto.Vtgate.CommitRequest;
import com.youtube.vitess.proto.Vtgate.CommitResponse;
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
import com.youtube.vitess.proto.Vtgate.GetSrvKeyspaceRequest;
import com.youtube.vitess.proto.Vtgate.GetSrvKeyspaceResponse;
import com.youtube.vitess.proto.Vtgate.RollbackRequest;
import com.youtube.vitess.proto.Vtgate.RollbackResponse;
import com.youtube.vitess.proto.Vtgate.SplitQueryRequest;
import com.youtube.vitess.proto.Vtgate.SplitQueryResponse;
import com.youtube.vitess.proto.Vtgate.StreamExecuteKeyRangesRequest;
import com.youtube.vitess.proto.Vtgate.StreamExecuteKeyspaceIdsRequest;
import com.youtube.vitess.proto.Vtgate.StreamExecuteRequest;
import com.youtube.vitess.proto.Vtgate.StreamExecuteShardsRequest;

import java.io.Closeable;

/**
 * RpcClient defines a set of methods to communicate with VTGates.
 */
public interface RpcClient extends Closeable {
  // execute sends a single query using the VTGate V3 API.
  ExecuteResponse execute(Context ctx, ExecuteRequest request)
      throws VitessException, VitessRpcException;

  // executeShards sends a single query to a set of shards.
  ExecuteShardsResponse executeShards(Context ctx, ExecuteShardsRequest request)
      throws VitessException, VitessRpcException;

  // executeKeyspaceIds sends a query with a set of keyspace IDs.
  ExecuteKeyspaceIdsResponse executeKeyspaceIds(Context ctx, ExecuteKeyspaceIdsRequest request)
      throws VitessException, VitessRpcException;

  // executeKeyRanges sends a query with a set of key ranges.
  ExecuteKeyRangesResponse executeKeyRanges(Context ctx, ExecuteKeyRangesRequest request)
      throws VitessException, VitessRpcException;

  // executeEntityIds sends a query with a set of entity IDs.
  ExecuteEntityIdsResponse executeEntityIds(Context ctx, ExecuteEntityIdsRequest request)
      throws VitessException, VitessRpcException;

  // executeBatchShards sends a list of queries to a set of shards.
  ExecuteBatchShardsResponse executeBatchShards(Context ctx, ExecuteBatchShardsRequest request)
      throws VitessException, VitessRpcException;

  // executeBatchKeyspaceIds sends a list of queries with keyspace ids as bind variables.
  ExecuteBatchKeyspaceIdsResponse executeBatchKeyspaceIds(
      Context ctx, ExecuteBatchKeyspaceIdsRequest request)
      throws VitessException, VitessRpcException;

  // streamExecute starts stream queries with the VTGate V3 API.
  StreamIterator<QueryResult> streamExecute(Context ctx, StreamExecuteRequest request)
      throws VitessRpcException;

  // streamExecuteShard starts stream queries with multiple shards.
  StreamIterator<QueryResult> streamExecuteShards(Context ctx, StreamExecuteShardsRequest request)
      throws VitessRpcException;

  // streamExecuteKeyspaceIds starts a list of stream queries with keyspace ids as bind variables.
  StreamIterator<QueryResult> streamExecuteKeyspaceIds(
      Context ctx, StreamExecuteKeyspaceIdsRequest request) throws VitessRpcException;

  // streamExecuteKeyRanges starts stream query with a set of key ranges.
  StreamIterator<QueryResult> streamExecuteKeyRanges(
      Context ctx, StreamExecuteKeyRangesRequest request) throws VitessRpcException;

  // begin starts a transaction.
  BeginResponse begin(Context ctx, BeginRequest request) throws VitessException, VitessRpcException;

  // commit commits a transaction.
  CommitResponse commit(Context ctx, CommitRequest request)
      throws VitessException, VitessRpcException;

  // rollback rolls back a pending transaction.
  RollbackResponse rollback(Context ctx, RollbackRequest request)
      throws VitessException, VitessRpcException;

  // splitQuery splits a query into smaller queries.
  SplitQueryResponse splitQuery(Context ctx, SplitQueryRequest request)
      throws VitessException, VitessRpcException;

  // getSrvKeyspace returns a list of serving keyspaces.
  GetSrvKeyspaceResponse getSrvKeyspace(Context ctx, GetSrvKeyspaceRequest request)
      throws VitessException, VitessRpcException;
}
