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
import java.sql.SQLException;

/**
 * RpcClient defines a set of methods to communicate with VTGates.
 */
public interface RpcClient extends Closeable {
  // execute sends a single query using the VTGate V3 API.
  ExecuteResponse execute(Context ctx, ExecuteRequest request) throws SQLException;

  // executeShards sends a single query to a set of shards.
  ExecuteShardsResponse executeShards(Context ctx, ExecuteShardsRequest request)
      throws SQLException;

  // executeKeyspaceIds sends a query with a set of keyspace IDs.
  ExecuteKeyspaceIdsResponse executeKeyspaceIds(Context ctx, ExecuteKeyspaceIdsRequest request)
      throws SQLException;

  // executeKeyRanges sends a query with a set of key ranges.
  ExecuteKeyRangesResponse executeKeyRanges(Context ctx, ExecuteKeyRangesRequest request)
      throws SQLException;

  // executeEntityIds sends a query with a set of entity IDs.
  ExecuteEntityIdsResponse executeEntityIds(Context ctx, ExecuteEntityIdsRequest request)
      throws SQLException;

  // executeBatchShards sends a list of queries to a set of shards.
  ExecuteBatchShardsResponse executeBatchShards(Context ctx, ExecuteBatchShardsRequest request)
      throws SQLException;

  // executeBatchKeyspaceIds sends a list of queries with keyspace ids as bind variables.
  ExecuteBatchKeyspaceIdsResponse executeBatchKeyspaceIds(
      Context ctx, ExecuteBatchKeyspaceIdsRequest request) throws SQLException;

  // streamExecute starts stream queries with the VTGate V3 API.
  StreamIterator<QueryResult> streamExecute(Context ctx, StreamExecuteRequest request)
      throws SQLException;

  // streamExecuteShard starts stream queries with multiple shards.
  StreamIterator<QueryResult> streamExecuteShards(Context ctx, StreamExecuteShardsRequest request)
      throws SQLException;

  // streamExecuteKeyspaceIds starts a list of stream queries with keyspace ids as bind variables.
  StreamIterator<QueryResult> streamExecuteKeyspaceIds(
      Context ctx, StreamExecuteKeyspaceIdsRequest request) throws SQLException;

  // streamExecuteKeyRanges starts stream query with a set of key ranges.
  StreamIterator<QueryResult> streamExecuteKeyRanges(
      Context ctx, StreamExecuteKeyRangesRequest request) throws SQLException;

  // begin starts a transaction.
  BeginResponse begin(Context ctx, BeginRequest request) throws SQLException;

  // commit commits a transaction.
  CommitResponse commit(Context ctx, CommitRequest request) throws SQLException;

  // rollback rolls back a pending transaction.
  RollbackResponse rollback(Context ctx, RollbackRequest request) throws SQLException;

  // splitQuery splits a query into smaller queries.
  SplitQueryResponse splitQuery(Context ctx, SplitQueryRequest request) throws SQLException;

  // getSrvKeyspace returns a list of serving keyspaces.
  GetSrvKeyspaceResponse getSrvKeyspace(Context ctx, GetSrvKeyspaceRequest request)
      throws SQLException;
}
