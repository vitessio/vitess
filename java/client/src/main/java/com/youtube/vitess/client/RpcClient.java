package com.youtube.vitess.client;

import com.google.common.util.concurrent.ListenableFuture;

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
  /**
   * Sends a single query using the VTGate V3 API.
   *
   * <p>See the
   * <a href="https://github.com/youtube/vitess/blob/master/proto/vtgateservice.proto">proto</a>
   * definition for canonical documentation on this VTGate API.
   */
  ListenableFuture<ExecuteResponse> execute(Context ctx, ExecuteRequest request)
      throws SQLException;

  /**
   * Sends a single query to a set of shards.
   *
   * <p>See the
   * <a href="https://github.com/youtube/vitess/blob/master/proto/vtgateservice.proto">proto</a>
   * definition for canonical documentation on this VTGate API.
   */
  ListenableFuture<ExecuteShardsResponse> executeShards(Context ctx, ExecuteShardsRequest request)
      throws SQLException;

  /**
   * Sends a query with a set of keyspace IDs.
   *
   * <p>See the
   * <a href="https://github.com/youtube/vitess/blob/master/proto/vtgateservice.proto">proto</a>
   * definition for canonical documentation on this VTGate API.
   */
  ListenableFuture<ExecuteKeyspaceIdsResponse> executeKeyspaceIds(
      Context ctx, ExecuteKeyspaceIdsRequest request) throws SQLException;

  /**
   * Sends a query with a set of key ranges.
   *
   * <p>See the
   * <a href="https://github.com/youtube/vitess/blob/master/proto/vtgateservice.proto">proto</a>
   * definition for canonical documentation on this VTGate API.
   */
  ListenableFuture<ExecuteKeyRangesResponse> executeKeyRanges(
      Context ctx, ExecuteKeyRangesRequest request) throws SQLException;

  /**
   * Sends a query with a set of entity IDs.
   *
   * <p>See the
   * <a href="https://github.com/youtube/vitess/blob/master/proto/vtgateservice.proto">proto</a>
   * definition for canonical documentation on this VTGate API.
   */
  ListenableFuture<ExecuteEntityIdsResponse> executeEntityIds(
      Context ctx, ExecuteEntityIdsRequest request) throws SQLException;

  /**
   * Sends a list of queries to a set of shards.
   *
   * <p>See the
   * <a href="https://github.com/youtube/vitess/blob/master/proto/vtgateservice.proto">proto</a>
   * definition for canonical documentation on this VTGate API.
   */
  ListenableFuture<ExecuteBatchShardsResponse> executeBatchShards(
      Context ctx, ExecuteBatchShardsRequest request) throws SQLException;

  /**
   * Sends a list of queries with keyspace ids as bind variables.
   *
   * <p>See the
   * <a href="https://github.com/youtube/vitess/blob/master/proto/vtgateservice.proto">proto</a>
   * definition for canonical documentation on this VTGate API.
   */
  ListenableFuture<ExecuteBatchKeyspaceIdsResponse> executeBatchKeyspaceIds(
      Context ctx, ExecuteBatchKeyspaceIdsRequest request) throws SQLException;

  /**
   * Starts stream queries with the VTGate V3 API.
   *
   * <p>Note: Streaming queries are not asynchronous, because they typically shouldn't
   * be used from a latency-critical serving path anyway. This method will return as
   * soon as the request is initiated, but StreamIterator methods will block until the
   * next chunk of results is received from the server.
   *
   * <p>See the
   * <a href="https://github.com/youtube/vitess/blob/master/proto/vtgateservice.proto">proto</a>
   * definition for canonical documentation on this VTGate API.
   */
  StreamIterator<QueryResult> streamExecute(Context ctx, StreamExecuteRequest request)
      throws SQLException;

  /**
   * Starts stream queries with multiple shards.
   *
   * <p>Note: Streaming queries are not asynchronous, because they typically shouldn't
   * be used from a latency-critical serving path anyway. This method will return as
   * soon as the request is initiated, but StreamIterator methods will block until the
   * next chunk of results is received from the server.
   *
   * <p>See the
   * <a href="https://github.com/youtube/vitess/blob/master/proto/vtgateservice.proto">proto</a>
   * definition for canonical documentation on this VTGate API.
   */
  StreamIterator<QueryResult> streamExecuteShards(Context ctx, StreamExecuteShardsRequest request)
      throws SQLException;

  /**
   * Starts a list of stream queries with keyspace ids as bind variables.
   *
   * <p>Note: Streaming queries are not asynchronous, because they typically shouldn't
   * be used from a latency-critical serving path anyway. This method will return as
   * soon as the request is initiated, but StreamIterator methods will block until the
   * next chunk of results is received from the server.
   *
   * <p>See the
   * <a href="https://github.com/youtube/vitess/blob/master/proto/vtgateservice.proto">proto</a>
   * definition for canonical documentation on this VTGate API.
   */
  StreamIterator<QueryResult> streamExecuteKeyspaceIds(
      Context ctx, StreamExecuteKeyspaceIdsRequest request) throws SQLException;

  /**
   * Starts stream query with a set of key ranges.
   *
   * <p>Note: Streaming queries are not asynchronous, because they typically shouldn't
   * be used from a latency-critical serving path anyway. This method will return as
   * soon as the request is initiated, but StreamIterator methods will block until the
   * next chunk of results is received from the server.
   *
   * <p>See the
   * <a href="https://github.com/youtube/vitess/blob/master/proto/vtgateservice.proto">proto</a>
   * definition for canonical documentation on this VTGate API.
   */
  StreamIterator<QueryResult> streamExecuteKeyRanges(
      Context ctx, StreamExecuteKeyRangesRequest request) throws SQLException;

  /**
   * Starts a transaction.
   *
   * <p>See the
   * <a href="https://github.com/youtube/vitess/blob/master/proto/vtgateservice.proto">proto</a>
   * definition for canonical documentation on this VTGate API.
   */
  ListenableFuture<BeginResponse> begin(Context ctx, BeginRequest request) throws SQLException;

  /**
   * Commits a transaction.
   *
   * <p>See the
   * <a href="https://github.com/youtube/vitess/blob/master/proto/vtgateservice.proto">proto</a>
   * definition for canonical documentation on this VTGate API.
   */
  ListenableFuture<CommitResponse> commit(Context ctx, CommitRequest request) throws SQLException;

  /**
   * Rolls back a pending transaction.
   *
   * <p>See the
   * <a href="https://github.com/youtube/vitess/blob/master/proto/vtgateservice.proto">proto</a>
   * definition for canonical documentation on this VTGate API.
   */
  ListenableFuture<RollbackResponse> rollback(Context ctx, RollbackRequest request)
      throws SQLException;

  /**
   * Splits a query into smaller queries.
   *
   * <p>See the
   * <a href="https://github.com/youtube/vitess/blob/master/proto/vtgateservice.proto">proto</a>
   * definition for canonical documentation on this VTGate API.
   */
  ListenableFuture<SplitQueryResponse> splitQuery(Context ctx, SplitQueryRequest request)
      throws SQLException;

  /**
   * Returns a list of serving keyspaces.
   *
   * <p>See the
   * <a href="https://github.com/youtube/vitess/blob/master/proto/vtgateservice.proto">proto</a>
   * definition for canonical documentation on this VTGate API.
   */
  ListenableFuture<GetSrvKeyspaceResponse> getSrvKeyspace(
      Context ctx, GetSrvKeyspaceRequest request) throws SQLException;
}
