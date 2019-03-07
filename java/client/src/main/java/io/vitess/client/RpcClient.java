/*
 * Copyright 2017 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.vitess.client;

import com.google.common.util.concurrent.ListenableFuture;

import io.vitess.proto.Query.QueryResult;
import io.vitess.proto.Vtgate;
import io.vitess.proto.Vtgate.BeginRequest;
import io.vitess.proto.Vtgate.BeginResponse;
import io.vitess.proto.Vtgate.CommitRequest;
import io.vitess.proto.Vtgate.CommitResponse;
import io.vitess.proto.Vtgate.ExecuteBatchKeyspaceIdsRequest;
import io.vitess.proto.Vtgate.ExecuteBatchKeyspaceIdsResponse;
import io.vitess.proto.Vtgate.ExecuteBatchShardsRequest;
import io.vitess.proto.Vtgate.ExecuteBatchShardsResponse;
import io.vitess.proto.Vtgate.ExecuteEntityIdsRequest;
import io.vitess.proto.Vtgate.ExecuteEntityIdsResponse;
import io.vitess.proto.Vtgate.ExecuteKeyRangesRequest;
import io.vitess.proto.Vtgate.ExecuteKeyRangesResponse;
import io.vitess.proto.Vtgate.ExecuteKeyspaceIdsRequest;
import io.vitess.proto.Vtgate.ExecuteKeyspaceIdsResponse;
import io.vitess.proto.Vtgate.ExecuteRequest;
import io.vitess.proto.Vtgate.ExecuteResponse;
import io.vitess.proto.Vtgate.ExecuteShardsRequest;
import io.vitess.proto.Vtgate.ExecuteShardsResponse;
import io.vitess.proto.Vtgate.GetSrvKeyspaceRequest;
import io.vitess.proto.Vtgate.GetSrvKeyspaceResponse;
import io.vitess.proto.Vtgate.RollbackRequest;
import io.vitess.proto.Vtgate.RollbackResponse;
import io.vitess.proto.Vtgate.SplitQueryRequest;
import io.vitess.proto.Vtgate.SplitQueryResponse;
import io.vitess.proto.Vtgate.StreamExecuteKeyRangesRequest;
import io.vitess.proto.Vtgate.StreamExecuteKeyspaceIdsRequest;
import io.vitess.proto.Vtgate.StreamExecuteRequest;
import io.vitess.proto.Vtgate.StreamExecuteShardsRequest;

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
   * <a href="https://github.com/vitessio/vitess/blob/master/proto/vtgateservice.proto">proto</a>
   * definition for canonical documentation on this VTGate API.
   */
  ListenableFuture<ExecuteResponse> execute(Context ctx, ExecuteRequest request)
      throws SQLException;

  /**
   * Sends a single query to a set of shards.
   *
   * <p>See the
   * <a href="https://github.com/vitessio/vitess/blob/master/proto/vtgateservice.proto">proto</a>
   * definition for canonical documentation on this VTGate API.
   */
  ListenableFuture<ExecuteShardsResponse> executeShards(Context ctx, ExecuteShardsRequest request)
      throws SQLException;

  /**
   * Sends a query with a set of keyspace IDs.
   *
   * <p>See the
   * <a href="https://github.com/vitessio/vitess/blob/master/proto/vtgateservice.proto">proto</a>
   * definition for canonical documentation on this VTGate API.
   */
  ListenableFuture<ExecuteKeyspaceIdsResponse> executeKeyspaceIds(
      Context ctx, ExecuteKeyspaceIdsRequest request) throws SQLException;

  /**
   * Sends a query with a set of key ranges.
   *
   * <p>See the
   * <a href="https://github.com/vitessio/vitess/blob/master/proto/vtgateservice.proto">proto</a>
   * definition for canonical documentation on this VTGate API.
   */
  ListenableFuture<ExecuteKeyRangesResponse> executeKeyRanges(
      Context ctx, ExecuteKeyRangesRequest request) throws SQLException;

  /**
   * Sends a query with a set of entity IDs.
   *
   * <p>See the
   * <a href="https://github.com/vitessio/vitess/blob/master/proto/vtgateservice.proto">proto</a>
   * definition for canonical documentation on this VTGate API.
   */
  ListenableFuture<ExecuteEntityIdsResponse> executeEntityIds(
      Context ctx, ExecuteEntityIdsRequest request) throws SQLException;

  /**
   * Sends a list of queries using the VTGate V3 API.
   *
   * <p>See the
   * <a href="https://github.com/vitessio/vitess/blob/master/proto/vtgateservice.proto">proto</a>
   * definition for canonical documentation on this VTGate API.
   */
  ListenableFuture<Vtgate.ExecuteBatchResponse> executeBatch(Context ctx,
      Vtgate.ExecuteBatchRequest request)
      throws SQLException;

  /**
   * Sends a list of queries to a set of shards.
   *
   * <p>See the
   * <a href="https://github.com/vitessio/vitess/blob/master/proto/vtgateservice.proto">proto</a>
   * definition for canonical documentation on this VTGate API.
   */
  ListenableFuture<ExecuteBatchShardsResponse> executeBatchShards(
      Context ctx, ExecuteBatchShardsRequest request) throws SQLException;

  /**
   * Sends a list of queries with keyspace ids as bind variables.
   *
   * <p>See the
   * <a href="https://github.com/vitessio/vitess/blob/master/proto/vtgateservice.proto">proto</a>
   * definition for canonical documentation on this VTGate API.
   */
  ListenableFuture<ExecuteBatchKeyspaceIdsResponse> executeBatchKeyspaceIds(
      Context ctx, ExecuteBatchKeyspaceIdsRequest request) throws SQLException;

  /**
   * Starts stream queries with the VTGate V3 API.
   *
   * <p>Note: Streaming queries are not asynchronous, because they typically shouldn't
   * be used from a latency-critical serving path anyway. This method will return as soon as the
   * request is initiated, but StreamIterator methods will block until the next chunk of results is
   * received from the server.
   *
   * <p>See the
   * <a href="https://github.com/vitessio/vitess/blob/master/proto/vtgateservice.proto">proto</a>
   * definition for canonical documentation on this VTGate API.
   */
  StreamIterator<QueryResult> streamExecute(Context ctx, StreamExecuteRequest request)
      throws SQLException;

  /**
   * Starts stream queries with multiple shards.
   *
   * <p>Note: Streaming queries are not asynchronous, because they typically shouldn't
   * be used from a latency-critical serving path anyway. This method will return as soon as the
   * request is initiated, but StreamIterator methods will block until the next chunk of results is
   * received from the server.
   *
   * <p>See the
   * <a href="https://github.com/vitessio/vitess/blob/master/proto/vtgateservice.proto">proto</a>
   * definition for canonical documentation on this VTGate API.
   */
  StreamIterator<QueryResult> streamExecuteShards(Context ctx, StreamExecuteShardsRequest request)
      throws SQLException;

  /**
   * Starts a list of stream queries with keyspace ids as bind variables.
   *
   * <p>Note: Streaming queries are not asynchronous, because they typically shouldn't
   * be used from a latency-critical serving path anyway. This method will return as soon as the
   * request is initiated, but StreamIterator methods will block until the next chunk of results is
   * received from the server.
   *
   * <p>See the
   * <a href="https://github.com/vitessio/vitess/blob/master/proto/vtgateservice.proto">proto</a>
   * definition for canonical documentation on this VTGate API.
   */
  StreamIterator<QueryResult> streamExecuteKeyspaceIds(
      Context ctx, StreamExecuteKeyspaceIdsRequest request) throws SQLException;

  /**
   * Starts stream query with a set of key ranges.
   *
   * <p>Note: Streaming queries are not asynchronous, because they typically shouldn't
   * be used from a latency-critical serving path anyway. This method will return as soon as the
   * request is initiated, but StreamIterator methods will block until the next chunk of results is
   * received from the server.
   *
   * <p>See the
   * <a href="https://github.com/vitessio/vitess/blob/master/proto/vtgateservice.proto">proto</a>
   * definition for canonical documentation on this VTGate API.
   */
  StreamIterator<QueryResult> streamExecuteKeyRanges(
      Context ctx, StreamExecuteKeyRangesRequest request) throws SQLException;

  /**
   * Starts a transaction.
   *
   * <p>See the
   * <a href="https://github.com/vitessio/vitess/blob/master/proto/vtgateservice.proto">proto</a>
   * definition for canonical documentation on this VTGate API.
   */
  ListenableFuture<BeginResponse> begin(Context ctx, BeginRequest request) throws SQLException;

  /**
   * Commits a transaction.
   *
   * <p>See the
   * <a href="https://github.com/vitessio/vitess/blob/master/proto/vtgateservice.proto">proto</a>
   * definition for canonical documentation on this VTGate API.
   */
  ListenableFuture<CommitResponse> commit(Context ctx, CommitRequest request) throws SQLException;

  /**
   * Rolls back a pending transaction.
   *
   * <p>See the
   * <a href="https://github.com/vitessio/vitess/blob/master/proto/vtgateservice.proto">proto</a>
   * definition for canonical documentation on this VTGate API.
   */
  ListenableFuture<RollbackResponse> rollback(Context ctx, RollbackRequest request)
      throws SQLException;

  /**
   * Splits a query into smaller queries.
   *
   * <p>See the
   * <a href="https://github.com/vitessio/vitess/blob/master/proto/vtgateservice.proto">proto</a>
   * definition for canonical documentation on this VTGate API.
   */
  ListenableFuture<SplitQueryResponse> splitQuery(Context ctx, SplitQueryRequest request)
      throws SQLException;

  /**
   * Returns a list of serving keyspaces.
   *
   * <p>See the
   * <a href="https://github.com/vitessio/vitess/blob/master/proto/vtgateservice.proto">proto</a>
   * definition for canonical documentation on this VTGate API.
   */
  ListenableFuture<GetSrvKeyspaceResponse> getSrvKeyspace(
      Context ctx, GetSrvKeyspaceRequest request) throws SQLException;
}
