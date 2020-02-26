/*
 * Copyright 2019 The Vitess Authors.

 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

 *     http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.vitess.client;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.util.concurrent.Futures.transformAsync;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import io.vitess.client.cursor.Cursor;
import io.vitess.client.cursor.CursorWithError;
import io.vitess.client.cursor.SimpleCursor;
import io.vitess.client.cursor.StreamCursor;
import io.vitess.proto.Query;
import io.vitess.proto.Query.SplitQueryRequest.Algorithm;
import io.vitess.proto.Topodata.KeyRange;
import io.vitess.proto.Topodata.SrvKeyspace;
import io.vitess.proto.Topodata.TabletType;
import io.vitess.proto.Vtgate;
import io.vitess.proto.Vtgate.BeginRequest;
import io.vitess.proto.Vtgate.BeginResponse;
import io.vitess.proto.Vtgate.BoundKeyspaceIdQuery;
import io.vitess.proto.Vtgate.BoundShardQuery;
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
import io.vitess.proto.Vtgate.SplitQueryRequest;
import io.vitess.proto.Vtgate.SplitQueryResponse;
import io.vitess.proto.Vtgate.StreamExecuteKeyRangesRequest;
import io.vitess.proto.Vtgate.StreamExecuteKeyspaceIdsRequest;
import io.vitess.proto.Vtgate.StreamExecuteRequest;
import io.vitess.proto.Vtgate.StreamExecuteShardsRequest;

import java.io.Closeable;
import java.io.IOException;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

/**
 * An asynchronous VTGate connection.
 *
 * <p>
 * See the <a href= "https://github.com/vitessio/vitess/blob/master/java/example/src/main/java/io/vitess/example/VitessClientExample.java">VitessClientExample</a>
 * for a usage example.
 *
 * <p>
 * All non-streaming calls on {@code VTGateConn} are asynchronous. Use {@link VTGateBlockingConn} if
 * you want synchronous calls.
 */
@Deprecated
public final class VTGateConn implements Closeable {

  private final RpcClient client;
  private final String keyspace;

  /**
   * Creates a VTGateConn with no default keyspace.
   *
   * <p>
   * In this mode, methods like {@code execute()} and {@code streamExecute()} that don't have a
   * per-call {@code keyspace} parameter will use VSchema to resolve the keyspace for any unprefixed
   * table names. Note that this only works if the table name is unique across all keyspaces.
   */
  public VTGateConn(RpcClient client) {
    this.client = checkNotNull(client);
    this.keyspace = "";
  }

  /**
   * Creates a VTGateConn with a default keyspace.
   *
   * <p>
   * The given {@code keyspace} will be used as the connection-wide default for {@code execute()}
   * and {@code streamExecute()} calls, since those do not specify the keyspace for each call. Like
   * the connection-wide default database of a MySQL connection, individual queries can still refer
   * to other keyspaces by prefixing table names. For example: {@code "SELECT ... FROM
   * keyspace.table ..."}
   */
  public VTGateConn(RpcClient client, String keyspace) {
    this.client = checkNotNull(client);
    this.keyspace = checkNotNull(keyspace);
  }

  public SQLFuture<Cursor> execute(Context ctx, String query, @Nullable Map<String, ?> bindVars,
      TabletType tabletType, Query.ExecuteOptions.IncludedFields includedFields)
      throws SQLException {
    ExecuteRequest.Builder requestBuilder = ExecuteRequest.newBuilder()
        .setQuery(Proto.bindQuery(checkNotNull(query), bindVars))
        .setKeyspaceShard(keyspace)
        .setTabletType(checkNotNull(tabletType))
        .setOptions(Query.ExecuteOptions.newBuilder()
            .setIncludedFields(includedFields));

    if (ctx.getCallerId() != null) {
      requestBuilder.setCallerId(ctx.getCallerId());
    }

    return new SQLFuture<Cursor>(
        transformAsync(
            client.execute(ctx, requestBuilder.build()),
            new AsyncFunction<ExecuteResponse, Cursor>() {
              @Override
              public ListenableFuture<Cursor> apply(ExecuteResponse response) throws Exception {
                Proto.checkError(response.getError());
                return Futures.<Cursor>immediateFuture(new SimpleCursor(response.getResult()));
              }
            },
            directExecutor()));
  }

  public SQLFuture<Cursor> executeShards(Context ctx, String query, String keyspace,
      Iterable<String> shards, @Nullable Map<String, ?> bindVars, TabletType tabletType,
      Query.ExecuteOptions.IncludedFields includedFields)
      throws SQLException {
    ExecuteShardsRequest.Builder requestBuilder =
        ExecuteShardsRequest.newBuilder()
            .setQuery(Proto.bindQuery(checkNotNull(query), bindVars))
            .setKeyspace(checkNotNull(keyspace))
            .addAllShards(checkNotNull(shards))
            .setTabletType(checkNotNull(tabletType))
            .setOptions(Query.ExecuteOptions.newBuilder()
                .setIncludedFields(includedFields));

    if (ctx.getCallerId() != null) {
      requestBuilder.setCallerId(ctx.getCallerId());
    }

    return new SQLFuture<Cursor>(
        transformAsync(
            client.executeShards(ctx, requestBuilder.build()),
            new AsyncFunction<ExecuteShardsResponse, Cursor>() {
              @Override
              public ListenableFuture<Cursor> apply(ExecuteShardsResponse response)
                  throws Exception {
                Proto.checkError(response.getError());
                return Futures.<Cursor>immediateFuture(new SimpleCursor(response.getResult()));
              }
            },
            directExecutor()));
  }

  public SQLFuture<Cursor> executeKeyspaceIds(Context ctx, String query, String keyspace,
      Iterable<byte[]> keyspaceIds, @Nullable Map<String, ?> bindVars, TabletType tabletType,
      Query.ExecuteOptions.IncludedFields includedFields)
      throws SQLException {
    ExecuteKeyspaceIdsRequest.Builder requestBuilder = ExecuteKeyspaceIdsRequest.newBuilder()
        .setQuery(Proto.bindQuery(checkNotNull(query), bindVars))
        .setKeyspace(checkNotNull(keyspace))
        .addAllKeyspaceIds(
            Iterables.transform(checkNotNull(keyspaceIds), Proto.BYTE_ARRAY_TO_BYTE_STRING))
        .setTabletType(checkNotNull(tabletType))
        .setOptions(Query.ExecuteOptions.newBuilder()
            .setIncludedFields(includedFields));

    if (ctx.getCallerId() != null) {
      requestBuilder.setCallerId(ctx.getCallerId());
    }

    return new SQLFuture<Cursor>(
        transformAsync(
            client.executeKeyspaceIds(ctx, requestBuilder.build()),
            new AsyncFunction<ExecuteKeyspaceIdsResponse, Cursor>() {
              @Override
              public ListenableFuture<Cursor> apply(ExecuteKeyspaceIdsResponse response)
                  throws Exception {
                Proto.checkError(response.getError());
                return Futures.<Cursor>immediateFuture(new SimpleCursor(response.getResult()));
              }
            },
            directExecutor()));
  }

  public SQLFuture<Cursor> executeKeyRanges(Context ctx, String query, String keyspace,
      Iterable<? extends KeyRange> keyRanges, @Nullable Map<String, ?> bindVars,
      TabletType tabletType, Query.ExecuteOptions.IncludedFields includedFields)
      throws SQLException {
    ExecuteKeyRangesRequest.Builder requestBuilder = ExecuteKeyRangesRequest.newBuilder()
        .setQuery(Proto.bindQuery(checkNotNull(query), bindVars))
        .setKeyspace(checkNotNull(keyspace))
        .addAllKeyRanges(checkNotNull(keyRanges))
        .setTabletType(checkNotNull(tabletType))
        .setOptions(Query.ExecuteOptions.newBuilder()
            .setIncludedFields(includedFields));

    if (ctx.getCallerId() != null) {
      requestBuilder.setCallerId(ctx.getCallerId());
    }

    return new SQLFuture<Cursor>(
        transformAsync(
            client.executeKeyRanges(ctx, requestBuilder.build()),
            new AsyncFunction<ExecuteKeyRangesResponse, Cursor>() {
              @Override
              public ListenableFuture<Cursor> apply(ExecuteKeyRangesResponse response)
                  throws Exception {
                Proto.checkError(response.getError());
                return Futures.<Cursor>immediateFuture(new SimpleCursor(response.getResult()));
              }
            },
            directExecutor()));
  }

  public SQLFuture<Cursor> executeEntityIds(Context ctx, String query, String keyspace,
      String entityColumnName, Map<byte[], ?> entityKeyspaceIds, @Nullable Map<String, ?> bindVars,
      TabletType tabletType, Query.ExecuteOptions.IncludedFields includedFields)
      throws SQLException {
    ExecuteEntityIdsRequest.Builder requestBuilder = ExecuteEntityIdsRequest.newBuilder()
        .setQuery(Proto.bindQuery(checkNotNull(query), bindVars))
        .setKeyspace(checkNotNull(keyspace))
        .setEntityColumnName(checkNotNull(entityColumnName))
        .addAllEntityKeyspaceIds(Iterables
            .transform(entityKeyspaceIds.entrySet(), Proto.MAP_ENTRY_TO_ENTITY_KEYSPACE_ID))
        .setTabletType(checkNotNull(tabletType))
        .setOptions(Query.ExecuteOptions.newBuilder()
            .setIncludedFields(includedFields));

    if (ctx.getCallerId() != null) {
      requestBuilder.setCallerId(ctx.getCallerId());
    }

    return new SQLFuture<Cursor>(
        transformAsync(
            client.executeEntityIds(ctx, requestBuilder.build()),
            new AsyncFunction<ExecuteEntityIdsResponse, Cursor>() {
              @Override
              public ListenableFuture<Cursor> apply(ExecuteEntityIdsResponse response)
                  throws Exception {
                Proto.checkError(response.getError());
                return Futures.<Cursor>immediateFuture(new SimpleCursor(response.getResult()));
              }
            },
            directExecutor()));
  }

  public SQLFuture<List<CursorWithError>> executeBatch(Context ctx, List<String> queryList,
      @Nullable List<Map<String, ?>> bindVarsList, TabletType tabletType,
      Query.ExecuteOptions.IncludedFields includedFields) throws SQLException {
    return executeBatch(ctx, queryList, bindVarsList, tabletType, false, includedFields);
  }

  public SQLFuture<List<CursorWithError>> executeBatch(Context ctx, List<String> queryList,
      @Nullable List<Map<String, ?>> bindVarsList, TabletType tabletType,
      boolean asTransaction, Query.ExecuteOptions.IncludedFields includedFields)
      throws SQLException {
    List<Query.BoundQuery> queries = new ArrayList<>();

    if (null != bindVarsList && bindVarsList.size() != queryList.size()) {
      throw new SQLDataException(
          "Size of SQL Query list does not match the bind variables list");
    }

    for (int i = 0; i < queryList.size(); ++i) {
      queries.add(i, Proto.bindQuery(checkNotNull(queryList.get(i)),
          bindVarsList == null ? null : bindVarsList.get(i)));
    }

    Vtgate.ExecuteBatchRequest.Builder requestBuilder =
        Vtgate.ExecuteBatchRequest.newBuilder()
            .addAllQueries(checkNotNull(queries))
            .setKeyspaceShard(keyspace)
            .setTabletType(checkNotNull(tabletType))
            .setAsTransaction(asTransaction)
            .setOptions(Query.ExecuteOptions.newBuilder()
                .setIncludedFields(includedFields));

    if (ctx.getCallerId() != null) {
      requestBuilder.setCallerId(ctx.getCallerId());
    }

    return new SQLFuture<>(
        transformAsync(
            client.executeBatch(ctx, requestBuilder.build()),
            new AsyncFunction<Vtgate.ExecuteBatchResponse, List<CursorWithError>>() {
              @Override
              public ListenableFuture<List<CursorWithError>> apply(
                  Vtgate.ExecuteBatchResponse response) throws Exception {
                Proto.checkError(response.getError());
                return Futures.immediateFuture(
                    Proto.fromQueryResponsesToCursorList(response.getResultsList()));
              }
            },
            directExecutor()));
  }

  /**
   * Execute multiple keyspace ID queries as a batch.
   *
   * @param asTransaction If true, automatically create a transaction (per shard) that encloses
   *     all the batch queries.
   */
  public SQLFuture<List<Cursor>> executeBatchShards(Context ctx,
      Iterable<? extends BoundShardQuery> queries, TabletType tabletType, boolean asTransaction,
      Query.ExecuteOptions.IncludedFields includedFields)
      throws SQLException {
    ExecuteBatchShardsRequest.Builder requestBuilder =
        ExecuteBatchShardsRequest.newBuilder()
            .addAllQueries(checkNotNull(queries))
            .setTabletType(checkNotNull(tabletType))
            .setAsTransaction(asTransaction)
            .setOptions(Query.ExecuteOptions.newBuilder()
                .setIncludedFields(includedFields));

    if (ctx.getCallerId() != null) {
      requestBuilder.setCallerId(ctx.getCallerId());
    }

    return new SQLFuture<List<Cursor>>(
        transformAsync(
            client.executeBatchShards(ctx, requestBuilder.build()),
            new AsyncFunction<ExecuteBatchShardsResponse, List<Cursor>>() {
              @Override
              public ListenableFuture<List<Cursor>> apply(ExecuteBatchShardsResponse response)
                  throws Exception {
                Proto.checkError(response.getError());
                return Futures.<List<Cursor>>immediateFuture(
                    Proto.toCursorList(response.getResultsList()));
              }
            },
            directExecutor()));
  }

  /**
   * Execute multiple keyspace ID queries as a batch.
   *
   * @param asTransaction If true, automatically create a transaction (per shard) that encloses
   *     all the batch queries.
   */
  public SQLFuture<List<Cursor>> executeBatchKeyspaceIds(Context ctx,
      Iterable<? extends BoundKeyspaceIdQuery> queries, TabletType tabletType,
      boolean asTransaction, Query.ExecuteOptions.IncludedFields includedFields)
      throws SQLException {
    ExecuteBatchKeyspaceIdsRequest.Builder requestBuilder =
        ExecuteBatchKeyspaceIdsRequest.newBuilder()
            .addAllQueries(checkNotNull(queries))
            .setTabletType(checkNotNull(tabletType))
            .setAsTransaction(asTransaction)
            .setOptions(Query.ExecuteOptions.newBuilder()
                .setIncludedFields(includedFields));

    if (ctx.getCallerId() != null) {
      requestBuilder.setCallerId(ctx.getCallerId());
    }

    return new SQLFuture<List<Cursor>>(
        transformAsync(
            client.executeBatchKeyspaceIds(ctx, requestBuilder.build()),
            new AsyncFunction<ExecuteBatchKeyspaceIdsResponse, List<Cursor>>() {
              @Override
              public ListenableFuture<List<Cursor>> apply(ExecuteBatchKeyspaceIdsResponse response)
                  throws Exception {
                Proto.checkError(response.getError());
                return Futures.<List<Cursor>>immediateFuture(
                    Proto.toCursorList(response.getResultsList()));
              }
            },
            directExecutor()));
  }

  public Cursor streamExecute(Context ctx, String query, @Nullable Map<String, ?> bindVars,
      TabletType tabletType, Query.ExecuteOptions.IncludedFields includedFields)
      throws SQLException {
    StreamExecuteRequest.Builder requestBuilder =
        StreamExecuteRequest.newBuilder()
            .setQuery(Proto.bindQuery(checkNotNull(query), bindVars))
            .setKeyspaceShard(keyspace)
            .setTabletType(checkNotNull(tabletType))
            .setOptions(Query.ExecuteOptions.newBuilder()
                .setIncludedFields(includedFields));

    if (ctx.getCallerId() != null) {
      requestBuilder.setCallerId(ctx.getCallerId());
    }

    return new StreamCursor(client.streamExecute(ctx, requestBuilder.build()));
  }

  public Cursor streamExecuteShards(Context ctx, String query, String keyspace,
      Iterable<String> shards, @Nullable Map<String, ?> bindVars, TabletType tabletType,
      Query.ExecuteOptions.IncludedFields includedFields)
      throws SQLException {
    StreamExecuteShardsRequest.Builder requestBuilder = StreamExecuteShardsRequest.newBuilder()
        .setQuery(Proto.bindQuery(checkNotNull(query), bindVars))
        .setKeyspace(checkNotNull(keyspace))
        .addAllShards(checkNotNull(shards))
        .setTabletType(checkNotNull(tabletType))
        .setOptions(Query.ExecuteOptions.newBuilder()
            .setIncludedFields(includedFields));

    if (ctx.getCallerId() != null) {
      requestBuilder.setCallerId(ctx.getCallerId());
    }

    return new StreamCursor(client.streamExecuteShards(ctx, requestBuilder.build()));
  }

  public Cursor streamExecuteKeyspaceIds(Context ctx, String query, String keyspace,
      Iterable<byte[]> keyspaceIds, @Nullable Map<String, ?> bindVars, TabletType tabletType,
      Query.ExecuteOptions.IncludedFields includedFields)
      throws SQLException {
    StreamExecuteKeyspaceIdsRequest.Builder requestBuilder = StreamExecuteKeyspaceIdsRequest
        .newBuilder()
        .setQuery(Proto.bindQuery(checkNotNull(query), bindVars))
        .setKeyspace(checkNotNull(keyspace))
        .addAllKeyspaceIds(
            Iterables.transform(checkNotNull(keyspaceIds), Proto.BYTE_ARRAY_TO_BYTE_STRING))
        .setTabletType(checkNotNull(tabletType))
        .setOptions(Query.ExecuteOptions.newBuilder()
            .setIncludedFields(includedFields));

    if (ctx.getCallerId() != null) {
      requestBuilder.setCallerId(ctx.getCallerId());
    }

    return new StreamCursor(client.streamExecuteKeyspaceIds(ctx, requestBuilder.build()));
  }

  public Cursor streamExecuteKeyRanges(Context ctx, String query, String keyspace,
      Iterable<? extends KeyRange> keyRanges, @Nullable Map<String, ?> bindVars,
      TabletType tabletType, Query.ExecuteOptions.IncludedFields includedFields)
      throws SQLException {
    StreamExecuteKeyRangesRequest.Builder requestBuilder = StreamExecuteKeyRangesRequest
        .newBuilder()
        .setQuery(Proto.bindQuery(checkNotNull(query), bindVars))
        .setKeyspace(checkNotNull(keyspace))
        .addAllKeyRanges(checkNotNull(keyRanges))
        .setTabletType(checkNotNull(tabletType))
        .setOptions(Query.ExecuteOptions.newBuilder()
            .setIncludedFields(includedFields));

    if (ctx.getCallerId() != null) {
      requestBuilder.setCallerId(ctx.getCallerId());
    }

    return new StreamCursor(client.streamExecuteKeyRanges(ctx, requestBuilder.build()));
  }

  public SQLFuture<VTGateTx> begin(Context ctx) throws SQLException {
    return begin(ctx, false);
  }

  public SQLFuture<VTGateTx> begin(Context ctx, boolean singleDB) throws SQLException {
    BeginRequest.Builder requestBuilder = BeginRequest.newBuilder().setSingleDb(singleDB);
    if (ctx.getCallerId() != null) {
      requestBuilder.setCallerId(ctx.getCallerId());
    }
    return new SQLFuture<VTGateTx>(
        transformAsync(
            client.begin(ctx, requestBuilder.build()),
            new AsyncFunction<BeginResponse, VTGateTx>() {
              @Override
              public ListenableFuture<VTGateTx> apply(BeginResponse response) throws Exception {
                return Futures.<VTGateTx>immediateFuture(
                    new VTGateTx(client, response.getSession(), keyspace));
              }
            },
            directExecutor()));
  }

  public SQLFuture<List<SplitQueryResponse.Part>> splitQuery(Context ctx, String keyspace,
      String query, @Nullable Map<String, ?> bindVars, Iterable<String> splitColumns,
      int splitCount, int numRowsPerQueryPart, Algorithm algorithm) throws SQLException {
    SplitQueryRequest.Builder requestBuilder =
        SplitQueryRequest.newBuilder()
            .setKeyspace(checkNotNull(keyspace))
            .setQuery(Proto.bindQuery(checkNotNull(query), bindVars))
            .addAllSplitColumn(splitColumns)
            .setSplitCount(splitCount)
            .setNumRowsPerQueryPart(numRowsPerQueryPart)
            .setAlgorithm(algorithm);
    if (ctx.getCallerId() != null) {
      requestBuilder.setCallerId(ctx.getCallerId());
    }
    return new SQLFuture<List<SplitQueryResponse.Part>>(
        transformAsync(
            client.splitQuery(ctx, requestBuilder.build()),
            new AsyncFunction<SplitQueryResponse, List<SplitQueryResponse.Part>>() {
              @Override
              public ListenableFuture<List<SplitQueryResponse.Part>> apply(
                  SplitQueryResponse response) throws Exception {
                return Futures.<List<SplitQueryResponse.Part>>immediateFuture(
                    response.getSplitsList());
              }
            },
            directExecutor()));
  }

  public SQLFuture<SrvKeyspace> getSrvKeyspace(Context ctx, String keyspace) throws SQLException {
    GetSrvKeyspaceRequest.Builder requestBuilder =
        GetSrvKeyspaceRequest.newBuilder().setKeyspace(checkNotNull(keyspace));
    return new SQLFuture<SrvKeyspace>(
        transformAsync(
            client.getSrvKeyspace(ctx, requestBuilder.build()),
            new AsyncFunction<GetSrvKeyspaceResponse, SrvKeyspace>() {
              @Override
              public ListenableFuture<SrvKeyspace> apply(GetSrvKeyspaceResponse response)
                  throws Exception {
                return Futures.<SrvKeyspace>immediateFuture(response.getSrvKeyspace());
              }
            },
            directExecutor()));
  }

  @Override
  public void close() throws IOException {
    client.close();
  }
}
