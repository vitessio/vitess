package com.youtube.vitess.client;

import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import com.youtube.vitess.client.cursor.Cursor;
import com.youtube.vitess.client.cursor.SimpleCursor;
import com.youtube.vitess.client.cursor.StreamCursor;
import com.youtube.vitess.proto.Topodata.KeyRange;
import com.youtube.vitess.proto.Topodata.SrvKeyspace;
import com.youtube.vitess.proto.Topodata.TabletType;
import com.youtube.vitess.proto.Vtgate.BeginRequest;
import com.youtube.vitess.proto.Vtgate.BeginResponse;
import com.youtube.vitess.proto.Vtgate.BoundKeyspaceIdQuery;
import com.youtube.vitess.proto.Vtgate.BoundShardQuery;
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
import com.youtube.vitess.proto.Vtgate.SplitQueryRequest;
import com.youtube.vitess.proto.Vtgate.SplitQueryResponse;
import com.youtube.vitess.proto.Vtgate.StreamExecuteKeyRangesRequest;
import com.youtube.vitess.proto.Vtgate.StreamExecuteKeyspaceIdsRequest;
import com.youtube.vitess.proto.Vtgate.StreamExecuteRequest;
import com.youtube.vitess.proto.Vtgate.StreamExecuteShardsRequest;

import java.io.Closeable;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * VTGateConn manages a VTGate connection.
 *
 * <p>See the
 * <a href="https://github.com/youtube/vitess/blob/master/java/example/src/main/java/com/youtube/vitess/example/VitessClientExample.java">VitessClientExample</a>
 * for a usage example.
 *
 * <p>Non-streaming calls are asynchronous by default. To use these calls synchronously,
 * append {@code .checkedGet()}. For example:
 *
 * <blockquote><pre>
 * Cursor cursor = vtgateConn.execute(...).checkedGet();
 * </pre></blockquote>
 * */
public class VTGateConn implements Closeable {
  private RpcClient client;

  public VTGateConn(RpcClient client) {
    this.client = client;
  }

  public SQLFuture<Cursor> execute(
      Context ctx, String query, Map<String, ?> bindVars, TabletType tabletType)
      throws SQLException {
    ExecuteRequest.Builder requestBuilder =
        ExecuteRequest.newBuilder()
            .setQuery(Proto.bindQuery(query, bindVars))
            .setTabletType(tabletType);
    if (ctx.getCallerId() != null) {
      requestBuilder.setCallerId(ctx.getCallerId());
    }
    return new SQLFuture<Cursor>(
        Futures.transformAsync(
            client.execute(ctx, requestBuilder.build()),
            new AsyncFunction<ExecuteResponse, Cursor>() {
              @Override
              public ListenableFuture<Cursor> apply(ExecuteResponse response) throws Exception {
                Proto.checkError(response.getError());
                return Futures.<Cursor>immediateFuture(new SimpleCursor(response.getResult()));
              }
            }));
  }

  public SQLFuture<Cursor> executeShards(
      Context ctx,
      String query,
      String keyspace,
      Iterable<String> shards,
      Map<String, ?> bindVars,
      TabletType tabletType)
      throws SQLException {
    ExecuteShardsRequest.Builder requestBuilder =
        ExecuteShardsRequest.newBuilder()
            .setQuery(Proto.bindQuery(query, bindVars))
            .setKeyspace(keyspace)
            .addAllShards(shards)
            .setTabletType(tabletType);
    if (ctx.getCallerId() != null) {
      requestBuilder.setCallerId(ctx.getCallerId());
    }
    return new SQLFuture<Cursor>(
        Futures.transformAsync(
            client.executeShards(ctx, requestBuilder.build()),
            new AsyncFunction<ExecuteShardsResponse, Cursor>() {
              @Override
              public ListenableFuture<Cursor> apply(ExecuteShardsResponse response)
                  throws Exception {
                Proto.checkError(response.getError());
                return Futures.<Cursor>immediateFuture(new SimpleCursor(response.getResult()));
              }
            }));
  }

  public SQLFuture<Cursor> executeKeyspaceIds(
      Context ctx,
      String query,
      String keyspace,
      Iterable<byte[]> keyspaceIds,
      Map<String, ?> bindVars,
      TabletType tabletType)
      throws SQLException {
    ExecuteKeyspaceIdsRequest.Builder requestBuilder =
        ExecuteKeyspaceIdsRequest.newBuilder()
            .setQuery(Proto.bindQuery(query, bindVars))
            .setKeyspace(keyspace)
            .addAllKeyspaceIds(Iterables.transform(keyspaceIds, Proto.BYTE_ARRAY_TO_BYTE_STRING))
            .setTabletType(tabletType);
    if (ctx.getCallerId() != null) {
      requestBuilder.setCallerId(ctx.getCallerId());
    }
    return new SQLFuture<Cursor>(
        Futures.transformAsync(
            client.executeKeyspaceIds(ctx, requestBuilder.build()),
            new AsyncFunction<ExecuteKeyspaceIdsResponse, Cursor>() {
              @Override
              public ListenableFuture<Cursor> apply(ExecuteKeyspaceIdsResponse response)
                  throws Exception {
                Proto.checkError(response.getError());
                return Futures.<Cursor>immediateFuture(new SimpleCursor(response.getResult()));
              }
            }));
  }

  public SQLFuture<Cursor> executeKeyRanges(
      Context ctx,
      String query,
      String keyspace,
      Iterable<? extends KeyRange> keyRanges,
      Map<String, ?> bindVars,
      TabletType tabletType)
      throws SQLException {
    ExecuteKeyRangesRequest.Builder requestBuilder =
        ExecuteKeyRangesRequest.newBuilder()
            .setQuery(Proto.bindQuery(query, bindVars))
            .setKeyspace(keyspace)
            .addAllKeyRanges(keyRanges)
            .setTabletType(tabletType);
    if (ctx.getCallerId() != null) {
      requestBuilder.setCallerId(ctx.getCallerId());
    }
    return new SQLFuture<Cursor>(
        Futures.transformAsync(
            client.executeKeyRanges(ctx, requestBuilder.build()),
            new AsyncFunction<ExecuteKeyRangesResponse, Cursor>() {
              @Override
              public ListenableFuture<Cursor> apply(ExecuteKeyRangesResponse response)
                  throws Exception {
                Proto.checkError(response.getError());
                return Futures.<Cursor>immediateFuture(new SimpleCursor(response.getResult()));
              }
            }));
  }

  public SQLFuture<Cursor> executeEntityIds(
      Context ctx,
      String query,
      String keyspace,
      String entityColumnName,
      Map<byte[], ?> entityKeyspaceIds,
      Map<String, ?> bindVars,
      TabletType tabletType)
      throws SQLException {
    ExecuteEntityIdsRequest.Builder requestBuilder =
        ExecuteEntityIdsRequest.newBuilder()
            .setQuery(Proto.bindQuery(query, bindVars))
            .setKeyspace(keyspace)
            .setEntityColumnName(entityColumnName)
            .addAllEntityKeyspaceIds(
                Iterables.transform(
                    entityKeyspaceIds.entrySet(), Proto.MAP_ENTRY_TO_ENTITY_KEYSPACE_ID))
            .setTabletType(tabletType);
    if (ctx.getCallerId() != null) {
      requestBuilder.setCallerId(ctx.getCallerId());
    }
    return new SQLFuture<Cursor>(
        Futures.transformAsync(
            client.executeEntityIds(ctx, requestBuilder.build()),
            new AsyncFunction<ExecuteEntityIdsResponse, Cursor>() {
              @Override
              public ListenableFuture<Cursor> apply(ExecuteEntityIdsResponse response)
                  throws Exception {
                Proto.checkError(response.getError());
                return Futures.<Cursor>immediateFuture(new SimpleCursor(response.getResult()));
              }
            }));
  }

  /**
   * Execute multiple keyspace ID queries as a batch.
   *
   * @param asTransaction If true, automatically create a transaction (per shard) that encloses all
   * the batch queries.
   */
  public SQLFuture<List<Cursor>> executeBatchShards(
      Context ctx,
      Iterable<? extends BoundShardQuery> queries,
      TabletType tabletType,
      boolean asTransaction)
      throws SQLException {
    ExecuteBatchShardsRequest.Builder requestBuilder =
        ExecuteBatchShardsRequest.newBuilder()
            .addAllQueries(queries)
            .setTabletType(tabletType)
            .setAsTransaction(asTransaction);
    if (ctx.getCallerId() != null) {
      requestBuilder.setCallerId(ctx.getCallerId());
    }
    return new SQLFuture<List<Cursor>>(
        Futures.transformAsync(
            client.executeBatchShards(ctx, requestBuilder.build()),
            new AsyncFunction<ExecuteBatchShardsResponse, List<Cursor>>() {
              @Override
              public ListenableFuture<List<Cursor>> apply(ExecuteBatchShardsResponse response)
                  throws Exception {
                Proto.checkError(response.getError());
                return Futures.<List<Cursor>>immediateFuture(
                    Proto.toCursorList(response.getResultsList()));
              }
            }));
  }

  /**
   * Execute multiple keyspace ID queries as a batch.
   *
   * @param asTransaction If true, automatically create a transaction (per shard) that encloses all
   * the batch queries.
   */
  public SQLFuture<List<Cursor>> executeBatchKeyspaceIds(
      Context ctx,
      Iterable<? extends BoundKeyspaceIdQuery> queries,
      TabletType tabletType,
      boolean asTransaction)
      throws SQLException {
    ExecuteBatchKeyspaceIdsRequest.Builder requestBuilder =
        ExecuteBatchKeyspaceIdsRequest.newBuilder()
            .addAllQueries(queries)
            .setTabletType(tabletType)
            .setAsTransaction(asTransaction);
    if (ctx.getCallerId() != null) {
      requestBuilder.setCallerId(ctx.getCallerId());
    }
    return new SQLFuture<List<Cursor>>(
        Futures.transformAsync(
            client.executeBatchKeyspaceIds(ctx, requestBuilder.build()),
            new AsyncFunction<ExecuteBatchKeyspaceIdsResponse, List<Cursor>>() {
              @Override
              public ListenableFuture<List<Cursor>> apply(ExecuteBatchKeyspaceIdsResponse response)
                  throws Exception {
                Proto.checkError(response.getError());
                return Futures.<List<Cursor>>immediateFuture(
                    Proto.toCursorList(response.getResultsList()));
              }
            }));
  }

  public Cursor streamExecute(
      Context ctx, String query, Map<String, ?> bindVars, TabletType tabletType)
      throws SQLException {
    StreamExecuteRequest.Builder requestBuilder =
        StreamExecuteRequest.newBuilder()
            .setQuery(Proto.bindQuery(query, bindVars))
            .setTabletType(tabletType);
    if (ctx.getCallerId() != null) {
      requestBuilder.setCallerId(ctx.getCallerId());
    }
    return new StreamCursor(client.streamExecute(ctx, requestBuilder.build()));
  }

  public Cursor streamExecuteShards(
      Context ctx,
      String query,
      String keyspace,
      Iterable<String> shards,
      Map<String, ?> bindVars,
      TabletType tabletType)
      throws SQLException {
    StreamExecuteShardsRequest.Builder requestBuilder =
        StreamExecuteShardsRequest.newBuilder()
            .setQuery(Proto.bindQuery(query, bindVars))
            .setKeyspace(keyspace)
            .addAllShards(shards)
            .setTabletType(tabletType);
    if (ctx.getCallerId() != null) {
      requestBuilder.setCallerId(ctx.getCallerId());
    }
    return new StreamCursor(client.streamExecuteShards(ctx, requestBuilder.build()));
  }

  public Cursor streamExecuteKeyspaceIds(
      Context ctx,
      String query,
      String keyspace,
      Iterable<byte[]> keyspaceIds,
      Map<String, ?> bindVars,
      TabletType tabletType)
      throws SQLException {
    StreamExecuteKeyspaceIdsRequest.Builder requestBuilder =
        StreamExecuteKeyspaceIdsRequest.newBuilder()
            .setQuery(Proto.bindQuery(query, bindVars))
            .setKeyspace(keyspace)
            .addAllKeyspaceIds(Iterables.transform(keyspaceIds, Proto.BYTE_ARRAY_TO_BYTE_STRING))
            .setTabletType(tabletType);
    if (ctx.getCallerId() != null) {
      requestBuilder.setCallerId(ctx.getCallerId());
    }
    return new StreamCursor(client.streamExecuteKeyspaceIds(ctx, requestBuilder.build()));
  }

  public Cursor streamExecuteKeyRanges(
      Context ctx,
      String query,
      String keyspace,
      Iterable<? extends KeyRange> keyRanges,
      Map<String, ?> bindVars,
      TabletType tabletType)
      throws SQLException {
    StreamExecuteKeyRangesRequest.Builder requestBuilder =
        StreamExecuteKeyRangesRequest.newBuilder()
            .setQuery(Proto.bindQuery(query, bindVars))
            .setKeyspace(keyspace)
            .addAllKeyRanges(keyRanges)
            .setTabletType(tabletType);
    if (ctx.getCallerId() != null) {
      requestBuilder.setCallerId(ctx.getCallerId());
    }
    return new StreamCursor(client.streamExecuteKeyRanges(ctx, requestBuilder.build()));
  }

  public SQLFuture<VTGateTx> begin(Context ctx) throws SQLException {
    BeginRequest.Builder requestBuilder = BeginRequest.newBuilder();
    if (ctx.getCallerId() != null) {
      requestBuilder.setCallerId(ctx.getCallerId());
    }
    return new SQLFuture<VTGateTx>(
        Futures.transformAsync(
            client.begin(ctx, requestBuilder.build()),
            new AsyncFunction<BeginResponse, VTGateTx>() {
              @Override
              public ListenableFuture<VTGateTx> apply(BeginResponse response) throws Exception {
                return Futures.<VTGateTx>immediateFuture(
                    VTGateTx.withRpcClientAndSession(client, response.getSession()));
              }
            }));
  }

  public SQLFuture<List<SplitQueryResponse.Part>> splitQuery(
      Context ctx,
      String keyspace,
      String query,
      Map<String, ?> bindVars,
      String splitColumn,
      long splitCount)
      throws SQLException {
    SplitQueryRequest.Builder requestBuilder =
        SplitQueryRequest.newBuilder()
            .setKeyspace(keyspace)
            .setQuery(Proto.bindQuery(query, bindVars))
            .setSplitColumn(splitColumn)
            .setSplitCount(splitCount);
    if (ctx.getCallerId() != null) {
      requestBuilder.setCallerId(ctx.getCallerId());
    }
    return new SQLFuture<List<SplitQueryResponse.Part>>(
        Futures.transformAsync(
            client.splitQuery(ctx, requestBuilder.build()),
            new AsyncFunction<SplitQueryResponse, List<SplitQueryResponse.Part>>() {
              @Override
              public ListenableFuture<List<SplitQueryResponse.Part>> apply(
                  SplitQueryResponse response) throws Exception {
                return Futures.<List<SplitQueryResponse.Part>>immediateFuture(
                    response.getSplitsList());
              }
            }));
  }

  public SQLFuture<SrvKeyspace> getSrvKeyspace(Context ctx, String keyspace) throws SQLException {
    GetSrvKeyspaceRequest.Builder requestBuilder =
        GetSrvKeyspaceRequest.newBuilder().setKeyspace(keyspace);
    return new SQLFuture<SrvKeyspace>(
        Futures.transformAsync(
            client.getSrvKeyspace(ctx, requestBuilder.build()),
            new AsyncFunction<GetSrvKeyspaceResponse, SrvKeyspace>() {
              @Override
              public ListenableFuture<SrvKeyspace> apply(GetSrvKeyspaceResponse response)
                  throws Exception {
                return Futures.<SrvKeyspace>immediateFuture(response.getSrvKeyspace());
              }
            }));
  }

  @Override
  public void close() throws IOException {
    client.close();
  }
}
