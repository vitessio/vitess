package com.youtube.vitess.client;

import static com.google.common.base.Preconditions.checkNotNull;

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

import javax.annotation.Nullable;

/**
 * An asynchronous VTGate connection.
 *
 * <p>
 * See the <a href=
 * "https://github.com/youtube/vitess/blob/master/java/example/src/main/java/com/youtube/vitess/example/VitessClientExample.java">VitessClientExample</a>
 * for a usage example.
 *
 * <p>
 * All non-streaming calls on {@code VTGateConn} are asynchronous. Use {@link VTGateBlockingConn} if
 * you want synchronous calls.
 */
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
   * to other keyspaces by prefixing table names. For example:
   * {@code "SELECT ... FROM keyspace.table ..."}
   */
  public VTGateConn(RpcClient client, String keyspace) {
    this.client = checkNotNull(client);
    this.keyspace = checkNotNull(keyspace);
  }

  public SQLFuture<Cursor> execute(Context ctx, String query, @Nullable Map<String, ?> bindVars,
      TabletType tabletType) throws SQLException {
    ExecuteRequest.Builder requestBuilder =
        ExecuteRequest.newBuilder().setQuery(Proto.bindQuery(checkNotNull(query), bindVars))
            .setKeyspace(keyspace).setTabletType(checkNotNull(tabletType));
    if (ctx.getCallerId() != null) {
      requestBuilder.setCallerId(ctx.getCallerId());
    }
    return new SQLFuture<Cursor>(Futures.transformAsync(client.execute(ctx, requestBuilder.build()),
        new AsyncFunction<ExecuteResponse, Cursor>() {
          @Override
          public ListenableFuture<Cursor> apply(ExecuteResponse response) throws Exception {
            Proto.checkError(response.getError());
            return Futures.<Cursor>immediateFuture(new SimpleCursor(response.getResult()));
          }
        }));
  }

  public SQLFuture<Cursor> executeShards(Context ctx, String query, String keyspace,
      Iterable<String> shards, @Nullable Map<String, ?> bindVars, TabletType tabletType)
      throws SQLException {
    ExecuteShardsRequest.Builder requestBuilder =
        ExecuteShardsRequest.newBuilder().setQuery(Proto.bindQuery(checkNotNull(query), bindVars))
            .setKeyspace(checkNotNull(keyspace)).addAllShards(checkNotNull(shards))
            .setTabletType(checkNotNull(tabletType));
    if (ctx.getCallerId() != null) {
      requestBuilder.setCallerId(ctx.getCallerId());
    }
    return new SQLFuture<Cursor>(
        Futures.transformAsync(client.executeShards(ctx, requestBuilder.build()),
            new AsyncFunction<ExecuteShardsResponse, Cursor>() {
              @Override
              public ListenableFuture<Cursor> apply(ExecuteShardsResponse response)
                  throws Exception {
                Proto.checkError(response.getError());
                return Futures.<Cursor>immediateFuture(new SimpleCursor(response.getResult()));
              }
            }));
  }

  public SQLFuture<Cursor> executeKeyspaceIds(Context ctx, String query, String keyspace,
      Iterable<byte[]> keyspaceIds, @Nullable Map<String, ?> bindVars, TabletType tabletType)
      throws SQLException {
    ExecuteKeyspaceIdsRequest.Builder requestBuilder = ExecuteKeyspaceIdsRequest.newBuilder()
        .setQuery(Proto.bindQuery(checkNotNull(query), bindVars))
        .setKeyspace(checkNotNull(keyspace))
        .addAllKeyspaceIds(
            Iterables.transform(checkNotNull(keyspaceIds), Proto.BYTE_ARRAY_TO_BYTE_STRING))
        .setTabletType(checkNotNull(tabletType));
    if (ctx.getCallerId() != null) {
      requestBuilder.setCallerId(ctx.getCallerId());
    }
    return new SQLFuture<Cursor>(
        Futures.transformAsync(client.executeKeyspaceIds(ctx, requestBuilder.build()),
            new AsyncFunction<ExecuteKeyspaceIdsResponse, Cursor>() {
              @Override
              public ListenableFuture<Cursor> apply(ExecuteKeyspaceIdsResponse response)
                  throws Exception {
                Proto.checkError(response.getError());
                return Futures.<Cursor>immediateFuture(new SimpleCursor(response.getResult()));
              }
            }));
  }

  public SQLFuture<Cursor> executeKeyRanges(Context ctx, String query, String keyspace,
      Iterable<? extends KeyRange> keyRanges, @Nullable Map<String, ?> bindVars,
      TabletType tabletType) throws SQLException {
    ExecuteKeyRangesRequest.Builder requestBuilder = ExecuteKeyRangesRequest.newBuilder()
        .setQuery(Proto.bindQuery(checkNotNull(query), bindVars))
        .setKeyspace(checkNotNull(keyspace)).addAllKeyRanges(checkNotNull(keyRanges))
        .setTabletType(checkNotNull(tabletType));
    if (ctx.getCallerId() != null) {
      requestBuilder.setCallerId(ctx.getCallerId());
    }
    return new SQLFuture<Cursor>(
        Futures.transformAsync(client.executeKeyRanges(ctx, requestBuilder.build()),
            new AsyncFunction<ExecuteKeyRangesResponse, Cursor>() {
              @Override
              public ListenableFuture<Cursor> apply(ExecuteKeyRangesResponse response)
                  throws Exception {
                Proto.checkError(response.getError());
                return Futures.<Cursor>immediateFuture(new SimpleCursor(response.getResult()));
              }
            }));
  }

  public SQLFuture<Cursor> executeEntityIds(Context ctx, String query, String keyspace,
      String entityColumnName, Map<byte[], ?> entityKeyspaceIds, @Nullable Map<String, ?> bindVars,
      TabletType tabletType) throws SQLException {
    ExecuteEntityIdsRequest.Builder requestBuilder = ExecuteEntityIdsRequest.newBuilder()
        .setQuery(Proto.bindQuery(checkNotNull(query), bindVars))
        .setKeyspace(checkNotNull(keyspace))
        .setEntityColumnName(checkNotNull(entityColumnName)).addAllEntityKeyspaceIds(Iterables
            .transform(entityKeyspaceIds.entrySet(), Proto.MAP_ENTRY_TO_ENTITY_KEYSPACE_ID))
        .setTabletType(checkNotNull(tabletType));
    if (ctx.getCallerId() != null) {
      requestBuilder.setCallerId(ctx.getCallerId());
    }
    return new SQLFuture<Cursor>(
        Futures.transformAsync(client.executeEntityIds(ctx, requestBuilder.build()),
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
   *        the batch queries.
   */
  public SQLFuture<List<Cursor>> executeBatchShards(Context ctx,
      Iterable<? extends BoundShardQuery> queries, TabletType tabletType, boolean asTransaction)
      throws SQLException {
    ExecuteBatchShardsRequest.Builder requestBuilder =
        ExecuteBatchShardsRequest.newBuilder().addAllQueries(checkNotNull(queries))
            .setTabletType(checkNotNull(tabletType)).setAsTransaction(asTransaction);
    if (ctx.getCallerId() != null) {
      requestBuilder.setCallerId(ctx.getCallerId());
    }
    return new SQLFuture<List<Cursor>>(
        Futures.transformAsync(client.executeBatchShards(ctx, requestBuilder.build()),
            new AsyncFunction<ExecuteBatchShardsResponse, List<Cursor>>() {
              @Override
              public ListenableFuture<List<Cursor>> apply(ExecuteBatchShardsResponse response)
                  throws Exception {
                Proto.checkError(response.getError());
                return Futures
                    .<List<Cursor>>immediateFuture(Proto.toCursorList(response.getResultsList()));
              }
            }));
  }

  /**
   * Execute multiple keyspace ID queries as a batch.
   *
   * @param asTransaction If true, automatically create a transaction (per shard) that encloses all
   *        the batch queries.
   */
  public SQLFuture<List<Cursor>> executeBatchKeyspaceIds(Context ctx,
      Iterable<? extends BoundKeyspaceIdQuery> queries, TabletType tabletType,
      boolean asTransaction) throws SQLException {
    ExecuteBatchKeyspaceIdsRequest.Builder requestBuilder =
        ExecuteBatchKeyspaceIdsRequest.newBuilder().addAllQueries(checkNotNull(queries))
            .setTabletType(checkNotNull(tabletType)).setAsTransaction(asTransaction);
    if (ctx.getCallerId() != null) {
      requestBuilder.setCallerId(ctx.getCallerId());
    }
    return new SQLFuture<List<Cursor>>(
        Futures.transformAsync(client.executeBatchKeyspaceIds(ctx, requestBuilder.build()),
            new AsyncFunction<ExecuteBatchKeyspaceIdsResponse, List<Cursor>>() {
              @Override
              public ListenableFuture<List<Cursor>> apply(ExecuteBatchKeyspaceIdsResponse response)
                  throws Exception {
                Proto.checkError(response.getError());
                return Futures
                    .<List<Cursor>>immediateFuture(Proto.toCursorList(response.getResultsList()));
              }
            }));
  }

  public Cursor streamExecute(Context ctx, String query, @Nullable Map<String, ?> bindVars,
      TabletType tabletType) throws SQLException {
    StreamExecuteRequest.Builder requestBuilder =
        StreamExecuteRequest.newBuilder().setQuery(Proto.bindQuery(checkNotNull(query), bindVars))
            .setKeyspace(keyspace).setTabletType(checkNotNull(tabletType));
    if (ctx.getCallerId() != null) {
      requestBuilder.setCallerId(ctx.getCallerId());
    }
    return new StreamCursor(client.streamExecute(ctx, requestBuilder.build()));
  }

  public Cursor streamExecuteShards(Context ctx, String query, String keyspace,
      Iterable<String> shards, @Nullable Map<String, ?> bindVars, TabletType tabletType)
      throws SQLException {
    StreamExecuteShardsRequest.Builder requestBuilder = StreamExecuteShardsRequest.newBuilder()
        .setQuery(Proto.bindQuery(checkNotNull(query), bindVars))
        .setKeyspace(checkNotNull(keyspace)).addAllShards(checkNotNull(shards))
        .setTabletType(checkNotNull(tabletType));
    if (ctx.getCallerId() != null) {
      requestBuilder.setCallerId(ctx.getCallerId());
    }
    return new StreamCursor(client.streamExecuteShards(ctx, requestBuilder.build()));
  }

  public Cursor streamExecuteKeyspaceIds(Context ctx, String query, String keyspace,
      Iterable<byte[]> keyspaceIds, @Nullable Map<String, ?> bindVars, TabletType tabletType)
      throws SQLException {
    StreamExecuteKeyspaceIdsRequest.Builder requestBuilder = StreamExecuteKeyspaceIdsRequest
        .newBuilder().setQuery(Proto.bindQuery(checkNotNull(query), bindVars))
        .setKeyspace(checkNotNull(keyspace))
        .addAllKeyspaceIds(
            Iterables.transform(checkNotNull(keyspaceIds), Proto.BYTE_ARRAY_TO_BYTE_STRING))
        .setTabletType(checkNotNull(tabletType));
    if (ctx.getCallerId() != null) {
      requestBuilder.setCallerId(ctx.getCallerId());
    }
    return new StreamCursor(client.streamExecuteKeyspaceIds(ctx, requestBuilder.build()));
  }

  public Cursor streamExecuteKeyRanges(Context ctx, String query, String keyspace,
      Iterable<? extends KeyRange> keyRanges, @Nullable Map<String, ?> bindVars,
      TabletType tabletType) throws SQLException {
    StreamExecuteKeyRangesRequest.Builder requestBuilder = StreamExecuteKeyRangesRequest
        .newBuilder().setQuery(Proto.bindQuery(checkNotNull(query), bindVars))
        .setKeyspace(checkNotNull(keyspace)).addAllKeyRanges(checkNotNull(keyRanges))
        .setTabletType(checkNotNull(tabletType));
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
    return new SQLFuture<VTGateTx>(Futures.transformAsync(client.begin(ctx, requestBuilder.build()),
        new AsyncFunction<BeginResponse, VTGateTx>() {
          @Override
          public ListenableFuture<VTGateTx> apply(BeginResponse response) throws Exception {
            return Futures
                .<VTGateTx>immediateFuture(new VTGateTx(client, response.getSession(), keyspace));
          }
        }));
  }

  public SQLFuture<List<SplitQueryResponse.Part>> splitQuery(Context ctx, String keyspace,
      String query, @Nullable Map<String, ?> bindVars, Iterable<String> splitColumns, 
      int splitCount, int numRowsPerQueryPart, 
      com.youtube.vitess.proto.Query.SplitQueryRequest.Algorithm algorithm) throws SQLException {
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
        Futures.transformAsync(client.splitQuery(ctx, requestBuilder.build()),
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
        GetSrvKeyspaceRequest.newBuilder().setKeyspace(checkNotNull(keyspace));
    return new SQLFuture<SrvKeyspace>(
        Futures.transformAsync(client.getSrvKeyspace(ctx, requestBuilder.build()),
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
