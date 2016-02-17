package com.youtube.vitess.client;

import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import com.youtube.vitess.client.cursor.Cursor;
import com.youtube.vitess.client.cursor.SimpleCursor;
import com.youtube.vitess.proto.Topodata.KeyRange;
import com.youtube.vitess.proto.Topodata.TabletType;
import com.youtube.vitess.proto.Vtgate.BoundKeyspaceIdQuery;
import com.youtube.vitess.proto.Vtgate.BoundShardQuery;
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
import com.youtube.vitess.proto.Vtgate.RollbackRequest;
import com.youtube.vitess.proto.Vtgate.RollbackResponse;
import com.youtube.vitess.proto.Vtgate.Session;

import java.sql.SQLDataException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * VTGateTx manages a pending transaction.
 *
 * <p>Because VTGateTx manages a session cookie, only one operation can be in flight
 * at a time on a given instance. The methods are {@code synchronized} because the
 * session cookie is updated asynchronously when the RPC response comes back.
 *
 * <p>To use these calls synchronously, append {@code .checkedGet()}. For example:
 *
 * <blockquote><pre>
 * Cursor cursor = tx.execute(...).checkedGet();
 * </pre></blockquote>
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

  public synchronized SQLFuture<Cursor> execute(
      Context ctx, String query, Map<String, ?> bindVars, TabletType tabletType)
      throws SQLException {
    if (!inTransaction()) {
      throw new SQLDataException("execute: not in transaction");
    }
    ExecuteRequest.Builder requestBuilder =
        ExecuteRequest.newBuilder()
            .setQuery(Proto.bindQuery(query, bindVars))
            .setTabletType(tabletType)
            .setSession(session);
    if (ctx.getCallerId() != null) {
      requestBuilder.setCallerId(ctx.getCallerId());
    }
    return new SQLFuture<Cursor>(
        Futures.transformAsync(
            client.execute(ctx, requestBuilder.build()),
            new AsyncFunction<ExecuteResponse, Cursor>() {
              @Override
              public ListenableFuture<Cursor> apply(ExecuteResponse response) throws Exception {
                setSession(response.getSession());
                Proto.checkError(response.getError());
                return Futures.<Cursor>immediateFuture(new SimpleCursor(response.getResult()));
              }
            }));
  }

  public synchronized SQLFuture<Cursor> executeShards(
      Context ctx,
      String query,
      String keyspace,
      Iterable<String> shards,
      Map<String, ?> bindVars,
      TabletType tabletType)
      throws SQLException {
    if (!inTransaction()) {
      throw new SQLDataException("executeShards: not in transaction");
    }
    ExecuteShardsRequest.Builder requestBuilder =
        ExecuteShardsRequest.newBuilder()
            .setQuery(Proto.bindQuery(query, bindVars))
            .setKeyspace(keyspace)
            .addAllShards(shards)
            .setTabletType(tabletType)
            .setSession(session);
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
                setSession(response.getSession());
                Proto.checkError(response.getError());
                return Futures.<Cursor>immediateFuture(new SimpleCursor(response.getResult()));
              }
            }));
  }

  public synchronized SQLFuture<Cursor> executeKeyspaceIds(
      Context ctx,
      String query,
      String keyspace,
      Iterable<byte[]> keyspaceIds,
      Map<String, ?> bindVars,
      TabletType tabletType)
      throws SQLException {
    if (!inTransaction()) {
      throw new SQLDataException("executeKeyspaceIds: not in transaction");
    }
    ExecuteKeyspaceIdsRequest.Builder requestBuilder =
        ExecuteKeyspaceIdsRequest.newBuilder()
            .setQuery(Proto.bindQuery(query, bindVars))
            .setKeyspace(keyspace)
            .addAllKeyspaceIds(Iterables.transform(keyspaceIds, Proto.BYTE_ARRAY_TO_BYTE_STRING))
            .setTabletType(tabletType)
            .setSession(session);
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
                setSession(response.getSession());
                Proto.checkError(response.getError());
                return Futures.<Cursor>immediateFuture(new SimpleCursor(response.getResult()));
              }
            }));
  }

  public synchronized SQLFuture<Cursor> executeKeyRanges(
      Context ctx,
      String query,
      String keyspace,
      Iterable<? extends KeyRange> keyRanges,
      Map<String, ?> bindVars,
      TabletType tabletType)
      throws SQLException {
    if (!inTransaction()) {
      throw new SQLDataException("executeKeyRanges: not in transaction");
    }
    ExecuteKeyRangesRequest.Builder requestBuilder =
        ExecuteKeyRangesRequest.newBuilder()
            .setQuery(Proto.bindQuery(query, bindVars))
            .setKeyspace(keyspace)
            .addAllKeyRanges(keyRanges)
            .setTabletType(tabletType)
            .setSession(session);
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
                setSession(response.getSession());
                Proto.checkError(response.getError());
                return Futures.<Cursor>immediateFuture(new SimpleCursor(response.getResult()));
              }
            }));
  }

  public synchronized SQLFuture<Cursor> executeEntityIds(
      Context ctx,
      String query,
      String keyspace,
      String entityColumnName,
      Map<byte[], ?> entityKeyspaceIds,
      Map<String, ?> bindVars,
      TabletType tabletType)
      throws SQLException {
    if (!inTransaction()) {
      throw new SQLDataException("executeEntityIds: not in transaction");
    }
    ExecuteEntityIdsRequest.Builder requestBuilder =
        ExecuteEntityIdsRequest.newBuilder()
            .setQuery(Proto.bindQuery(query, bindVars))
            .setKeyspace(keyspace)
            .setEntityColumnName(entityColumnName)
            .addAllEntityKeyspaceIds(
                Iterables.transform(
                    entityKeyspaceIds.entrySet(), Proto.MAP_ENTRY_TO_ENTITY_KEYSPACE_ID))
            .setTabletType(tabletType)
            .setSession(session);
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
                setSession(response.getSession());
                Proto.checkError(response.getError());
                return Futures.<Cursor>immediateFuture(new SimpleCursor(response.getResult()));
              }
            }));
  }

  public synchronized SQLFuture<List<Cursor>> executeBatchShards(
      Context ctx, Iterable<? extends BoundShardQuery> queries, TabletType tabletType)
      throws SQLException {
    if (!inTransaction()) {
      throw new SQLDataException("executeBatchShards: not in transaction");
    }
    ExecuteBatchShardsRequest.Builder requestBuilder =
        ExecuteBatchShardsRequest.newBuilder()
            .addAllQueries(queries)
            .setTabletType(tabletType)
            .setSession(session);
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
                setSession(response.getSession());
                Proto.checkError(response.getError());
                return Futures.<List<Cursor>>immediateFuture(
                    Proto.toCursorList(response.getResultsList()));
              }
            }));
  }

  public synchronized SQLFuture<List<Cursor>> executeBatchKeyspaceIds(
      Context ctx, Iterable<? extends BoundKeyspaceIdQuery> queries, TabletType tabletType)
      throws SQLException {
    if (!inTransaction()) {
      throw new SQLDataException("executeBatchKeyspaceIds: not in transaction");
    }
    ExecuteBatchKeyspaceIdsRequest.Builder requestBuilder =
        ExecuteBatchKeyspaceIdsRequest.newBuilder()
            .addAllQueries(queries)
            .setTabletType(tabletType)
            .setSession(session);
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
                setSession(response.getSession());
                Proto.checkError(response.getError());
                return Futures.<List<Cursor>>immediateFuture(
                    Proto.toCursorList(response.getResultsList()));
              }
            }));
  }

  public synchronized SQLFuture<Void> commit(Context ctx) throws SQLException {
    if (!inTransaction()) {
      throw new SQLDataException("commit: not in transaction");
    }
    CommitRequest.Builder requestBuilder = CommitRequest.newBuilder().setSession(session);
    if (ctx.getCallerId() != null) {
      requestBuilder.setCallerId(ctx.getCallerId());
    }
    return new SQLFuture<Void>(
        Futures.transformAsync(
            client.commit(ctx, requestBuilder.build()),
            new AsyncFunction<CommitResponse, Void>() {
              @Override
              public ListenableFuture<Void> apply(CommitResponse response) throws Exception {
                setSession(null);
                return Futures.<Void>immediateFuture(null);
              }
            }));
  }

  public synchronized SQLFuture<Void> rollback(Context ctx) throws SQLException {
    if (!inTransaction()) {
      throw new SQLDataException("rollback: not in transaction");
    }
    RollbackRequest.Builder requestBuilder = RollbackRequest.newBuilder().setSession(session);
    if (ctx.getCallerId() != null) {
      requestBuilder.setCallerId(ctx.getCallerId());
    }
    return new SQLFuture<Void>(
        Futures.transformAsync(
            client.rollback(ctx, requestBuilder.build()),
            new AsyncFunction<RollbackResponse, Void>() {
              @Override
              public ListenableFuture<Void> apply(RollbackResponse response) throws Exception {
                setSession(null);
                return Futures.<Void>immediateFuture(null);
              }
            }));
  }

  protected synchronized boolean inTransaction() {
    return session != null && session.getInTransaction();
  }

  protected synchronized void setSession(Session session) {
    this.session = session;
  }
}
