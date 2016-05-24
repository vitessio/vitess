package com.youtube.vitess.client;

import static com.google.common.base.Preconditions.checkNotNull;

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
 * An asynchronous VTGate transaction session.
 *
 * <p>
 * Because {@code VTGateTx} manages a session cookie, only one operation can be in flight at a time
 * on a given instance. The methods are {@code synchronized} only because the session cookie is
 * updated asynchronously when the RPC response comes back.
 *
 * <p>
 * After calling any method that returns a {@link SQLFuture}, you must wait for that future to
 * complete before calling any other methods on that {@code VTGateTx} instance. An
 * {@link IllegalStateException} will be thrown if this constraint is violated.
 *
 * <p>
 * All operations on {@code VTGateTx} are asynchronous, including those whose ultimate return type
 * is {@link Void}, such as {@link #commit(Context)} and {@link #rollback(Context)}. You must still
 * wait for the futures returned by these methods to complete and check the error on them (such as
 * by calling {@code checkedGet()} before you can assume the operation has finished successfully.
 *
 * <p>
 * If you prefer a synchronous API, you can use {@link VTGateBlockingConn#begin(Context)}, which
 * returns a {@link VTGateBlockingTx} instead.
 */
public class VTGateTx {
  private final RpcClient client;
  private final String keyspace;
  private Session session;
  private SQLFuture<?> lastCall;

  VTGateTx(RpcClient client, Session session, String keyspace) {
    this.client = checkNotNull(client);
    this.keyspace = checkNotNull(keyspace);
    setSession(checkNotNull(session));
  }

  public synchronized SQLFuture<Cursor> execute(Context ctx, String query, Map<String, ?> bindVars,
      TabletType tabletType) throws SQLException {
    checkCallIsAllowed("execute");
    ExecuteRequest.Builder requestBuilder =
        ExecuteRequest.newBuilder().setQuery(Proto.bindQuery(query, bindVars)).setKeyspace(keyspace)
            .setTabletType(tabletType).setSession(session);
    if (ctx.getCallerId() != null) {
      requestBuilder.setCallerId(ctx.getCallerId());
    }
    SQLFuture<Cursor> call =
        new SQLFuture<>(Futures.transformAsync(client.execute(ctx, requestBuilder.build()),
            new AsyncFunction<ExecuteResponse, Cursor>() {
              @Override
              public ListenableFuture<Cursor> apply(ExecuteResponse response) throws Exception {
                setSession(response.getSession());
                Proto.checkError(response.getError());
                return Futures.<Cursor>immediateFuture(new SimpleCursor(response.getResult()));
              }
            }));
    lastCall = call;
    return call;
  }

  public synchronized SQLFuture<Cursor> executeShards(Context ctx, String query, String keyspace,
      Iterable<String> shards, Map<String, ?> bindVars, TabletType tabletType) throws SQLException {
    checkCallIsAllowed("executeShards");
    ExecuteShardsRequest.Builder requestBuilder = ExecuteShardsRequest.newBuilder()
        .setQuery(Proto.bindQuery(query, bindVars)).setKeyspace(keyspace).addAllShards(shards)
        .setTabletType(tabletType).setSession(session);
    if (ctx.getCallerId() != null) {
      requestBuilder.setCallerId(ctx.getCallerId());
    }
    SQLFuture<Cursor> call =
        new SQLFuture<>(Futures.transformAsync(client.executeShards(ctx, requestBuilder.build()),
            new AsyncFunction<ExecuteShardsResponse, Cursor>() {
              @Override
              public ListenableFuture<Cursor> apply(ExecuteShardsResponse response)
                  throws Exception {
                setSession(response.getSession());
                Proto.checkError(response.getError());
                return Futures.<Cursor>immediateFuture(new SimpleCursor(response.getResult()));
              }
            }));
    lastCall = call;
    return call;
  }

  public synchronized SQLFuture<Cursor> executeKeyspaceIds(Context ctx, String query,
      String keyspace, Iterable<byte[]> keyspaceIds, Map<String, ?> bindVars, TabletType tabletType)
      throws SQLException {
    checkCallIsAllowed("executeKeyspaceIds");
    ExecuteKeyspaceIdsRequest.Builder requestBuilder = ExecuteKeyspaceIdsRequest.newBuilder()
        .setQuery(Proto.bindQuery(query, bindVars)).setKeyspace(keyspace)
        .addAllKeyspaceIds(Iterables.transform(keyspaceIds, Proto.BYTE_ARRAY_TO_BYTE_STRING))
        .setTabletType(tabletType).setSession(session);
    if (ctx.getCallerId() != null) {
      requestBuilder.setCallerId(ctx.getCallerId());
    }
    SQLFuture<Cursor> call = new SQLFuture<>(
        Futures.transformAsync(client.executeKeyspaceIds(ctx, requestBuilder.build()),
            new AsyncFunction<ExecuteKeyspaceIdsResponse, Cursor>() {
              @Override
              public ListenableFuture<Cursor> apply(ExecuteKeyspaceIdsResponse response)
                  throws Exception {
                setSession(response.getSession());
                Proto.checkError(response.getError());
                return Futures.<Cursor>immediateFuture(new SimpleCursor(response.getResult()));
              }
            }));
    lastCall = call;
    return call;
  }

  public synchronized SQLFuture<Cursor> executeKeyRanges(Context ctx, String query, String keyspace,
      Iterable<? extends KeyRange> keyRanges, Map<String, ?> bindVars, TabletType tabletType)
      throws SQLException {
    checkCallIsAllowed("executeKeyRanges");
    ExecuteKeyRangesRequest.Builder requestBuilder = ExecuteKeyRangesRequest.newBuilder()
        .setQuery(Proto.bindQuery(query, bindVars)).setKeyspace(keyspace).addAllKeyRanges(keyRanges)
        .setTabletType(tabletType).setSession(session);
    if (ctx.getCallerId() != null) {
      requestBuilder.setCallerId(ctx.getCallerId());
    }
    SQLFuture<Cursor> call =
        new SQLFuture<>(Futures.transformAsync(client.executeKeyRanges(ctx, requestBuilder.build()),
            new AsyncFunction<ExecuteKeyRangesResponse, Cursor>() {
              @Override
              public ListenableFuture<Cursor> apply(ExecuteKeyRangesResponse response)
                  throws Exception {
                setSession(response.getSession());
                Proto.checkError(response.getError());
                return Futures.<Cursor>immediateFuture(new SimpleCursor(response.getResult()));
              }
            }));
    lastCall = call;
    return call;
  }

  public synchronized SQLFuture<Cursor> executeEntityIds(Context ctx, String query, String keyspace,
      String entityColumnName, Map<byte[], ?> entityKeyspaceIds, Map<String, ?> bindVars,
      TabletType tabletType) throws SQLException {
    checkCallIsAllowed("executeEntityIds");
    ExecuteEntityIdsRequest.Builder requestBuilder = ExecuteEntityIdsRequest.newBuilder()
        .setQuery(Proto.bindQuery(query, bindVars)).setKeyspace(keyspace)
        .setEntityColumnName(entityColumnName).addAllEntityKeyspaceIds(Iterables
            .transform(entityKeyspaceIds.entrySet(), Proto.MAP_ENTRY_TO_ENTITY_KEYSPACE_ID))
        .setTabletType(tabletType).setSession(session);
    if (ctx.getCallerId() != null) {
      requestBuilder.setCallerId(ctx.getCallerId());
    }
    SQLFuture<Cursor> call =
        new SQLFuture<>(Futures.transformAsync(client.executeEntityIds(ctx, requestBuilder.build()),
            new AsyncFunction<ExecuteEntityIdsResponse, Cursor>() {
              @Override
              public ListenableFuture<Cursor> apply(ExecuteEntityIdsResponse response)
                  throws Exception {
                setSession(response.getSession());
                Proto.checkError(response.getError());
                return Futures.<Cursor>immediateFuture(new SimpleCursor(response.getResult()));
              }
            }));
    lastCall = call;
    return call;
  }

  public synchronized SQLFuture<List<Cursor>> executeBatchShards(Context ctx,
      Iterable<? extends BoundShardQuery> queries, TabletType tabletType) throws SQLException {
    checkCallIsAllowed("executeBatchShards");
    ExecuteBatchShardsRequest.Builder requestBuilder = ExecuteBatchShardsRequest.newBuilder()
        .addAllQueries(queries).setTabletType(tabletType).setSession(session);
    if (ctx.getCallerId() != null) {
      requestBuilder.setCallerId(ctx.getCallerId());
    }
    SQLFuture<List<Cursor>> call = new SQLFuture<>(
        Futures.transformAsync(client.executeBatchShards(ctx, requestBuilder.build()),
            new AsyncFunction<ExecuteBatchShardsResponse, List<Cursor>>() {
              @Override
              public ListenableFuture<List<Cursor>> apply(ExecuteBatchShardsResponse response)
                  throws Exception {
                setSession(response.getSession());
                Proto.checkError(response.getError());
                return Futures
                    .<List<Cursor>>immediateFuture(Proto.toCursorList(response.getResultsList()));
              }
            }));
    lastCall = call;
    return call;
  }

  public synchronized SQLFuture<List<Cursor>> executeBatchKeyspaceIds(Context ctx,
      Iterable<? extends BoundKeyspaceIdQuery> queries, TabletType tabletType) throws SQLException {
    checkCallIsAllowed("executeBatchKeyspaceIds");
    ExecuteBatchKeyspaceIdsRequest.Builder requestBuilder = ExecuteBatchKeyspaceIdsRequest
        .newBuilder().addAllQueries(queries).setTabletType(tabletType).setSession(session);
    if (ctx.getCallerId() != null) {
      requestBuilder.setCallerId(ctx.getCallerId());
    }
    SQLFuture<List<Cursor>> call = new SQLFuture<>(
        Futures.transformAsync(client.executeBatchKeyspaceIds(ctx, requestBuilder.build()),
            new AsyncFunction<ExecuteBatchKeyspaceIdsResponse, List<Cursor>>() {
              @Override
              public ListenableFuture<List<Cursor>> apply(ExecuteBatchKeyspaceIdsResponse response)
                  throws Exception {
                setSession(response.getSession());
                Proto.checkError(response.getError());
                return Futures
                    .<List<Cursor>>immediateFuture(Proto.toCursorList(response.getResultsList()));
              }
            }));
    lastCall = call;
    return call;
  }

  public synchronized SQLFuture<Void> commit(Context ctx) throws SQLException {
    checkCallIsAllowed("commit");
    CommitRequest.Builder requestBuilder = CommitRequest.newBuilder().setSession(session);
    if (ctx.getCallerId() != null) {
      requestBuilder.setCallerId(ctx.getCallerId());
    }
    SQLFuture<Void> call =
        new SQLFuture<>(Futures.transformAsync(client.commit(ctx, requestBuilder.build()),
            new AsyncFunction<CommitResponse, Void>() {
              @Override
              public ListenableFuture<Void> apply(CommitResponse response) throws Exception {
                setSession(null);
                return Futures.<Void>immediateFuture(null);
              }
            }));
    lastCall = call;
    return call;
  }

  public synchronized SQLFuture<Void> rollback(Context ctx) throws SQLException {
    checkCallIsAllowed("rollback");
    RollbackRequest.Builder requestBuilder = RollbackRequest.newBuilder().setSession(session);
    if (ctx.getCallerId() != null) {
      requestBuilder.setCallerId(ctx.getCallerId());
    }
    SQLFuture<Void> call =
        new SQLFuture<>(Futures.transformAsync(client.rollback(ctx, requestBuilder.build()),
            new AsyncFunction<RollbackResponse, Void>() {
              @Override
              public ListenableFuture<Void> apply(RollbackResponse response) throws Exception {
                setSession(null);
                return Futures.<Void>immediateFuture(null);
              }
            }));
    lastCall = call;
    return call;
  }

  protected synchronized void checkCallIsAllowed(String call) throws SQLException {
    // Calls are not allowed to overlap.
    if (lastCall != null && !lastCall.isDone()) {
      throw new IllegalStateException("Can't call " + call
          + "() on a VTGateTx instance until the last asynchronous call is done.");
    }
    // All calls must occur within a valid transaction.
    if (session == null || !session.getInTransaction()) {
      throw new SQLDataException("Can't perform " + call + "() while not in transaction.");
    }
  }

  protected synchronized void setSession(Session session) {
    this.session = session;
  }
}
