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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.util.concurrent.Futures.transformAsync;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

import io.vitess.client.cursor.Cursor;
import io.vitess.client.cursor.CursorWithError;
import io.vitess.client.cursor.SimpleCursor;
import io.vitess.proto.Query;
import io.vitess.proto.Topodata.KeyRange;
import io.vitess.proto.Topodata.TabletType;
import io.vitess.proto.Vtgate;
import io.vitess.proto.Vtgate.BoundKeyspaceIdQuery;
import io.vitess.proto.Vtgate.BoundShardQuery;
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
import io.vitess.proto.Vtgate.RollbackRequest;
import io.vitess.proto.Vtgate.RollbackResponse;
import io.vitess.proto.Vtgate.Session;

/**
 * An asynchronous VTGate transaction session.
 *
 * <p>Because {@code VTGateTx} manages a session cookie, only one operation can be in flight at a
 * time on a given instance. The methods are {@code synchronized} only because the session cookie is
 * updated asynchronously when the RPC response comes back.
 *
 * <p>After calling any method that returns a {@link SQLFuture}, you must wait for that future to
 * complete before calling any other methods on that {@code VTGateTx} instance. An {@link
 * IllegalStateException} will be thrown if this constraint is violated.
 *
 * <p>All operations on {@code VTGateTx} are asynchronous, including those whose ultimate return
 * type is {@link Void}, such as {@link #commit(Context)} and {@link #rollback(Context)}. You must
 * still wait for the futures returned by these methods to complete and check the error on them
 * (such as by calling {@code checkedGet()} before you can assume the operation has finished
 * successfully.
 *
 * <p>If you prefer a synchronous API, you can use {@link VTGateBlockingConn#begin(Context)}, which
 * returns a {@link VTGateBlockingTx} instead.
 */
@Deprecated
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
      TabletType tabletType, Query.ExecuteOptions.IncludedFields includedFields) throws SQLException {
    checkCallIsAllowed("execute");
    ExecuteRequest.Builder requestBuilder =
        ExecuteRequest.newBuilder()
            .setQuery(Proto.bindQuery(query, bindVars))
            .setKeyspaceShard(keyspace)
            .setTabletType(tabletType)
            .setSession(session)
            .setOptions(Query.ExecuteOptions.newBuilder()
                .setIncludedFields(includedFields));

    if (ctx.getCallerId() != null) {
      requestBuilder.setCallerId(ctx.getCallerId());
    }

    SQLFuture<Cursor> call =
        new SQLFuture<>(
            transformAsync(
                client.execute(ctx, requestBuilder.build()),
                new AsyncFunction<ExecuteResponse, Cursor>() {
                  @Override
                  public ListenableFuture<Cursor> apply(ExecuteResponse response) throws Exception {
                    setSession(response.getSession());
                    Proto.checkError(response.getError());
                    return Futures.<Cursor>immediateFuture(new SimpleCursor(response.getResult()));
                  }
                },
                directExecutor()));
    lastCall = call;
    return call;
  }

  public synchronized SQLFuture<Cursor> executeShards(Context ctx, String query, String keyspace,
      Iterable<String> shards, Map<String, ?> bindVars, TabletType tabletType,
      Query.ExecuteOptions.IncludedFields includedFields) throws SQLException {
    checkCallIsAllowed("executeShards");
    ExecuteShardsRequest.Builder requestBuilder = ExecuteShardsRequest.newBuilder()
        .setQuery(Proto.bindQuery(query, bindVars))
        .setKeyspace(keyspace)
        .addAllShards(shards)
        .setTabletType(tabletType)
        .setSession(session)
        .setOptions(Query.ExecuteOptions.newBuilder()
            .setIncludedFields(includedFields));

    if (ctx.getCallerId() != null) {
      requestBuilder.setCallerId(ctx.getCallerId());
    }

    SQLFuture<Cursor> call =
        new SQLFuture<>(
            transformAsync(
                client.executeShards(ctx, requestBuilder.build()),
                new AsyncFunction<ExecuteShardsResponse, Cursor>() {
                  @Override
                  public ListenableFuture<Cursor> apply(ExecuteShardsResponse response)
                      throws Exception {
                    setSession(response.getSession());
                    Proto.checkError(response.getError());
                    return Futures.<Cursor>immediateFuture(new SimpleCursor(response.getResult()));
                  }
                },
                directExecutor()));
    lastCall = call;
    return call;
  }

  public synchronized SQLFuture<Cursor> executeKeyspaceIds(Context ctx, String query,
      String keyspace, Iterable<byte[]> keyspaceIds, Map<String, ?> bindVars, TabletType tabletType,
      Query.ExecuteOptions.IncludedFields includedFields)
      throws SQLException {
    checkCallIsAllowed("executeKeyspaceIds");
    ExecuteKeyspaceIdsRequest.Builder requestBuilder = ExecuteKeyspaceIdsRequest.newBuilder()
        .setQuery(Proto.bindQuery(query, bindVars))
        .setKeyspace(keyspace)
        .addAllKeyspaceIds(Iterables.transform(keyspaceIds, Proto.BYTE_ARRAY_TO_BYTE_STRING))
        .setTabletType(tabletType)
        .setSession(session)
        .setOptions(Query.ExecuteOptions.newBuilder()
            .setIncludedFields(includedFields));

    if (ctx.getCallerId() != null) {
      requestBuilder.setCallerId(ctx.getCallerId());
    }

    SQLFuture<Cursor> call =
        new SQLFuture<>(
            transformAsync(
                client.executeKeyspaceIds(ctx, requestBuilder.build()),
                new AsyncFunction<ExecuteKeyspaceIdsResponse, Cursor>() {
                  @Override
                  public ListenableFuture<Cursor> apply(ExecuteKeyspaceIdsResponse response)
                      throws Exception {
                    setSession(response.getSession());
                    Proto.checkError(response.getError());
                    return Futures.<Cursor>immediateFuture(new SimpleCursor(response.getResult()));
                  }
                },
                directExecutor()));
    lastCall = call;
    return call;
  }

  public synchronized SQLFuture<Cursor> executeKeyRanges(Context ctx, String query, String keyspace,
      Iterable<? extends KeyRange> keyRanges, Map<String, ?> bindVars, TabletType tabletType,
      Query.ExecuteOptions.IncludedFields includedFields)
      throws SQLException {
    checkCallIsAllowed("executeKeyRanges");
    ExecuteKeyRangesRequest.Builder requestBuilder = ExecuteKeyRangesRequest.newBuilder()
        .setQuery(Proto.bindQuery(query, bindVars))
        .setKeyspace(keyspace)
        .addAllKeyRanges(keyRanges)
        .setTabletType(tabletType)
        .setSession(session)
        .setOptions(Query.ExecuteOptions.newBuilder()
            .setIncludedFields(includedFields));

    if (ctx.getCallerId() != null) {
      requestBuilder.setCallerId(ctx.getCallerId());
    }

    SQLFuture<Cursor> call =
        new SQLFuture<>(
            transformAsync(
                client.executeKeyRanges(ctx, requestBuilder.build()),
                new AsyncFunction<ExecuteKeyRangesResponse, Cursor>() {
                  @Override
                  public ListenableFuture<Cursor> apply(ExecuteKeyRangesResponse response)
                      throws Exception {
                    setSession(response.getSession());
                    Proto.checkError(response.getError());
                    return Futures.<Cursor>immediateFuture(new SimpleCursor(response.getResult()));
                  }
                },
                directExecutor()));
    lastCall = call;
    return call;
  }

  public synchronized SQLFuture<Cursor> executeEntityIds(Context ctx, String query, String keyspace,
      String entityColumnName, Map<byte[], ?> entityKeyspaceIds, Map<String, ?> bindVars,
      TabletType tabletType, Query.ExecuteOptions.IncludedFields includedFields) throws SQLException {
    checkCallIsAllowed("executeEntityIds");
    ExecuteEntityIdsRequest.Builder requestBuilder = ExecuteEntityIdsRequest.newBuilder()
        .setQuery(Proto.bindQuery(query, bindVars))
        .setKeyspace(keyspace)
        .setEntityColumnName(entityColumnName)
        .addAllEntityKeyspaceIds(Iterables
            .transform(entityKeyspaceIds.entrySet(), Proto.MAP_ENTRY_TO_ENTITY_KEYSPACE_ID))
        .setTabletType(tabletType)
        .setSession(session)
        .setOptions(Query.ExecuteOptions.newBuilder()
            .setIncludedFields(includedFields));

    if (ctx.getCallerId() != null) {
      requestBuilder.setCallerId(ctx.getCallerId());
    }

    SQLFuture<Cursor> call =
        new SQLFuture<>(
            transformAsync(
                client.executeEntityIds(ctx, requestBuilder.build()),
                new AsyncFunction<ExecuteEntityIdsResponse, Cursor>() {
                  @Override
                  public ListenableFuture<Cursor> apply(ExecuteEntityIdsResponse response)
                      throws Exception {
                    setSession(response.getSession());
                    Proto.checkError(response.getError());
                    return Futures.<Cursor>immediateFuture(new SimpleCursor(response.getResult()));
                  }
                },
                directExecutor()));
    lastCall = call;
    return call;
  }

    public SQLFuture<List<CursorWithError>> executeBatch(Context ctx, List<String> queryList,
        @Nullable List<Map<String, ?>> bindVarsList, TabletType tabletType,
        Query.ExecuteOptions.IncludedFields includedFields)
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
                .setSession(session)
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
                setSession(response.getSession());
                Proto.checkError(response.getError());
                return Futures.immediateFuture(
                    Proto.fromQueryResponsesToCursorList(response.getResultsList()));
              }
            },
            directExecutor()));
    }

  public synchronized SQLFuture<List<Cursor>> executeBatchShards(Context ctx,
      Iterable<? extends BoundShardQuery> queries, TabletType tabletType,
      Query.ExecuteOptions.IncludedFields includedFields) throws SQLException {
    checkCallIsAllowed("executeBatchShards");
    ExecuteBatchShardsRequest.Builder requestBuilder = ExecuteBatchShardsRequest.newBuilder()
        .addAllQueries(queries)
        .setTabletType(tabletType)
        .setSession(session)
        .setOptions(Query.ExecuteOptions.newBuilder()
            .setIncludedFields(includedFields));

    if (ctx.getCallerId() != null) {
      requestBuilder.setCallerId(ctx.getCallerId());
    }

    SQLFuture<List<Cursor>> call =
        new SQLFuture<>(
            transformAsync(
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
                },
                directExecutor()));
    lastCall = call;
    return call;
  }

  public synchronized SQLFuture<List<Cursor>> executeBatchKeyspaceIds(Context ctx,
      Iterable<? extends BoundKeyspaceIdQuery> queries, TabletType tabletType,
      Query.ExecuteOptions.IncludedFields includedFields) throws SQLException {
    checkCallIsAllowed("executeBatchKeyspaceIds");
    ExecuteBatchKeyspaceIdsRequest.Builder requestBuilder = ExecuteBatchKeyspaceIdsRequest
        .newBuilder()
        .addAllQueries(queries)
        .setTabletType(tabletType)
        .setSession(session)
        .setOptions(Query.ExecuteOptions.newBuilder()
            .setIncludedFields(includedFields));

    if (ctx.getCallerId() != null) {
      requestBuilder.setCallerId(ctx.getCallerId());
    }

    SQLFuture<List<Cursor>> call =
        new SQLFuture<>(
            transformAsync(
                client.executeBatchKeyspaceIds(ctx, requestBuilder.build()),
                new AsyncFunction<ExecuteBatchKeyspaceIdsResponse, List<Cursor>>() {
                  @Override
                  public ListenableFuture<List<Cursor>> apply(
                      ExecuteBatchKeyspaceIdsResponse response) throws Exception {
                    setSession(response.getSession());
                    Proto.checkError(response.getError());
                    return Futures.<List<Cursor>>immediateFuture(
                        Proto.toCursorList(response.getResultsList()));
                  }
                },
                directExecutor()));
    lastCall = call;
    return call;
  }

    public synchronized SQLFuture<Void> commit(Context ctx) throws SQLException {
        return commit(ctx, false);
    }

  public synchronized SQLFuture<Void> commit(Context ctx, boolean atomic) throws SQLException {
    checkCallIsAllowed("commit");
    CommitRequest.Builder requestBuilder = CommitRequest.newBuilder().setSession(session)
                                                        .setAtomic(atomic);
    if (ctx.getCallerId() != null) {
      requestBuilder.setCallerId(ctx.getCallerId());
    }
    SQLFuture<Void> call =
        new SQLFuture<>(
            transformAsync(
                client.commit(ctx, requestBuilder.build()),
                new AsyncFunction<CommitResponse, Void>() {
                  @Override
                  public ListenableFuture<Void> apply(CommitResponse response) throws Exception {
                    setSession(null);
                    return Futures.<Void>immediateFuture(null);
                  }
                },
                directExecutor()));
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
        new SQLFuture<>(
            transformAsync(
                client.rollback(ctx, requestBuilder.build()),
                new AsyncFunction<RollbackResponse, Void>() {
                  @Override
                  public ListenableFuture<Void> apply(RollbackResponse response) throws Exception {
                    setSession(null);
                    return Futures.<Void>immediateFuture(null);
                  }
                },
                directExecutor()));
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
