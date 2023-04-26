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

import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import io.vitess.client.cursor.Cursor;
import io.vitess.client.cursor.CursorWithError;
import io.vitess.client.cursor.SimpleCursor;
import io.vitess.client.cursor.StreamCursor;
import io.vitess.proto.Query;
import io.vitess.proto.Vtgate;
import io.vitess.proto.Vtgate.ExecuteRequest;
import io.vitess.proto.Vtgate.ExecuteResponse;
import io.vitess.proto.Vtgate.StreamExecuteRequest;
import io.vitess.proto.Vtgate.VStreamRequest;
import io.vitess.proto.Vtgate.VStreamResponse;

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
 * <p>
 * <p>All the information regarding this connection is maintained by {@code Session},
 * only one operation can be in flight at a time on a given instance. The methods are {@code
 * synchronized} only because the session cookie is updated asynchronously when the RPC response
 * comes back.</p>
 * <p>
 * <p>After calling any method that returns a {@link SQLFuture}, you must wait for that future to
 * complete before calling any other methods on that {@code VTGateConnection} instance. An {@link
 * IllegalStateException} will be thrown if this constraint is violated.</p>
 * <p>
 * <p>All non-streaming calls on {@code VTGateConnection} are asynchronous. Use {@link
 * VTGateBlockingConnection} if you want synchronous calls.</p>
 */
public class VTGateConnection implements Closeable {

  private final RpcClient client;

  /**
   * Creates a VTGate connection with no specific parameters.
   * <p>
   * <p>In this mode, VTGate will use VSchema to resolve the keyspace for any unprefixed
   * table names. Note that this only works if the table name is unique across all keyspaces.</p>
   *
   * @param client RPC connection
   */
  public VTGateConnection(RpcClient client) {
    this.client = checkNotNull(client);
  }

  /**
   * This method calls the VTGate to execute the query.
   *
   * @param ctx Context on user and execution deadline if any.
   * @param query Sql Query to be executed.
   * @param bindVars Parameters to bind with sql.
   * @param vtSession Session to be used with the call.
   * @return SQL Future Cursor
   * @throws SQLException If anything fails on query execution.
   */
  public SQLFuture<Cursor> execute(Context ctx, String query, @Nullable Map<String, ?> bindVars,
      final VTSession vtSession) throws SQLException {
    vtSession.checkCallIsAllowed("execute");
    ExecuteRequest.Builder requestBuilder = ExecuteRequest.newBuilder()
        .setQuery(Proto.bindQuery(checkNotNull(query), bindVars))
        .setSession(vtSession.getSession());

    if (ctx.getCallerId() != null) {
      requestBuilder.setCallerId(ctx.getCallerId());
    }

    SQLFuture<Cursor> call = new SQLFuture<>(
        transformAsync(client.execute(ctx, requestBuilder.build()),
            new AsyncFunction<ExecuteResponse, Cursor>() {
              @Override
              public ListenableFuture<Cursor> apply(ExecuteResponse response) throws Exception {
                vtSession.setSession(response.getSession());
                Proto.checkError(response.getError());
                return Futures.<Cursor>immediateFuture(new SimpleCursor(response.getResult()));
              }
            }, directExecutor()));
    vtSession.setLastCall(call);
    return call;
  }

  /**
   * This method calls the VTGate to execute list of queries as a batch.
   * <p>
   *
   * @param ctx Context on user and execution deadline if any.
   * @param queryList List of sql queries to be executed.
   * @param bindVarsList <p>For each sql query it will provide a list of parameters to bind with. If
   * provided, should match the number of sql queries.</p>
   * @param vtSession Session to be used with the call.
   * @return SQL Future with List of Cursors
   * @throws SQLException If anything fails on query execution.
   */
  public SQLFuture<List<CursorWithError>> executeBatch(Context ctx, List<String> queryList,
      @Nullable List<Map<String, ?>> bindVarsList, final VTSession vtSession)
      throws SQLException {
    vtSession.checkCallIsAllowed("executeBatch");
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
            .setSession(vtSession.getSession());

    if (ctx.getCallerId() != null) {
      requestBuilder.setCallerId(ctx.getCallerId());
    }

    SQLFuture<List<CursorWithError>> call = new SQLFuture<>(
        transformAsync(client.executeBatch(ctx, requestBuilder.build()),
            new AsyncFunction<Vtgate.ExecuteBatchResponse, List<CursorWithError>>() {
              @Override
              public ListenableFuture<List<CursorWithError>> apply(
                  Vtgate.ExecuteBatchResponse response) throws Exception {
                vtSession.setSession(response.getSession());
                Proto.checkError(response.getError());
                return Futures.immediateFuture(
                    Proto.fromQueryResponsesToCursorList(response.getResultsList()));
              }
            }, directExecutor()));
    vtSession.setLastCall(call);
    return call;
  }

  /**
   * @param ctx Context on user and execution deadline if any.
   * @param query Sql Query to be executed.
   * @param bindVars Parameters to bind with sql.
   * @param vtSession Session to be used with the call.
   */
  public Cursor streamExecute(Context ctx, String query, @Nullable Map<String, ?> bindVars,
      VTSession vtSession) throws SQLException {
    StreamExecuteRequest.Builder requestBuilder =
        StreamExecuteRequest.newBuilder()
            .setQuery(Proto.bindQuery(checkNotNull(query), bindVars))
            .setSession(vtSession.getSession());

    if (ctx.getCallerId() != null) {
      requestBuilder.setCallerId(ctx.getCallerId());
    }

    return new StreamCursor(client.streamExecute(ctx, requestBuilder.build()));
  }

  /**
   * Starts streaming the vstream binlog events.
   *
   * @param ctx Context on user and execution deadline if any.
   * @param vstreamRequest VStreamRequest containing starting VGtid positions
   *                       in binlog and optional Filters
   * @return Streaming iterator over VStream events
   * @throws SQLException If anything fails on query execution.
   */
  StreamIterator<VStreamResponse> getVStream(Context ctx, VStreamRequest vstreamRequest)
    throws SQLException {
    VStreamRequest request = vstreamRequest;

    if (ctx.getCallerId() != null) {
      request = request.toBuilder().setCallerId(ctx.getCallerId()).build();
    }

    return client.getVStream(ctx, request);
  }

  /**
   * @inheritDoc
   */
  @Override
  public void close() throws IOException {
    client.close();
  }

  @Override
  public String toString() {
    return String.format("[VTGateConnection-%s client=%s]",
        Integer.toHexString(this.hashCode()),
        client.toString()
    );
  }
}
