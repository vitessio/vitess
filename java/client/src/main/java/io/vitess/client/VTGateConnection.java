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

import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.vitess.client.cursor.Cursor;
import io.vitess.client.cursor.CursorWithError;
import io.vitess.client.cursor.SimpleCursor;
import io.vitess.client.cursor.StreamCursor;
import io.vitess.proto.Query;
import io.vitess.proto.Query.SplitQueryRequest.Algorithm;
import io.vitess.proto.Vtgate;
import io.vitess.proto.Vtgate.*;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.util.concurrent.Futures.transformAsync;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

public final class VTGateConnection implements Closeable {
    private final RpcClient client;
    private Session session;
    private SQLFuture<?> lastCall;

    public VTGateConnection(RpcClient client) {
        this(client, null, null);
    }

    public VTGateConnection(RpcClient client, String targetString) {
        this(client, targetString, null);
    }

    public VTGateConnection(RpcClient client, Query.ExecuteOptions options) {
        this(client, null, options);
    }

    public VTGateConnection(RpcClient client, String targetString, Query.ExecuteOptions options) {
        this.client = checkNotNull(client);
        this.session = Session.newBuilder()
                .setTargetString(targetString)
                .setOptions(options)
                .build();
    }

    public SQLFuture<Cursor> execute(Context ctx, String query, @Nullable Map<String, ?> bindVars) throws SQLException {
        checkCallIsAllowed("execute");
        ExecuteRequest.Builder requestBuilder = ExecuteRequest.newBuilder()
                .setQuery(Proto.bindQuery(checkNotNull(query), bindVars))
                .setSession(session);

        if (ctx.getCallerId() != null) {
            requestBuilder.setCallerId(ctx.getCallerId());
        }

        SQLFuture<Cursor> call = new SQLFuture<>(
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

    public SQLFuture<List<CursorWithError>> executeBatch(Context ctx, List<String> queryList, @Nullable List<Map<String, ?>> bindVarsList) throws SQLException {
        return executeBatch(ctx, queryList, bindVarsList, false);
    }

    public SQLFuture<List<CursorWithError>> executeBatch(Context ctx, List<String> queryList, @Nullable List<Map<String, ?>> bindVarsList, boolean asTransaction) throws SQLException {
        checkCallIsAllowed("executeBatch");
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
                        .setSession(session)
                        .setAsTransaction(asTransaction);

        if (ctx.getCallerId() != null) {
            requestBuilder.setCallerId(ctx.getCallerId());
        }

        SQLFuture<List<CursorWithError>> call = new SQLFuture<>(
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
        lastCall = call;
        return call;
    }

    public Cursor streamExecute(Context ctx, String query, @Nullable Map<String, ?> bindVars) throws SQLException {
        checkCallIsAllowed("streamExecute");
        StreamExecuteRequest.Builder requestBuilder =
                StreamExecuteRequest.newBuilder()
                        .setQuery(Proto.bindQuery(checkNotNull(query), bindVars))
                        .setSession(session);

        if (ctx.getCallerId() != null) {
            requestBuilder.setCallerId(ctx.getCallerId());
        }

        return new StreamCursor(client.streamExecute(ctx, requestBuilder.build()));
    }

    public SQLFuture<List<SplitQueryResponse.Part>> splitQuery(Context ctx, String keyspace, String query, @Nullable Map<String, ?> bindVars, Iterable<String> splitColumns,
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

    @Override
    public void close() throws IOException {
        client.close();
    }

    protected synchronized void checkCallIsAllowed(String call) throws SQLException {
        // Calls are not allowed to overlap.
        if (lastCall != null && !lastCall.isDone()) {
            throw new IllegalStateException("Can't call " + call
                    + "() on a VTGateTx instance until the last asynchronous call is done.");
        }
        // All calls must occur within a valid session.
        if (session == null) {
            throw new SQLDataException("Can't perform " + call + "() without a session.");
        }
    }

    protected synchronized void setSession(Session session) {
        this.session = session;
    }

}
