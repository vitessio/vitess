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

import io.vitess.client.cursor.Cursor;
import io.vitess.client.cursor.CursorWithError;
import io.vitess.proto.Query.SplitQueryRequest.Algorithm;
import io.vitess.proto.Vtgate.SplitQueryResponse;

import java.io.Closeable;
import java.io.IOException;
import java.sql.SQLException;
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
 * VTGateBlockingConnection} if
 * you want synchronous calls.</p>
 */
public final class VTGateBlockingConnection implements Closeable {

  private final VTGateConnection vtGateConnection;

  /**
   * Creates a VTGate connection with no specific parameters.
   * <p>
   * <p>In this mode, VTGate will use VSchema to resolve the keyspace for any unprefixed
   * table names. Note that this only works if the table name is unique across all keyspaces.</p>
   *
   * @param client RPC connection
   */
  public VTGateBlockingConnection(RpcClient client) {
    vtGateConnection = new VTGateConnection(client);
  }

  /**
   * This method calls the VTGate to execute the query.
   *
   * @param ctx Context on user and execution deadline if any.
   * @param query Sql Query to be executed.
   * @param bindVars Parameters to bind with sql.
   * @param vtSession Session to be used with the call.
   * @return Cursor
   * @throws SQLException If anything fails on query execution.
   */
  public Cursor execute(Context ctx,
      String query,
      @Nullable Map<String, ?> bindVars,
      final VTSession vtSession) throws SQLException {
    return vtGateConnection.execute(ctx, query, bindVars, vtSession).checkedGet();
  }

  /**
   * This method calls the VTGate to execute list of queries as a batch.
   *
   * @param ctx Context on user and execution deadline if any.
   * @param queryList List of sql queries to be executed.
   * @param bindVarsList <p>For each sql query it will provide a list of parameters to bind with. If
   * provided, should match the number of sql queries.</p>
   * @param vtSession Session to be used with the call.
   * @return List of Cursors
   * @throws SQLException If anything fails on query execution.
   */
  public List<CursorWithError> executeBatch(Context ctx,
      List<String> queryList,
      @Nullable List<Map<String, ?>> bindVarsList,
      final VTSession vtSession) throws SQLException {
    return vtGateConnection.executeBatch(ctx, queryList, bindVarsList, vtSession).checkedGet();
  }

  /**
   * This method calls the VTGate to execute list of queries as a batch.
   * <p>
   * <p>If asTransaction is set to <code>true</code> then query execution will not change the
   * session cookie.
   * Otherwise, query execution will become part of the session.</p>
   *
   * @param ctx Context on user and execution deadline if any.
   * @param queryList List of sql queries to be executed.
   * @param bindVarsList <p>For each sql query it will provide a list of parameters to bind with. If
   * provided, should match the number of sql queries.</p>
   * @param asTransaction To execute query without impacting session cookie.
   * @param vtSession Session to be used with the call.
   * @return List of Cursors
   * @throws SQLException If anything fails on query execution.
   */
  public List<CursorWithError> executeBatch(Context ctx,
      List<String> queryList,
      @Nullable List<Map<String, ?>> bindVarsList,
      boolean asTransaction,
      final VTSession vtSession) throws SQLException {
    return vtGateConnection.executeBatch(ctx, queryList, bindVarsList, asTransaction, vtSession)
        .checkedGet();
  }

  /**
   * This method should be used execute select query to return response as a stream.
   *
   * @param ctx Context on user and execution deadline if any.
   * @param query Sql Query to be executed.
   * @param bindVars Parameters to bind with sql.
   * @param vtSession Session to be used with the call.
   * @return Cursor
   * @throws SQLException Returns SQLException if there is any failure on VTGate.
   */
  public Cursor streamExecute(Context ctx,
      String query,
      @Nullable Map<String, ?> bindVars,
      VTSession vtSession) throws SQLException {
    return vtGateConnection.streamExecute(ctx, query, bindVars, vtSession);
  }

  /**
   * This method splits the query into small parts based on the splitColumn and Algorithm type
   * provided.
   *
   * @param ctx Context on user and execution deadline if any.
   * @param keyspace Keyspace to execute the query on.
   * @param query Sql Query to be executed.
   * @param bindVars Parameters to bind with sql.
   * @param splitColumns Column to be used to split the data.
   * @param splitCount Number of Partitions
   * @param numRowsPerQueryPart Limit the number of records per query part.
   * @param algorithm EQUAL_SPLITS or FULL_SCAN
   * @return Query Parts
   * @throws SQLException If anything fails on query execution.
   */
  public List<SplitQueryResponse.Part> splitQuery(Context ctx,
      String keyspace,
      String query,
      @Nullable Map<String, ?> bindVars,
      Iterable<String> splitColumns,
      int splitCount,
      int numRowsPerQueryPart,
      Algorithm algorithm) throws SQLException {
    return vtGateConnection
        .splitQuery(ctx, keyspace, query, bindVars, splitColumns, splitCount, numRowsPerQueryPart,
            algorithm).checkedGet();
  }

  /**
   * @inheritDoc
   */
  @Override
  public void close() throws IOException {
    vtGateConnection.close();
  }

}
