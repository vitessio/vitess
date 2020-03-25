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
import io.vitess.proto.Query;
import io.vitess.proto.Topodata.TabletType;

import java.io.Closeable;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

/**
 * A synchronous wrapper around a VTGate connection.
 *
 * <p>
 * This is a wrapper around the asynchronous {@link VTGateConn} class that converts all methods to
 * synchronous.
 */
@Deprecated
public class VTGateBlockingConn implements Closeable {

  private final VTGateConn conn;

  /**
   * Creates a new {@link VTGateConn} with the given {@link RpcClient} and wraps it in a synchronous
   * API.
   */
  public VTGateBlockingConn(RpcClient client) {
    conn = new VTGateConn(client);
  }

  /**
   * Creates a new {@link VTGateConn} with the given {@link RpcClient} and wraps it in a synchronous
   * API.
   */
  public VTGateBlockingConn(RpcClient client, String keyspace) {
    conn = new VTGateConn(client, keyspace);
  }

  /**
   * Wraps an existing {@link VTGateConn} in a synchronous API.
   */
  public VTGateBlockingConn(VTGateConn conn) {
    this.conn = conn;
  }

  public Cursor execute(Context ctx, String query, Map<String, ?> bindVars, TabletType tabletType,
      Query.ExecuteOptions.IncludedFields includedFields)
      throws SQLException {
    return conn.execute(ctx, query, bindVars, tabletType, includedFields).checkedGet();
  }

  public List<CursorWithError> executeBatch(Context ctx, ArrayList<String> queryList,
      @Nullable ArrayList<Map<String, ?>> bindVarsList, TabletType tabletType,
      Query.ExecuteOptions.IncludedFields includedFields) throws SQLException {
    return executeBatch(ctx, queryList, bindVarsList, tabletType, false, includedFields);
  }

  public List<CursorWithError> executeBatch(Context ctx, ArrayList<String> queryList,
      @Nullable ArrayList<Map<String, ?>> bindVarsList, TabletType tabletType,
      boolean asTransaction, Query.ExecuteOptions.IncludedFields includedFields)
      throws SQLException {
    return conn
        .executeBatch(ctx, queryList, bindVarsList, tabletType, asTransaction, includedFields)
        .checkedGet();
  }

  public Cursor streamExecute(Context ctx, String query, Map<String, ?> bindVars,
      TabletType tabletType, Query.ExecuteOptions.IncludedFields includedFields)
      throws SQLException {
    return conn.streamExecute(ctx, query, bindVars, tabletType, includedFields);
  }

  @Override
  public void close() throws IOException {
    conn.close();
  }
}
