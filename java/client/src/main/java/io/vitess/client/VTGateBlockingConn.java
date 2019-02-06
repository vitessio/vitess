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

import io.vitess.client.cursor.Cursor;
import io.vitess.client.cursor.CursorWithError;
import io.vitess.proto.Query;
import io.vitess.proto.Query.SplitQueryRequest.Algorithm;
import io.vitess.proto.Topodata.KeyRange;
import io.vitess.proto.Topodata.SrvKeyspace;
import io.vitess.proto.Topodata.TabletType;
import io.vitess.proto.Vtgate.BoundKeyspaceIdQuery;
import io.vitess.proto.Vtgate.BoundShardQuery;
import io.vitess.proto.Vtgate.SplitQueryResponse;

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

  public Cursor executeShards(Context ctx, String query, String keyspace, Iterable<String> shards,
      Map<String, ?> bindVars, TabletType tabletType,
      Query.ExecuteOptions.IncludedFields includedFields) throws SQLException {
    return conn.executeShards(ctx, query, keyspace, shards, bindVars, tabletType, includedFields)
        .checkedGet();
  }

  public Cursor executeKeyspaceIds(Context ctx, String query, String keyspace,
      Iterable<byte[]> keyspaceIds, Map<String, ?> bindVars, TabletType tabletType,
      Query.ExecuteOptions.IncludedFields includedFields)
      throws SQLException {
    return conn
        .executeKeyspaceIds(ctx, query, keyspace, keyspaceIds, bindVars, tabletType, includedFields)
        .checkedGet();
  }

  public Cursor executeKeyRanges(Context ctx, String query, String keyspace,
      Iterable<? extends KeyRange> keyRanges, Map<String, ?> bindVars, TabletType tabletType,
      Query.ExecuteOptions.IncludedFields includedFields)
      throws SQLException {
    return conn
        .executeKeyRanges(ctx, query, keyspace, keyRanges, bindVars, tabletType, includedFields)
        .checkedGet();
  }

  public Cursor executeEntityIds(Context ctx, String query, String keyspace,
      String entityColumnName, Map<byte[], ?> entityKeyspaceIds, Map<String, ?> bindVars,
      TabletType tabletType, Query.ExecuteOptions.IncludedFields includedFields)
      throws SQLException {
    return conn.executeEntityIds(ctx, query, keyspace, entityColumnName, entityKeyspaceIds,
        bindVars, tabletType, includedFields).checkedGet();
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

  /**
   * Execute multiple keyspace ID queries as a batch.
   *
   * @param asTransaction If true, automatically create a transaction (per shard) that encloses
   *     all the batch queries.
   */
  public List<Cursor> executeBatchShards(Context ctx, Iterable<? extends BoundShardQuery> queries,
      TabletType tabletType, boolean asTransaction,
      Query.ExecuteOptions.IncludedFields includedFields)
      throws SQLException {
    return conn.executeBatchShards(ctx, queries, tabletType, asTransaction, includedFields)
        .checkedGet();
  }

  /**
   * Execute multiple keyspace ID queries as a batch.
   *
   * @param asTransaction If true, automatically create a transaction (per shard) that encloses
   *     all the batch queries.
   */
  public List<Cursor> executeBatchKeyspaceIds(Context ctx,
      Iterable<? extends BoundKeyspaceIdQuery> queries, TabletType tabletType,
      boolean asTransaction, Query.ExecuteOptions.IncludedFields includedFields)
      throws SQLException {
    return conn.executeBatchKeyspaceIds(ctx, queries, tabletType, asTransaction, includedFields)
        .checkedGet();
  }

  public Cursor streamExecute(Context ctx, String query, Map<String, ?> bindVars,
      TabletType tabletType, Query.ExecuteOptions.IncludedFields includedFields)
      throws SQLException {
    return conn.streamExecute(ctx, query, bindVars, tabletType, includedFields);
  }

  public Cursor streamExecuteShards(Context ctx, String query, String keyspace,
      Iterable<String> shards, Map<String, ?> bindVars, TabletType tabletType,
      Query.ExecuteOptions.IncludedFields includedFields)
      throws SQLException {
    return conn
        .streamExecuteShards(ctx, query, keyspace, shards, bindVars, tabletType, includedFields);
  }

  public Cursor streamExecuteKeyspaceIds(Context ctx, String query, String keyspace,
      Iterable<byte[]> keyspaceIds, Map<String, ?> bindVars, TabletType tabletType,
      Query.ExecuteOptions.IncludedFields includedFields)
      throws SQLException {
    return conn.streamExecuteKeyspaceIds(ctx, query, keyspace, keyspaceIds, bindVars, tabletType,
        includedFields);
  }

  public Cursor streamExecuteKeyRanges(Context ctx, String query, String keyspace,
      Iterable<? extends KeyRange> keyRanges, Map<String, ?> bindVars, TabletType tabletType,
      Query.ExecuteOptions.IncludedFields includedFields)
      throws SQLException {
    return conn.streamExecuteKeyRanges(ctx, query, keyspace, keyRanges, bindVars, tabletType,
        includedFields);
  }

  public VTGateBlockingTx begin(Context ctx) throws SQLException {
    return begin(ctx, false);
  }

  public VTGateBlockingTx begin(Context ctx, boolean singleDB) throws SQLException {
    return new VTGateBlockingTx(conn.begin(ctx, singleDB).checkedGet());
  }

  public List<SplitQueryResponse.Part> splitQuery(
      Context ctx,
      String keyspace,
      String query,
      Map<String, ?> bindVars,
      Iterable<String> splitColumns,
      int splitCount,
      int numRowsPerQueryPart,
      Algorithm algorithm) throws SQLException {
    return conn.splitQuery(
        ctx, keyspace, query, bindVars, splitColumns, splitCount, numRowsPerQueryPart, algorithm)
        .checkedGet();
  }

  public SrvKeyspace getSrvKeyspace(Context ctx, String keyspace) throws SQLException {
    return conn.getSrvKeyspace(ctx, keyspace).checkedGet();
  }

  @Override
  public void close() throws IOException {
    conn.close();
  }
}
