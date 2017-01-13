package com.youtube.vitess.client;

import com.youtube.vitess.client.cursor.Cursor;
import com.youtube.vitess.client.cursor.CursorWithError;
import com.youtube.vitess.proto.Query;
import com.youtube.vitess.proto.Topodata.KeyRange;
import com.youtube.vitess.proto.Topodata.TabletType;
import com.youtube.vitess.proto.Vtgate.BoundKeyspaceIdQuery;
import com.youtube.vitess.proto.Vtgate.BoundShardQuery;

import javax.annotation.Nullable;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * A synchronous wrapper around a VTGate transaction.
 *
 * <p>This is a wrapper around the asynchronous {@link VTGateTx} class
 * that converts all methods to synchronous.
 */
public class VTGateBlockingTx {
  private final VTGateTx tx;

  /**
   * Wraps an existing {@link VTGateTx} in a synchronous API.
   */
  public VTGateBlockingTx(VTGateTx tx) {
    this.tx = tx;
  }

  public Cursor execute(Context ctx, String query, Map<String, ?> bindVars, TabletType tabletType,
      Query.ExecuteOptions.IncludedFields includedFields)
      throws SQLException {
    return tx.execute(ctx, query, bindVars, tabletType, includedFields).checkedGet();
  }

  public Cursor executeShards(
      Context ctx,
      String query,
      String keyspace,
      Iterable<String> shards,
      Map<String, ?> bindVars,
      TabletType tabletType,
      Query.ExecuteOptions.IncludedFields includedFields)
      throws SQLException {
    return tx.executeShards(ctx, query, keyspace, shards, bindVars, tabletType, includedFields).checkedGet();
  }

  public Cursor executeKeyspaceIds(
      Context ctx,
      String query,
      String keyspace,
      Iterable<byte[]> keyspaceIds,
      Map<String, ?> bindVars,
      TabletType tabletType,
      Query.ExecuteOptions.IncludedFields includedFields)
      throws SQLException {
    return tx.executeKeyspaceIds(ctx, query, keyspace, keyspaceIds, bindVars, tabletType, includedFields)
        .checkedGet();
  }

  public Cursor executeKeyRanges(
      Context ctx,
      String query,
      String keyspace,
      Iterable<? extends KeyRange> keyRanges,
      Map<String, ?> bindVars,
      TabletType tabletType,
      Query.ExecuteOptions.IncludedFields includedFields)
      throws SQLException {
    return tx.executeKeyRanges(ctx, query, keyspace, keyRanges, bindVars, tabletType, includedFields).checkedGet();
  }

  public Cursor executeEntityIds(
      Context ctx,
      String query,
      String keyspace,
      String entityColumnName,
      Map<byte[], ?> entityKeyspaceIds,
      Map<String, ?> bindVars,
      TabletType tabletType,
      Query.ExecuteOptions.IncludedFields includedFields)
      throws SQLException {
    return tx.executeEntityIds(
            ctx, query, keyspace, entityColumnName, entityKeyspaceIds, bindVars, tabletType, includedFields)
        .checkedGet();
  }

  public List<CursorWithError> executeBatch(Context ctx, List<String> queryList,
      @Nullable List<Map<String, ?>> bindVarsList, TabletType tabletType,
      Query.ExecuteOptions.IncludedFields includedFields)
      throws SQLException {
    return tx.executeBatch(ctx, queryList, bindVarsList, tabletType, includedFields).checkedGet();
  }

  public List<Cursor> executeBatchShards(
      Context ctx, Iterable<? extends BoundShardQuery> queries, TabletType tabletType,
      Query.ExecuteOptions.IncludedFields includedFields)
      throws SQLException {
    return tx.executeBatchShards(ctx, queries, tabletType, includedFields).checkedGet();
  }

  public List<Cursor> executeBatchKeyspaceIds(
      Context ctx, Iterable<? extends BoundKeyspaceIdQuery> queries, TabletType tabletType,
      Query.ExecuteOptions.IncludedFields includedFields)
      throws SQLException {
    return tx.executeBatchKeyspaceIds(ctx, queries, tabletType, includedFields).checkedGet();
  }

  public void commit(Context ctx) throws SQLException {
    commit(ctx, false);
  }

  public void commit(Context ctx, boolean atomic) throws SQLException {
    tx.commit(ctx, atomic).checkedGet();
  }

  public void rollback(Context ctx) throws SQLException {
    tx.rollback(ctx).checkedGet();
  }
}
