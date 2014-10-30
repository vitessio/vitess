package com.github.youtube.vitess.jdbc.vtocc;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

import com.github.youtube.vitess.jdbc.vtocc.AcolyteRowList.Factory;
import com.github.youtube.vitess.jdbc.vtocc.QueryService.Query;
import com.github.youtube.vitess.jdbc.vtocc.QueryService.SqlQuery;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.regex.Pattern;

import javax.inject.Inject;

import acolyte.jdbc.StatementHandler;
import acolyte.jdbc.UpdateResult;
import acolyte.jdbc.QueryResult;

/**
 * Implementation of {@link StatementHandler} that glues Acolyte to {@link SqlQuery} calls.
 *
 * Instances are only called from within {@link acolyte.jdbc.Driver}, contract is not well defined
 * therefore there are no unit tests. This code is tested as a part of integration tests running SQL
 * queries through JDBC.
 */
public class VtoccStatementHandler implements StatementHandler {

  private static final Pattern selectQueryPattern =
      Pattern.compile("^\\s*SELECT.*", Pattern.CASE_INSENSITIVE);
  private static final Pattern updateWhenNonSelectQueryPattern = Pattern.compile("^\\s*\\w.*");
  private final Logger LOGGER = LoggerFactory.getLogger(VtoccStatementHandler.class);
  private final VtoccQueryFactory vtoccQueryFactory;
  private final Factory acolyteRowListFactory;
  private final SqlQuery.BlockingInterface sqlQueryBlockingInterface;


  private final com.google.inject.Provider<RpcController> rpcControllerProvider;


  @Inject
  @VisibleForTesting
  VtoccStatementHandler(VtoccQueryFactory vtoccQueryFactory,
      Factory acolyteRowListFactory, SqlQuery.BlockingInterface sqlQueryBlockingInterface,
      com.google.inject.Provider<RpcController> rpcControllerProvider) {
    this.vtoccQueryFactory = vtoccQueryFactory;
    this.acolyteRowListFactory = acolyteRowListFactory;
    this.sqlQueryBlockingInterface = sqlQueryBlockingInterface;
    this.rpcControllerProvider = rpcControllerProvider;
  }

  /**
   * Called by Acolyte when it's time to do a query.
   */
  @Override
  public QueryResult whenSQLQuery(String sql, List<Parameter> parameters) throws SQLException {
    try {
      Query query = vtoccQueryFactory.create(sql, parameters);
      // TODO(timofeyb): provide rpc controller
      QueryService.QueryResult response = sqlQueryBlockingInterface
          .execute(rpcControllerProvider.get(), query);
      return acolyteRowListFactory.create(response).asResult();
    } catch (ServiceException e) {
      LOGGER.debug("SqlQuery failed: ", e);
      throw VtoccSqlExceptionFactory.getSqlException(e);
    }
  }

  /**
   * Called by Acolyte when it's time to do a DML query.
   */
  @Override
  public UpdateResult whenSQLUpdate(String sql, List<Parameter> parameters) throws SQLException {
    try {
      Query query = vtoccQueryFactory.create(sql, parameters);

      // TODO(timofeyb): provide rpc controller
      QueryService.QueryResult response = sqlQueryBlockingInterface
          .execute(rpcControllerProvider.get(), query);
      return new UpdateResult((int) response.getRowsAffected());
    } catch (ServiceException e) {
      LOGGER.debug("SqlUpdate failed: ", e);
      throw VtoccSqlExceptionFactory.getSqlException(e);
    }
  }

  /**
   * Due to internal structure of current implementation of Acolyte we're required to know in
   * advance if any specific query is a query or a DML operation.
   *
   * Return true for queries, false for DMLs and throws {@link IllegalArgumentException} in all
   * cases when we're not completely sure.
   */
  @Override
  public boolean isQuery(String sql) {
    // TODO(timofeyb): don't call from Acolyte this often, throw unsupported in all cases
    if (selectQueryPattern.matcher(sql).matches()) {
      // check if we have a select in the very beginning
      return true;
    }
    if (updateWhenNonSelectQueryPattern.matcher(sql).matches()) {
      // in case query start with a statement
      return false;
    }
    // in all cases when we're not sure what to do
    throw new IllegalArgumentException("SQL should start with SELECT, UPDATE, etc.");
  }

  @Override
  public ResultSet getGeneratedKeys() {
    throw new UnsupportedOperationException("Generated keys are not supported");
  }
}
