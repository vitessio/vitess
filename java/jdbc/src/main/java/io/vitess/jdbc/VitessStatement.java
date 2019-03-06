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

package io.vitess.jdbc;

import io.vitess.client.Context;
import io.vitess.client.Proto;
import io.vitess.client.VTGateConnection;
import io.vitess.client.cursor.Cursor;
import io.vitess.client.cursor.CursorWithError;
import io.vitess.proto.Query;
import io.vitess.proto.Vtrpc;
import io.vitess.util.Constants;
import io.vitess.util.StringUtils;

import java.sql.BatchUpdateException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by harshit.gangal on 19/01/16.
 * <p>
 * This class expects an sql query to be provided when a call to DB is made by the method:
 * execute(sql), executeQuery(sql), executeUpdate(sql). When executeBatch() is called, the sql is
 * not required; instead all the queries to be executed must be added via the addBatch(sql) method.
 * In all the cases, once a method is called, no reference of the query/queries provided is/are
 * kept.
 */
public class VitessStatement implements Statement {

  protected static final String[] ON_DUPLICATE_KEY_UPDATE_CLAUSE = new String[]{"ON", "DUPLICATE",
      "KEY", "UPDATE"};
  protected VitessResultSet vitessResultSet;
  protected VitessConnection vitessConnection;
  protected boolean closed;
  protected long resultCount;
  protected long queryTimeoutInMillis;
  protected int maxFieldSize = Constants.MAX_BUFFER_SIZE;
  protected int maxRows = 0;
  protected int fetchSize = 0;
  protected int resultSetConcurrency;
  protected int resultSetType;
  protected boolean retrieveGeneratedKeys = false;
  protected long generatedId = -1;
  protected long[][] batchGeneratedKeys;
  /**
   * Holds batched commands
   */
  private List<String> batchedArgs;


  public VitessStatement(VitessConnection vitessConnection) {
    this(vitessConnection, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
  }

  public VitessStatement(VitessConnection vitessConnection, int resultSetType,
      int resultSetConcurrency) {
    this.vitessConnection = vitessConnection;
    this.queryTimeoutInMillis = vitessConnection.getTimeout();
    this.vitessResultSet = null;
    this.resultSetType = resultSetType;
    this.resultSetConcurrency = resultSetConcurrency;
    this.closed = false;
    this.resultCount = -1;
    this.vitessConnection.registerStatement(this);
    this.batchedArgs = new ArrayList<>();
    this.batchGeneratedKeys = null;
  }

  /**
   * To execute an Select/Show Statement
   *
   * @param sql - SQL Query
   * @return ResultSet
   */
  public ResultSet executeQuery(String sql) throws SQLException {
    checkOpen();
    checkSQLNullOrEmpty(sql);
    closeOpenResultSetAndResetCount();

    if (this instanceof VitessPreparedStatement) { // PreparedStatement cannot call this method
      throw new SQLException(Constants.SQLExceptionMessages.METHOD_NOT_ALLOWED);
    }

    //Setting to default value
    this.retrieveGeneratedKeys = false;
    this.generatedId = -1;

    VTGateConnection vtGateConn = this.vitessConnection.getVtGateConn();

    Cursor cursor;
    if ((vitessConnection.isSimpleExecute() && this.fetchSize == 0) || vitessConnection
        .isInTransaction()) {
      checkAndBeginTransaction();
      Context context = this.vitessConnection.createContext(this.queryTimeoutInMillis);
      cursor = vtGateConn.execute(context, sql, null, vitessConnection.getVtSession()).checkedGet();
    } else {
      /* Stream query is not suppose to run in a txn. */
      Context context = this.vitessConnection.createContext(this.queryTimeoutInMillis);
      cursor = vtGateConn.streamExecute(context, sql, null, vitessConnection.getVtSession());
    }

    if (null == cursor) {
      throw new SQLException(Constants.SQLExceptionMessages.METHOD_CALL_FAILED);
    }
    this.vitessResultSet = new VitessResultSet(cursor, this);
    return this.vitessResultSet;
  }

  /**
   * To execute a DML statement
   *
   * @param sql - SQL Query
   * @return Rows Affected Count
   */
  public int executeUpdate(String sql) throws SQLException {
    return executeUpdate(sql, Statement.NO_GENERATED_KEYS);
  }

  /**
   * To Execute Unknown Statement
   */
  public boolean execute(String sql) throws SQLException {
    return execute(sql, Statement.NO_GENERATED_KEYS);
  }

  /**
   * To get the resultSet generated
   *
   * @return ResultSet
   */
  public ResultSet getResultSet() throws SQLException {
    checkOpen();
    return this.vitessResultSet;
  }

  public int getUpdateCount() throws SQLException {
    int truncatedUpdateCount;

    checkOpen();

    if (null != this.vitessResultSet) {
      return -1;
    }

    if (this.resultCount > Integer.MAX_VALUE) {
      truncatedUpdateCount = Integer.MAX_VALUE;
    } else {
      truncatedUpdateCount = (int) this.resultCount;
    }
    return truncatedUpdateCount;
  }

  public void close() throws SQLException {
    SQLException postponedSQLException = null;

    if (!this.closed) {
      if (null != this.vitessConnection) {
        this.vitessConnection.unregisterStatement(this);
      }
      try {
        if (null != this.vitessResultSet) {
          this.vitessResultSet.close();
        }
      } catch (SQLException ex) {
        postponedSQLException = ex;
      }

      this.vitessConnection = null;
      this.vitessResultSet = null;
      this.closed = true;

      if (null != postponedSQLException) {
        throw postponedSQLException;
      }
    }
  }

  public int getMaxFieldSize() throws SQLException {
    checkOpen();
    return this.maxFieldSize;
  }

  public void setMaxFieldSize(int max) throws SQLException {
    /* Currently not used
    checkOpen();
    if (max < 0 || max > Constants.MAX_BUFFER_SIZE) {
        throw new SQLException(
            Constants.SQLExceptionMessages.ILLEGAL_VALUE_FOR + "max field size");
    }
    this.maxFieldSize = max;
    */
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public int getMaxRows() throws SQLException {
    checkOpen();
    return this.maxRows;
  }

  public void setMaxRows(int max) throws SQLException {
    checkOpen();
    if (max < 0) {
      throw new SQLException(Constants.SQLExceptionMessages.ILLEGAL_VALUE_FOR + "max row");
    }
    this.maxRows = max;
  }

  public int getQueryTimeout() throws SQLException {
    checkOpen();
    return (int) (this.queryTimeoutInMillis / 1000);
  }

  public void setQueryTimeout(int seconds) throws SQLException {
    checkOpen();
    if (seconds < 0) {
      throw new SQLException(Constants.SQLExceptionMessages.ILLEGAL_VALUE_FOR + "query timeout");
    }
    this.queryTimeoutInMillis =
        (0 == seconds) ? vitessConnection.getTimeout() : (long) seconds * 1000;
  }

  /**
   * Return Warnings
   * <p/>
   * Not implementing as Error is Thrown when occurred
   *
   * @return SQLWarning or null
   */
  public SQLWarning getWarnings() throws SQLException {
    checkOpen();
    return null;
  }

  /**
   * Clear the warnings - Not saving Warnings
   */
  public void clearWarnings() {
    //no-op
  }

  public void setCursorName(String name) throws SQLException {
    checkOpen();
  }

  public int getFetchDirection() throws SQLException {
    checkOpen();
    return ResultSet.FETCH_FORWARD;
  }

  public void setFetchDirection(int direction) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public int getFetchSize() throws SQLException {
    checkOpen();
    return this.fetchSize;
  }

  public void setFetchSize(int rows) throws SQLException {
    checkOpen();
    if (rows < 0) {
      throw new SQLException(Constants.SQLExceptionMessages.ILLEGAL_VALUE_FOR + "fetch size");
    }
    this.fetchSize = rows;
  }

  public int getResultSetConcurrency() throws SQLException {
    checkOpen();
    return this.resultSetConcurrency;
  }

  public int getResultSetType() throws SQLException {
    checkOpen();
    return this.resultSetType;
  }

  public VitessConnection getConnection() throws SQLException {
    checkOpen();
    return vitessConnection;
  }

  public boolean isClosed() {
    return this.closed;
  }

  /**
   * Unwrap a class
   *
   * @param iface - A Class defining an interface that the result must implement.
   * @param <T> - the type of the class modeled by this Class object
   * @return an object that implements the interface. May be a proxy for the actual implementing
   *     object.
   */
  public <T> T unwrap(Class<T> iface) throws SQLException {
    try {
      return iface.cast(this);
    } catch (ClassCastException cce) {
      throw new SQLException(Constants.SQLExceptionMessages.CLASS_CAST_EXCEPTION + iface.toString(),
          cce);
    }
  }

  /**
   * Checking Wrapper
   *
   * @param iface - A Class defining an interface that the result must implement.
   * @return true if this implements the interface or directly or indirectly wraps an object that
   *     does.
   */
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    checkOpen();
    return iface.isInstance(this);
  }

  /**
   *
   */
  public ResultSet getGeneratedKeys() throws SQLException {
    checkOpen();
    if (!this.retrieveGeneratedKeys) {
      throw new SQLException(Constants.SQLExceptionMessages.GENERATED_KEYS_NOT_REQUESTED);
    }

    String[] columnNames = new String[1];
    columnNames[0] = "GENERATED_KEY";
    Query.Type[] columnTypes = new Query.Type[1];
    columnTypes[0] = Query.Type.UINT64;
    String[][] data = null;

    if (this.generatedId > 0) {
      // This is as per Mysql JDBC Driver.
      // As the actual count of generated value is not known,
      // only the rows affected is known, so using firstInsertId all the auto_inc values
      // are generated. As per Vitess Config, the increment value is 1 and not changeable.
      data = new String[(int) this.resultCount][1];
      for (int i = 0; i < this.resultCount; ++i) {
        data[i][0] = String.valueOf(this.generatedId + i);
      }
    } else if (this.batchGeneratedKeys != null) {
      long totalAffected = 0;
      for (long[] batchGeneratedKey : this.batchGeneratedKeys) {
        long rowsAffected = batchGeneratedKey[1];
        totalAffected += rowsAffected;
      }
      data = new String[(int) totalAffected][1];
      int idx = 0;
      for (long[] batchGeneratedKey : this.batchGeneratedKeys) {
        long insertId = batchGeneratedKey[0];
        long rowsAffected = batchGeneratedKey[1];
        for (int j = 0; j < rowsAffected; j++) {
          data[idx++][0] = String.valueOf(insertId + j);
        }
      }
    }
    return new VitessResultSet(columnNames, columnTypes, data, this.vitessConnection);
  }

  /**
   * To execute DML statement
   *
   * @param sql - SQL Query
   * @param autoGeneratedKeys - Flag for generated Keys
   * @return Row Affected Count
   */
  public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
    checkOpen();
    checkNotReadOnly();
    checkSQLNullOrEmpty(sql);
    closeOpenResultSetAndResetCount();

    if (this instanceof VitessPreparedStatement) { // PreparedStatement cannot call this method
      throw new SQLException(Constants.SQLExceptionMessages.METHOD_NOT_ALLOWED);
    }

    VTGateConnection vtGateConn = this.vitessConnection.getVtGateConn();

    checkAndBeginTransaction();
    Context context = this.vitessConnection.createContext(this.queryTimeoutInMillis);
    Cursor cursor = vtGateConn.execute(context, sql, null, vitessConnection.getVtSession())
        .checkedGet();

    if (null == cursor) {
      throw new SQLException(Constants.SQLExceptionMessages.METHOD_CALL_FAILED);
    }

    if (!(null == cursor.getFields() || cursor.getFields().isEmpty())) {
      throw new SQLException(Constants.SQLExceptionMessages.SQL_RETURNED_RESULT_SET);
    }

    if (autoGeneratedKeys == Statement.RETURN_GENERATED_KEYS) {
      this.retrieveGeneratedKeys = true;
      this.generatedId = cursor.getInsertId();
    } else {
      this.retrieveGeneratedKeys = false;
      this.generatedId = -1;
    }

    this.resultCount = cursor.getRowsAffected();

    int truncatedUpdateCount;
    if (this.resultCount > Integer.MAX_VALUE) {
      truncatedUpdateCount = Integer.MAX_VALUE;
    } else {
      truncatedUpdateCount = (int) this.resultCount;
    }

    return truncatedUpdateCount;
  }

  /**
   * To execute Unknown Statement
   *
   * @param sql - SQL Query
   * @param autoGeneratedKeys - Flag for generated Keys
   * @return - <code>true</code> if the first result is a <code>ResultSet</code> object;
   *     <code>false</code> if it is an update count or there are no results
   */
  public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
    checkOpen();
    checkSQLNullOrEmpty(sql);
    closeOpenResultSetAndResetCount();

    if (this instanceof VitessPreparedStatement) { // PreparedStatement cannot call this method
      throw new SQLException(Constants.SQLExceptionMessages.METHOD_NOT_ALLOWED);
    }
    if (!maybeSelect(sql)) {
      this.executeUpdate(sql, autoGeneratedKeys);
      return false;
    } else {
      this.executeQuery(sql);
      return true;
    }
  }

  /**
   * Add the query in the batch.
   */
  public void addBatch(String sql) throws SQLException {
    checkOpen();
    checkSQLNullOrEmpty(sql);
    if (this instanceof VitessPreparedStatement) { // PreparedStatement cannot call this method
      throw new SQLException(Constants.SQLExceptionMessages.METHOD_NOT_ALLOWED);
    }
    this.batchedArgs.add(sql);
  }

  /**
   * Clear all the queries batched.
   */
  public void clearBatch() throws SQLException {
    checkOpen();
    this.batchedArgs.clear();
  }

  /**
   * Submits a batch of commands to the database for execution and if all commands execute
   * successfully, returns an array of update counts. The array returned is according to the order
   * in which they were added to the batch.
   * <p>
   * If one of the commands in a batch update fails to execute properly, this method throws a
   * <code>BatchUpdateException</code>, and a JDBC driver may or may not continue to process the
   * remaining commands in the batch. If the driver continues processing after a failure, the array
   * returned by the method <code>BatchUpdateException.getUpdateCounts</code> will contain as many
   * elements as there are commands in the batch.
   *
   * @return int[] of results corresponding to each command
   */
  public int[] executeBatch() throws SQLException {
    checkOpen();
    checkNotReadOnly();
    VTGateConnection vtGateConn;
    List<CursorWithError> cursorWithErrorList;

    if (0 == batchedArgs.size()) {
      return new int[0];
    }

    try {
      vtGateConn = this.vitessConnection.getVtGateConn();

      checkAndBeginTransaction();
      Context context = this.vitessConnection.createContext(this.queryTimeoutInMillis);
      cursorWithErrorList = vtGateConn
          .executeBatch(context, batchedArgs, null, vitessConnection.getVtSession()).checkedGet();

      if (null == cursorWithErrorList) {
        throw new SQLException(Constants.SQLExceptionMessages.METHOD_CALL_FAILED);
      }

      this.retrieveGeneratedKeys = true;// mimicking mysql-connector-j
      return this.generateBatchUpdateResult(cursorWithErrorList, batchedArgs);
    } finally {
      this.clearBatch();
    }
  }

  // Internal Methods Created


  protected void closeOpenResultSetAndResetCount() throws SQLException {
    try {
      if (null != this.vitessResultSet) {
        this.vitessResultSet.close();
      }
    } catch (SQLException ex) {
      throw new SQLException(ex);
    } finally {
      this.vitessResultSet = null;
      this.resultCount = -1;
    }
  }

  protected void checkOpen() throws SQLException {
    if (closed) {
      throw new SQLException(Constants.SQLExceptionMessages.STMT_CLOSED);
    }
  }

  protected void checkNotReadOnly() throws SQLException {
    if (vitessConnection.isReadOnly()) {
      throw new SQLException(Constants.SQLExceptionMessages.READ_ONLY);
    }
  }

  protected void checkSQLNullOrEmpty(String sql) throws SQLException {
    if (StringUtils.isNullOrEmptyWithoutWS(sql)) {
      throw new SQLException(Constants.SQLExceptionMessages.SQL_EMPTY);
    }
  }

  /**
   * This method returns the updateCounts array containing the status for each query sent in the
   * batch. If any of the query does return success. It throws a BatchUpdateException.
   *
   * @param cursorWithErrorList Consists list of Cursor and Error object.
   * @param batchedArgs holds batched commands
   * @return int[] of results corresponding to each query
   */
  protected int[] generateBatchUpdateResult(List<CursorWithError> cursorWithErrorList,
      List<String> batchedArgs) throws BatchUpdateException {
    int[] updateCounts = new int[cursorWithErrorList.size()];
    ArrayList<long[]> generatedKeys = new ArrayList<>();

    Vtrpc.RPCError rpcError = null;
    String batchCommand = null;
    CursorWithError cursorWithError = null;
    for (int i = 0; i < cursorWithErrorList.size(); i++) {
      cursorWithError = cursorWithErrorList.get(i);
      batchCommand = batchedArgs.get(i);
      if (null == cursorWithError.getError()) {
        try {
          long rowsAffected = cursorWithError.getCursor().getRowsAffected();
          int truncatedUpdateCount;
          boolean queryBatchUpsert = false;
          if (rowsAffected > Integer.MAX_VALUE) {
            truncatedUpdateCount = Integer.MAX_VALUE;
          } else {
            if (sqlIsUpsert(batchCommand)) {
              // mimicking mysql-connector-j here.
              // but it would fail for: insert into t1 values ('a'), ('b') on duplicate key
              // update ts = now();
              truncatedUpdateCount = 1;
              queryBatchUpsert = true;
            } else {
              truncatedUpdateCount = (int) rowsAffected;
            }
          }
          updateCounts[i] = truncatedUpdateCount;
          long insertId = cursorWithError.getCursor().getInsertId();
          if (this.retrieveGeneratedKeys && (!queryBatchUpsert || insertId > 0)) {
            generatedKeys.add(new long[]{insertId, truncatedUpdateCount});
          }
        } catch (SQLException ex) {
          /* This case should not happen as API has returned cursor and not error.
           * Handling by Statement.SUCCESS_NO_INFO
           */
          updateCounts[i] = Statement.SUCCESS_NO_INFO;
          if (this.retrieveGeneratedKeys) {
            generatedKeys.add(new long[]{Statement.SUCCESS_NO_INFO, Statement.SUCCESS_NO_INFO});
          }
        }
      } else {
        rpcError = cursorWithError.getError();
        updateCounts[i] = Statement.EXECUTE_FAILED;
        if (this.retrieveGeneratedKeys) {
          generatedKeys.add(new long[]{Statement.EXECUTE_FAILED, Statement.EXECUTE_FAILED});
        }
      }
    }

    if (null != rpcError) {
      int errno = Proto.getErrno(rpcError.getMessage());
      String sqlState = Proto.getSQLState(rpcError.getMessage());
      throw new BatchUpdateException(rpcError.toString(), sqlState, errno, updateCounts);
    }
    if (this.retrieveGeneratedKeys) {
      this.batchGeneratedKeys = generatedKeys.toArray(new long[generatedKeys.size()][2]);
    }
    return updateCounts;
  }

  private boolean sqlIsUpsert(String sql) {
    return StringUtils.indexOfIgnoreCase(0, sql, ON_DUPLICATE_KEY_UPDATE_CLAUSE, "\"'`", "\"'`",
        StringUtils.SEARCH_MODE__ALL) != -1;
  }

  protected boolean maybeSelect(String sql) {
    char firstNonWsCharOfQuery = StringUtils
        .firstAlphaCharUc(sql, StringUtils.findStartOfStatement(sql));
    return firstNonWsCharOfQuery == 'S';
  }

  protected void checkAndBeginTransaction() throws SQLException {
    if (!(this.vitessConnection.getAutoCommit() || this.vitessConnection.isInTransaction())) {
      Context context = this.vitessConnection.createContext(this.queryTimeoutInMillis);
      VTGateConnection vtGateConn = this.vitessConnection.getVtGateConn();
      vtGateConn.execute(context, "begin", null, this.vitessConnection.getVtSession()).checkedGet();
    }
  }

  //Unsupported Methods

  /**
   * Not Required to be implemented as More Results are handled in next() call
   */
  public boolean getMoreResults() throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  /**
   * Not Required to be implemented as More Results are handled in next() call
   */
  public boolean getMoreResults(int current) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void setEscapeProcessing(boolean enable) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void cancel() throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public int executeUpdate(String sql, String[] columnNames) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public boolean execute(String sql, int[] columnIndexes) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public boolean execute(String sql, String[] columnNames) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public int getResultSetHoldability() throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public boolean isPoolable() throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void setPoolable(boolean poolable) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void closeOnCompletion() throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public boolean isCloseOnCompletion() throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

}
