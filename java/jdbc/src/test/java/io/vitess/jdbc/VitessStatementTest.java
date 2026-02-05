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

package io.vitess.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.nullable;
import static org.mockito.Mockito.verify;

import io.vitess.client.Context;
import io.vitess.client.SQLFuture;
import io.vitess.client.VTGateConnection;
import io.vitess.client.VTSession;
import io.vitess.client.cursor.Cursor;
import io.vitess.client.cursor.CursorWithError;
import io.vitess.proto.Query;
import io.vitess.proto.Vtrpc;
import io.vitess.util.Constants;

import java.lang.reflect.Field;
import java.sql.BatchUpdateException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.mockito.Mockito;

/**
 * Created by harshit.gangal on 19/01/16.
 */

public class VitessStatementTest {

  private String sqlSelect = "select 1 from test_table";
  private String sqlShow = "show tables";
  private String sqlUpdate = "update test_table set msg = null";
  private String sqlInsert = "insert into test_table(msg) values ('abc')";
  private String sqlUpsert = "insert into test_table(msg) values ('abc') on duplicate key update "
      + "msg = 'def'";


  @Test
  public void testGetConnection() throws SQLException {
    VitessConnection mockConn = Mockito.mock(VitessConnection.class);

    VitessStatement statement = new VitessStatement(mockConn);
    assertEquals(mockConn, statement.getConnection());
  }

  @Test
  public void testGetResultSet() throws SQLException {
    VitessConnection mockConn = Mockito.mock(VitessConnection.class);
    VitessStatement statement = new VitessStatement(mockConn);
    assertEquals(null, statement.getResultSet());
  }

  @Test
  public void testExecuteQuery() throws SQLException {
    VitessConnection mockConn = Mockito.mock(VitessConnection.class);
    VTGateConnection mockVtGateConn = Mockito.mock(VTGateConnection.class);
    Cursor mockCursor = Mockito.mock(Cursor.class);
    SQLFuture mockSqlFutureCursor = Mockito.mock(SQLFuture.class);

    Mockito.when(mockConn.getVtGateConn()).thenReturn(mockVtGateConn);
    Mockito.when(mockVtGateConn.execute(nullable(Context.class), nullable(String.class), nullable(Map.class), nullable(VTSession.class)))
        .thenReturn(mockSqlFutureCursor);
    Mockito.when(mockConn.isSimpleExecute()).thenReturn(true);
    Mockito.when(mockSqlFutureCursor.checkedGet()).thenReturn(mockCursor);
    Mockito.when(mockCursor.getFields()).thenReturn(Query.QueryResult.getDefaultInstance().getFieldsList());

    VitessStatement statement = new VitessStatement(mockConn);
    //Empty Sql Statement
    try {
      statement.executeQuery("");
      fail("Should have thrown exception for empty sql");
    } catch (SQLException ex) {
      assertEquals("SQL statement is not valid", ex.getMessage());
    }

    ResultSet rs = statement.executeQuery(sqlSelect);
    assertEquals(-1, statement.getUpdateCount());

    //autocommit is false and not in transaction
    Mockito.when(mockConn.getAutoCommit()).thenReturn(false);
    Mockito.when(mockConn.isInTransaction()).thenReturn(false);
    rs = statement.executeQuery(sqlSelect);
    assertEquals(-1, statement.getUpdateCount());

    //when returned cursor is null
    Mockito.when(mockSqlFutureCursor.checkedGet()).thenReturn(null);
    try {
      statement.executeQuery(sqlSelect);
      fail("Should have thrown exception for cursor null");
    } catch (SQLException ex) {
      assertEquals("Failed to execute this method", ex.getMessage());
    }
  }

  @Test
  public void testExecuteQueryWithStreamExecuteType() throws SQLException {
    VitessConnection mockConn = Mockito.mock(VitessConnection.class);
    VTGateConnection mockVtGateConn = Mockito.mock(VTGateConnection.class);
    Cursor mockCursor = Mockito.mock(Cursor.class);
    SQLFuture mockSqlFutureCursor = Mockito.mock(SQLFuture.class);

    Mockito.when(mockConn.getVtGateConn()).thenReturn(mockVtGateConn);
    Mockito.when(mockVtGateConn
        .streamExecute(nullable(Context.class), nullable(String.class), nullable(Map.class), nullable(VTSession.class)))
        .thenReturn(mockCursor);
    Mockito.when(mockConn.getExecuteType()).thenReturn(Constants.QueryExecuteType.STREAM);
    Mockito.when(mockSqlFutureCursor.checkedGet()).thenReturn(mockCursor);
    Mockito.when(mockCursor.getFields()).thenReturn(Query.QueryResult.getDefaultInstance().getFieldsList());

    VitessStatement statement = new VitessStatement(mockConn);
    //Empty Sql Statement
    try {
      statement.executeQuery("");
      fail("Should have thrown exception for empty sql");
    } catch (SQLException ex) {
      assertEquals("SQL statement is not valid", ex.getMessage());
    }

    //select on replica
    ResultSet rs = statement.executeQuery(sqlSelect);
    assertEquals(-1, statement.getUpdateCount());

    //show query
    rs = statement.executeQuery(sqlShow);
    assertEquals(-1, statement.getUpdateCount());

    //select on primary when tx is null and autocommit is false
    Mockito.when(mockConn.getAutoCommit()).thenReturn(false);
    Mockito.when(mockConn.isInTransaction()).thenReturn(false);
    rs = statement.executeQuery(sqlSelect);
    assertEquals(-1, statement.getUpdateCount());

    //when returned cursor is null
    Mockito.when(mockVtGateConn
        .streamExecute(nullable(Context.class), nullable(String.class), nullable(Map.class), nullable(VTSession.class)))
        .thenReturn(null);
    try {
      statement.executeQuery(sqlSelect);
      fail("Should have thrown exception for cursor null");
    } catch (SQLException ex) {
      assertEquals("Failed to execute this method", ex.getMessage());
    }
  }

  @Test
  public void testExecuteFetchSizeAsStreaming() throws SQLException {
    testExecute(5, true, false, true);
    testExecute(5, false, false, true);
    testExecute(0, true, true, false);
    testExecute(0, false, false, true);
  }

  private void testExecute(int fetchSize, boolean simpleExecute, boolean shouldRunExecute,
      boolean shouldRunStreamExecute) throws SQLException {
    VTGateConnection mockVtGateConn = Mockito.mock(VTGateConnection.class);

    VitessConnection mockConn = Mockito.mock(VitessConnection.class);
    Mockito.when(mockConn.isSimpleExecute()).thenReturn(simpleExecute);
    Mockito.when(mockConn.getVtGateConn()).thenReturn(mockVtGateConn);

    Cursor mockCursor = Mockito.mock(Cursor.class);
    SQLFuture mockSqlFutureCursor = Mockito.mock(SQLFuture.class);
    Mockito.when(mockSqlFutureCursor.checkedGet()).thenReturn(mockCursor);

    Mockito.when(mockVtGateConn.execute(nullable(Context.class), nullable(String.class), nullable(Map.class), nullable(VTSession.class)))
        .thenReturn(mockSqlFutureCursor);
    Mockito.when(mockVtGateConn
        .streamExecute(nullable(Context.class), nullable(String.class), nullable(Map.class), nullable(VTSession.class)))
        .thenReturn(mockCursor);

    VitessStatement statement = new VitessStatement(mockConn);
    statement.setFetchSize(fetchSize);
    statement.executeQuery(sqlSelect);

    if (shouldRunExecute) {
      verify(mockVtGateConn, Mockito.times(2))
          .execute(nullable(Context.class), nullable(String.class), nullable(Map.class), nullable(VTSession.class));
    }

    if (shouldRunStreamExecute) {
      verify(mockVtGateConn)
          .streamExecute(nullable(Context.class), nullable(String.class), nullable(Map.class), nullable(VTSession.class));
    }
  }

  @Test
  public void testExecuteUpdate() throws SQLException {
    VitessConnection mockConn = Mockito.mock(VitessConnection.class);
    VTGateConnection mockVtGateConn = Mockito.mock(VTGateConnection.class);
    Cursor mockCursor = Mockito.mock(Cursor.class);
    SQLFuture mockSqlFutureCursor = Mockito.mock(SQLFuture.class);
    List<Query.Field> fieldList = Mockito.mock(ArrayList.class);

    Mockito.when(mockConn.getVtGateConn()).thenReturn(mockVtGateConn);
    Mockito.when(mockVtGateConn.execute(nullable(Context.class), nullable(String.class), nullable(Map.class), nullable(VTSession.class)))
        .thenReturn(mockSqlFutureCursor);
    Mockito.when(mockSqlFutureCursor.checkedGet()).thenReturn(mockCursor);
    Mockito.when(mockCursor.getFields()).thenReturn(Query.QueryResult.getDefaultInstance().getFieldsList());

    VitessStatement statement = new VitessStatement(mockConn);
    //executing dml on primary
    int updateCount = statement.executeUpdate(sqlUpdate);
    assertEquals(0, updateCount);

    //tx is null & autoCommit is true
    Mockito.when(mockConn.getAutoCommit()).thenReturn(true);
    updateCount = statement.executeUpdate(sqlUpdate);
    assertEquals(0, updateCount);

    //cursor fields is not null
    Mockito.when(mockCursor.getFields()).thenReturn(fieldList);
    Mockito.when(fieldList.isEmpty()).thenReturn(false);
    try {
      statement.executeUpdate(sqlSelect);
      fail("Should have thrown exception for field not null");
    } catch (SQLException ex) {
      assertEquals("ResultSet generation is not allowed through this method", ex.getMessage());
    }

    //cursor is null
    Mockito.when(mockSqlFutureCursor.checkedGet()).thenReturn(null);
    try {
      statement.executeUpdate(sqlUpdate);
      fail("Should have thrown exception for cursor null");
    } catch (SQLException ex) {
      assertEquals("Failed to execute this method", ex.getMessage());
    }

    //read only
    Mockito.when(mockConn.isReadOnly()).thenReturn(true);
    try {
      statement.execute("UPDATE SET foo = 1 ON mytable WHERE id = 1");
      fail("Should have thrown exception for read only");
    } catch (SQLException ex) {
      assertEquals(Constants.SQLExceptionMessages.READ_ONLY, ex.getMessage());
    }

    //read only
    Mockito.when(mockConn.isReadOnly()).thenReturn(true);
    try {
      statement.executeBatch();
      fail("Should have thrown exception for read only");
    } catch (SQLException ex) {
      assertEquals(Constants.SQLExceptionMessages.READ_ONLY, ex.getMessage());
    }
  }

  @Test
  public void testExecute() throws SQLException {
    VitessConnection mockConn = Mockito.mock(VitessConnection.class);
    VTGateConnection mockVtGateConn = Mockito.mock(VTGateConnection.class);
    Cursor mockCursor = Mockito.mock(Cursor.class);
    SQLFuture mockSqlFutureCursor = Mockito.mock(SQLFuture.class);
    List<Query.Field> mockFieldList = Mockito.spy(new ArrayList<>());

    Mockito.when(mockConn.getVtGateConn()).thenReturn(mockVtGateConn);
    Mockito.when(mockVtGateConn.execute(nullable(Context.class), nullable(String.class), nullable(Map.class), nullable(VTSession.class)))
        .thenReturn(mockSqlFutureCursor);
    Mockito.when(mockConn.getAutoCommit()).thenReturn(true);
    Mockito.when(mockConn.getExecuteType()).thenReturn(Constants.QueryExecuteType.SIMPLE);
    Mockito.when(mockConn.isSimpleExecute()).thenReturn(true);

    Mockito.when(mockSqlFutureCursor.checkedGet()).thenReturn(mockCursor);
    Mockito.when(mockCursor.getFields()).thenReturn(mockFieldList);

    VitessStatement statement = new VitessStatement(mockConn);
    int fieldSize = 5;
    Mockito.when(mockCursor.getFields()).thenReturn(mockFieldList);
    Mockito.doReturn(fieldSize).when(mockFieldList).size();
    Mockito.doReturn(false).when(mockFieldList).isEmpty();

    boolean hasResultSet = statement.execute(sqlSelect);
    assertTrue(hasResultSet);
    assertNotNull(statement.getResultSet());

    hasResultSet = statement.execute(sqlShow);
    assertTrue(hasResultSet);
    assertNotNull(statement.getResultSet());

    int mockUpdateCount = 10;
    Mockito.when(mockCursor.getFields()).thenReturn(Query.QueryResult.getDefaultInstance().getFieldsList());
    Mockito.when(mockCursor.getRowsAffected()).thenReturn((long) mockUpdateCount);
    hasResultSet = statement.execute(sqlUpdate);
    assertFalse(hasResultSet);
    assertNull(statement.getResultSet());
    assertEquals(mockUpdateCount, statement.getUpdateCount());

    //cursor is null
    Mockito.when(mockSqlFutureCursor.checkedGet()).thenReturn(null);
    try {
      statement.execute(sqlUpdate);
      fail("Should have thrown exception for cursor null");
    } catch (SQLException ex) {
      assertEquals("Failed to execute this method", ex.getMessage());
    }
  }

  @Test
  public void testGetUpdateCount() throws SQLException {
    VitessConnection mockConn = Mockito.mock(VitessConnection.class);
    VTGateConnection mockVtGateConn = Mockito.mock(VTGateConnection.class);
    Cursor mockCursor = Mockito.mock(Cursor.class);
    SQLFuture mockSqlFuture = Mockito.mock(SQLFuture.class);

    Mockito.when(mockConn.getVtGateConn()).thenReturn(mockVtGateConn);
    Mockito.when(mockVtGateConn.execute(nullable(Context.class), nullable(String.class), nullable(Map.class), nullable(VTSession.class)))
        .thenReturn(mockSqlFuture);
    Mockito.when(mockSqlFuture.checkedGet()).thenReturn(mockCursor);
    Mockito.when(mockCursor.getFields()).thenReturn(Query.QueryResult.getDefaultInstance().getFieldsList());

    VitessStatement statement = new VitessStatement(mockConn);
    Mockito.when(mockCursor.getRowsAffected()).thenReturn(10L);
    int updateCount = statement.executeUpdate(sqlUpdate);
    assertEquals(10L, updateCount);
    assertEquals(10L, statement.getUpdateCount());

    // Truncated Update Count
    Mockito.when(mockCursor.getRowsAffected()).thenReturn((long) Integer.MAX_VALUE + 10);
    updateCount = statement.executeUpdate(sqlUpdate);
    assertEquals(Integer.MAX_VALUE, updateCount);
    assertEquals(Integer.MAX_VALUE, statement.getUpdateCount());

    Mockito.when(mockConn.isSimpleExecute()).thenReturn(true);
    statement.executeQuery(sqlSelect);
    assertEquals(-1, statement.getUpdateCount());
  }

  @Test
  public void testClose() throws Exception {
    VitessConnection mockConn = Mockito.mock(VitessConnection.class);
    VTGateConnection mockVtGateConn = Mockito.mock(VTGateConnection.class);
    Cursor mockCursor = Mockito.mock(Cursor.class);
    SQLFuture mockSqlFutureCursor = Mockito.mock(SQLFuture.class);

    Mockito.when(mockConn.getVtGateConn()).thenReturn(mockVtGateConn);
    Mockito.when(mockVtGateConn.execute(nullable(Context.class), nullable(String.class), nullable(Map.class), nullable(VTSession.class)))
        .thenReturn(mockSqlFutureCursor);
    Mockito.when(mockConn.getExecuteType()).thenReturn(Constants.QueryExecuteType.SIMPLE);
    Mockito.when(mockConn.isSimpleExecute()).thenReturn(true);
    Mockito.when(mockSqlFutureCursor.checkedGet()).thenReturn(mockCursor);

    VitessStatement statement = new VitessStatement(mockConn);
    ResultSet rs = statement.executeQuery(sqlSelect);
    statement.close();
    try {
      statement.executeQuery(sqlSelect);
      fail("Should have thrown exception for statement closed");
    } catch (SQLException ex) {
      assertEquals("Statement is closed", ex.getMessage());
    }
  }

  @Test
  public void testGetMaxFieldSize() throws SQLException {
    VitessConnection mockConn = Mockito.mock(VitessConnection.class);

    VitessStatement statement = new VitessStatement(mockConn);
    assertEquals(65535, statement.getMaxFieldSize());
  }

  @Test
  public void testGetMaxRows() throws SQLException {
    VitessConnection mockConn = Mockito.mock(VitessConnection.class);

    VitessStatement statement = new VitessStatement(mockConn);

    statement.setMaxRows(10);
    assertEquals(10, statement.getMaxRows());

    try {
      statement.setMaxRows(-1);
      fail("Should have thrown exception for wrong value");
    } catch (SQLException ex) {
      assertEquals("Illegal value for max row", ex.getMessage());
    }

  }

  @Test
  public void testGetQueryTimeout() throws SQLException {
    VitessConnection mockConn = Mockito.mock(VitessConnection.class);
    Mockito.when(mockConn.getTimeout()).thenReturn((long) Constants.DEFAULT_TIMEOUT);

    VitessStatement statement = new VitessStatement(mockConn);
    assertEquals(30, statement.getQueryTimeout());
  }

  @Test
  public void testGetQueryTimeoutZeroDefault() throws SQLException {
    VitessConnection mockConn = Mockito.mock(VitessConnection.class);
    Mockito.when(mockConn.getTimeout()).thenReturn(0L);

    VitessStatement statement = new VitessStatement(mockConn);
    assertEquals(0, statement.getQueryTimeout());
  }

  @Test
  public void testSetQueryTimeout() throws SQLException {
    VitessConnection mockConn = Mockito.mock(VitessConnection.class);
    Mockito.when(mockConn.getTimeout()).thenReturn((long) Constants.DEFAULT_TIMEOUT);

    VitessStatement statement = new VitessStatement(mockConn);

    int queryTimeout = 10;
    statement.setQueryTimeout(queryTimeout);
    assertEquals(queryTimeout, statement.getQueryTimeout());
    try {
      queryTimeout = -1;
      statement.setQueryTimeout(queryTimeout);
      fail("Should have thrown exception for wrong value");
    } catch (SQLException ex) {
      assertEquals("Illegal value for query timeout", ex.getMessage());
    }

    statement.setQueryTimeout(0);
    assertEquals(30, statement.getQueryTimeout());
  }

  @Test
  public void testGetWarnings() throws SQLException {
    VitessConnection mockConn = Mockito.mock(VitessConnection.class);

    VitessStatement statement = new VitessStatement(mockConn);
    assertNull(statement.getWarnings());
  }

  @Test
  public void testGetFetchDirection() throws SQLException {
    VitessConnection mockConn = Mockito.mock(VitessConnection.class);

    VitessStatement statement = new VitessStatement(mockConn);
    assertEquals(ResultSet.FETCH_FORWARD, statement.getFetchDirection());
  }

  @Test
  public void testGetFetchSize() throws SQLException {
    VitessConnection mockConn = Mockito.mock(VitessConnection.class);

    VitessStatement statement = new VitessStatement(mockConn);
    assertEquals(0, statement.getFetchSize());
  }

  @Test
  public void testGetResultSetConcurrency() throws SQLException {
    VitessConnection mockConn = Mockito.mock(VitessConnection.class);

    VitessStatement statement = new VitessStatement(mockConn);
    assertEquals(ResultSet.CONCUR_READ_ONLY, statement.getResultSetConcurrency());
  }

  @Test
  public void testGetResultSetType() throws SQLException {
    VitessConnection mockConn = Mockito.mock(VitessConnection.class);

    VitessStatement statement = new VitessStatement(mockConn);
    assertEquals(ResultSet.TYPE_FORWARD_ONLY, statement.getResultSetType());
  }

  @Test
  public void testIsClosed() throws SQLException {
    VitessConnection mockConn = Mockito.mock(VitessConnection.class);

    VitessStatement statement = new VitessStatement(mockConn);
    assertFalse(statement.isClosed());
    statement.close();
    assertTrue(statement.isClosed());
  }

  @Test
  public void testAutoGeneratedKeys() throws SQLException {
    VitessConnection mockConn = Mockito.mock(VitessConnection.class);
    VTGateConnection mockVtGateConn = Mockito.mock(VTGateConnection.class);
    Cursor mockCursor = Mockito.mock(Cursor.class);
    SQLFuture mockSqlFutureCursor = Mockito.mock(SQLFuture.class);

    Mockito.when(mockConn.getVtGateConn()).thenReturn(mockVtGateConn);
    Mockito.when(mockVtGateConn.execute(nullable(Context.class), nullable(String.class), nullable(Map.class), nullable(VTSession.class)))
        .thenReturn(mockSqlFutureCursor);
    Mockito.when(mockSqlFutureCursor.checkedGet()).thenReturn(mockCursor);
    Mockito.when(mockCursor.getFields()).thenReturn(Query.QueryResult.getDefaultInstance().getFieldsList());

    VitessStatement statement = new VitessStatement(mockConn);
    long expectedFirstGeneratedId = 121;
    long[] expectedGeneratedIds = {121, 122, 123, 124, 125};
    int expectedAffectedRows = 5;
    Mockito.when(mockCursor.getInsertId()).thenReturn(expectedFirstGeneratedId);
    Mockito.when(mockCursor.getRowsAffected()).thenReturn(Long.valueOf(expectedAffectedRows));

    //Executing Insert Statement
    int updateCount = statement.executeUpdate(sqlInsert, Statement.RETURN_GENERATED_KEYS);
    assertEquals(expectedAffectedRows, updateCount);

    ResultSet rs = statement.getGeneratedKeys();
    int i = 0;
    while (rs.next()) {
      long generatedId = rs.getLong(1);
      assertEquals(expectedGeneratedIds[i++], generatedId);
    }

    //Fetching Generated Keys without notifying the driver
    statement.executeUpdate(sqlInsert);
    try {
      statement.getGeneratedKeys();
      fail("Should have thrown exception for not setting autoGeneratedKey flag");
    } catch (SQLException ex) {
      assertEquals("Generated keys not requested. You need to specify Statement"
              + ".RETURN_GENERATED_KEYS to Statement.executeUpdate() or Connection"
              + ".prepareStatement()",
          ex.getMessage());
    }

    //Fetching Generated Keys on update query
    expectedFirstGeneratedId = 0;
    Mockito.when(mockCursor.getInsertId()).thenReturn(expectedFirstGeneratedId);
    updateCount = statement.executeUpdate(sqlUpdate, Statement.RETURN_GENERATED_KEYS);
    assertEquals(expectedAffectedRows, updateCount);

    rs = statement.getGeneratedKeys();
    assertFalse(rs.next());
  }

  @Test
  public void testAddBatch() throws Exception {
    VitessConnection mockConn = Mockito.mock(VitessConnection.class);
    VitessStatement statement = new VitessStatement(mockConn);
    statement.addBatch(sqlInsert);
    Field privateStringField = VitessStatement.class.getDeclaredField("batchedArgs");
    privateStringField.setAccessible(true);
    assertEquals(sqlInsert, ((List<String>) privateStringField.get(statement)).get(0));
  }

  @Test
  public void testClearBatch() throws Exception {
    VitessConnection mockConn = Mockito.mock(VitessConnection.class);
    VitessStatement statement = new VitessStatement(mockConn);
    statement.addBatch(sqlInsert);
    statement.clearBatch();
    Field privateStringField = VitessStatement.class.getDeclaredField("batchedArgs");
    privateStringField.setAccessible(true);
    assertTrue(((List<String>) privateStringField.get(statement)).isEmpty());
  }

  @Test
  public void testExecuteBatch() throws SQLException {
    VitessConnection mockConn = Mockito.mock(VitessConnection.class);
    VitessStatement statement = new VitessStatement(mockConn);
    int[] updateCounts = statement.executeBatch();
    assertEquals(0, updateCounts.length);

    VTGateConnection mockVtGateConn = Mockito.mock(VTGateConnection.class);
    Mockito.when(mockConn.getVtGateConn()).thenReturn(mockVtGateConn);
    Mockito.when(mockConn.getAutoCommit()).thenReturn(true);

    SQLFuture mockSqlFutureCursor = Mockito.mock(SQLFuture.class);
    Mockito.when(
        mockVtGateConn.executeBatch(nullable(Context.class), nullable(List.class), nullable(List.class), nullable(VTSession.class)))
        .thenReturn(mockSqlFutureCursor);

    List<CursorWithError> mockCursorWithErrorList = new ArrayList<>();
    Mockito.when(mockSqlFutureCursor.checkedGet()).thenReturn(mockCursorWithErrorList);

    CursorWithError mockCursorWithError1 = Mockito.mock(CursorWithError.class);
    Mockito.when(mockCursorWithError1.getError()).thenReturn(null);
    Mockito.when(mockCursorWithError1.getCursor()).thenReturn(Mockito.mock(Cursor.class));
    mockCursorWithErrorList.add(mockCursorWithError1);

    statement.addBatch(sqlUpdate);
    updateCounts = statement.executeBatch();
    assertEquals(1, updateCounts.length);

    CursorWithError mockCursorWithError2 = Mockito.mock(CursorWithError.class);
    Vtrpc.RPCError rpcError = Vtrpc.RPCError.newBuilder()
        .setMessage("statement execute batch error").build();
    Mockito.when(mockCursorWithError2.getError()).thenReturn(rpcError);
    mockCursorWithErrorList.add(mockCursorWithError2);
    statement.addBatch(sqlUpdate);
    statement.addBatch(sqlUpdate);
    try {
      statement.executeBatch();
      fail("Should have thrown Exception");
    } catch (BatchUpdateException ex) {
      assertEquals(rpcError.toString(), ex.getMessage());
      assertEquals(2, ex.getUpdateCounts().length);
      assertEquals(Statement.EXECUTE_FAILED, ex.getUpdateCounts()[1]);
    }

  }

  @Test
  public void testBatchGeneratedKeys() throws SQLException {
    VitessConnection mockConn = Mockito.mock(VitessConnection.class);
    VitessStatement statement = new VitessStatement(mockConn);
    Cursor mockCursor = Mockito.mock(Cursor.class);
    SQLFuture mockSqlFutureCursor = Mockito.mock(SQLFuture.class);

    VTGateConnection mockVtGateConn = Mockito.mock(VTGateConnection.class);
    Mockito.when(mockConn.getVtGateConn()).thenReturn(mockVtGateConn);
    Mockito.when(mockConn.getAutoCommit()).thenReturn(true);

    Mockito.when(mockSqlFutureCursor.checkedGet()).thenReturn(mockCursor);
    Mockito.when(mockCursor.getFields()).thenReturn(Query.QueryResult.getDefaultInstance().getFieldsList());

    Mockito.when(
        mockVtGateConn.executeBatch(nullable(Context.class), nullable(List.class), nullable(List.class), nullable(VTSession.class)))
        .thenReturn(mockSqlFutureCursor);
    List<CursorWithError> mockCursorWithErrorList = new ArrayList<>();
    Mockito.when(mockSqlFutureCursor.checkedGet()).thenReturn(mockCursorWithErrorList);

    CursorWithError mockCursorWithError = Mockito.mock(CursorWithError.class);
    Mockito.when(mockCursorWithError.getError()).thenReturn(null);
    Mockito.when(mockCursorWithError.getCursor()).thenReturn(mockCursor);
    mockCursorWithErrorList.add(mockCursorWithError);

    long expectedFirstGeneratedId = 121;
    long[] expectedGeneratedIds = {121, 122, 123, 124, 125};
    Mockito.when(mockCursor.getInsertId()).thenReturn(expectedFirstGeneratedId);
    Mockito.when(mockCursor.getRowsAffected()).thenReturn(Long.valueOf(expectedGeneratedIds.length));

    statement.addBatch(sqlInsert);
    statement.executeBatch();

    ResultSet rs = statement.getGeneratedKeys();
    int i = 0;
    while (rs.next()) {
      long generatedId = rs.getLong(1);
      assertEquals(expectedGeneratedIds[i++], generatedId);
    }
  }

  @Test
  public void testBatchUpsertGeneratedKeys() throws SQLException {
    VitessConnection mockConn = Mockito.mock(VitessConnection.class);
    VitessStatement statement = new VitessStatement(mockConn);
    Cursor mockCursor = Mockito.mock(Cursor.class);
    SQLFuture mockSqlFutureCursor = Mockito.mock(SQLFuture.class);

    VTGateConnection mockVtGateConn = Mockito.mock(VTGateConnection.class);
    Mockito.when(mockConn.getVtGateConn()).thenReturn(mockVtGateConn);
    Mockito.when(mockConn.getAutoCommit()).thenReturn(true);

    Mockito.when(mockSqlFutureCursor.checkedGet()).thenReturn(mockCursor);
    Mockito.when(mockCursor.getFields()).thenReturn(Query.QueryResult.getDefaultInstance().getFieldsList());

    Mockito.when(
        mockVtGateConn.executeBatch(nullable(Context.class), nullable(List.class), nullable(List.class), nullable(VTSession.class)))
        .thenReturn(mockSqlFutureCursor);
    List<CursorWithError> mockCursorWithErrorList = new ArrayList<>();
    Mockito.when(mockSqlFutureCursor.checkedGet()).thenReturn(mockCursorWithErrorList);

    CursorWithError mockCursorWithError = Mockito.mock(CursorWithError.class);
    Mockito.when(mockCursorWithError.getError()).thenReturn(null);
    Mockito.when(mockCursorWithError.getCursor()).thenReturn(mockCursor);
    mockCursorWithErrorList.add(mockCursorWithError);

    long expectedFirstGeneratedId = 121;
    long[] expectedGeneratedIds = {121, 122};
    Mockito.when(mockCursor.getInsertId()).thenReturn(expectedFirstGeneratedId);
    Mockito.when(mockCursor.getRowsAffected()).thenReturn(Long.valueOf(expectedGeneratedIds.length));

    statement.addBatch(sqlUpsert);
    statement.executeBatch();

    ResultSet rs = statement.getGeneratedKeys();
    int i = 0;
    while (rs.next()) {
      long generatedId = rs.getLong(1);
      assertEquals(expectedGeneratedIds[i], generatedId);
      assertEquals(i, 0); // we should only have one
      i++;
    }

    VitessStatement noUpdate = new VitessStatement(mockConn);
    Mockito.when(mockCursor.getInsertId()).thenReturn(0L);
    Mockito.when(mockCursor.getRowsAffected()).thenReturn(1L);

    noUpdate.addBatch(sqlUpsert);
    noUpdate.executeBatch();

    ResultSet empty = noUpdate.getGeneratedKeys();
    assertFalse(empty.next());
  }
}
