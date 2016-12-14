package com.flipkart.vitess.jdbc.test;

import com.flipkart.vitess.jdbc.VitessConnection;
import com.flipkart.vitess.jdbc.VitessStatement;
import com.flipkart.vitess.util.Constants;
import com.youtube.vitess.client.Context;
import com.youtube.vitess.client.SQLFuture;
import com.youtube.vitess.client.VTGateConn;
import com.youtube.vitess.client.VTGateTx;
import com.youtube.vitess.client.cursor.Cursor;
import com.youtube.vitess.proto.Query;
import com.youtube.vitess.proto.Topodata;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by harshit.gangal on 19/01/16.
 */

@RunWith(PowerMockRunner.class) @PrepareForTest(VTGateConn.class) public class VitessStatementTest {

    private String sqlSelect = "select 1 from test_table";
    private String sqlShow = "show tables";
    private String sqlUpdate = "update test_table set msg = null";
    private String sqlInsert = "insert into test_table(msg) values ('abc')";


    @Test public void testGetConnection() {
        VitessConnection mockConn = PowerMockito.mock(VitessConnection.class);

        VitessStatement statement = new VitessStatement(mockConn);
        try {
            Assert.assertEquals(mockConn, statement.getConnection());
        } catch (SQLException e) {
            Assert.fail("Connection Object is different than expect");
        }
    }

    @Test public void testGetResultSet() {
        VitessConnection mockConn = PowerMockito.mock(VitessConnection.class);
        VitessStatement statement = new VitessStatement(mockConn);
        try {
            Assert.assertEquals(null, statement.getResultSet());
        } catch (SQLException e) {
            Assert.fail("ResultSet Object is different than expect");
        }
    }

    @Test public void testExecuteQuery() throws SQLException {
        VitessConnection mockConn = PowerMockito.mock(VitessConnection.class);
        VTGateConn mockVtGateConn = PowerMockito.mock(VTGateConn.class);
        VTGateTx mockVtGateTx = PowerMockito.mock(VTGateTx.class);
        Cursor mockCursor = PowerMockito.mock(Cursor.class);
        SQLFuture mockSqlFutureCursor = PowerMockito.mock(SQLFuture.class);
        SQLFuture mockSqlFutureVtGateTx = PowerMockito.mock(SQLFuture.class);

        PowerMockito.when(mockConn.getKeyspace()).thenReturn("test_keyspace");
        PowerMockito.when(mockConn.getVtGateConn()).thenReturn(mockVtGateConn);
        PowerMockito.when(mockConn.getVtGateTx()).thenReturn(mockVtGateTx);
        PowerMockito.when(mockVtGateTx
            .execute(Matchers.any(Context.class), Matchers.anyString(), Matchers.anyMap(),
                Matchers.any(Topodata.TabletType.class))).thenReturn(mockSqlFutureCursor);
        PowerMockito.when(mockVtGateConn
            .execute(Matchers.any(Context.class), Matchers.anyString(), Matchers.anyMap(),
                Matchers.any(Topodata.TabletType.class))).thenReturn(mockSqlFutureCursor);
        PowerMockito.when(mockVtGateConn
            .executeKeyspaceIds(Matchers.any(Context.class), Matchers.anyString(),
                Matchers.anyString(), Matchers.anyCollection(), Matchers.anyMap(),
                Matchers.any(Topodata.TabletType.class))).thenReturn(mockSqlFutureCursor);
        PowerMockito.when(mockConn.getExecuteTypeParam())
            .thenReturn(Constants.QueryExecuteType.SIMPLE);
        PowerMockito.when(mockSqlFutureCursor.checkedGet()).thenReturn(mockCursor);
        PowerMockito.when(mockSqlFutureVtGateTx.checkedGet()).thenReturn(mockVtGateTx);
        PowerMockito.when(mockCursor.getFields()).thenReturn(null);

        VitessStatement statement = new VitessStatement(mockConn);
        try {

            //Empty Sql Statement
            try {
                statement.executeQuery("");
                Assert.fail("Should have thrown exception for empty sql");
            } catch (SQLException ex) {
                Assert.assertEquals("SQL statement is not valid", ex.getMessage());
            }

            //select on replica
            PowerMockito.when(mockConn.getTabletType()).thenReturn(Topodata.TabletType.REPLICA);
            ResultSet rs = statement.executeQuery(sqlSelect);
            Assert.assertEquals(-1, statement.getUpdateCount());

            //show query
            rs = statement.executeQuery(sqlShow);
            Assert.assertEquals(-1, statement.getUpdateCount());

            //select on master when tx is null and autocommit is false
            PowerMockito.when(mockConn.getTabletType()).thenReturn(Topodata.TabletType.MASTER);
            PowerMockito.when(mockConn.getVtGateTx()).thenReturn(null);
            PowerMockito.when(mockVtGateConn.begin(Matchers.any(Context.class)))
                .thenReturn(mockSqlFutureVtGateTx);
            PowerMockito.when(mockConn.getAutoCommit()).thenReturn(false);
            rs = statement.executeQuery(sqlSelect);
            Assert.assertEquals(-1, statement.getUpdateCount());

            //when returned cursor is null
            PowerMockito.when(mockSqlFutureCursor.checkedGet()).thenReturn(null);
            try {
                statement.executeQuery(sqlSelect);
                Assert.fail("Should have thrown exception for cursor null");
            } catch (SQLException ex) {
                Assert.assertEquals("Failed to execute this method", ex.getMessage());
            }

        } catch (SQLException e) {
            Assert.fail("Test failed " + e.getMessage());
        }
    }

    @Test public void testExecuteQueryWithStreamExecuteType() throws SQLException {
        VitessConnection mockConn = PowerMockito.mock(VitessConnection.class);
        VTGateConn mockVtGateConn = PowerMockito.mock(VTGateConn.class);
        VTGateTx mockVtGateTx = PowerMockito.mock(VTGateTx.class);
        Cursor mockCursor = PowerMockito.mock(Cursor.class);
        SQLFuture mockSqlFutureCursor = PowerMockito.mock(SQLFuture.class);
        SQLFuture mockSqlFutureVtGateTx = PowerMockito.mock(SQLFuture.class);

        PowerMockito.when(mockConn.getKeyspace()).thenReturn("test_keyspace");
        PowerMockito.when(mockConn.getVtGateConn()).thenReturn(mockVtGateConn);
        PowerMockito.when(mockConn.getVtGateTx()).thenReturn(mockVtGateTx);
        PowerMockito.when(mockVtGateTx
            .execute(Matchers.any(Context.class), Matchers.anyString(), Matchers.anyMap(),
                Matchers.any(Topodata.TabletType.class))).thenReturn(mockSqlFutureCursor);
        PowerMockito.when(mockVtGateConn
            .streamExecute(Matchers.any(Context.class), Matchers.anyString(), Matchers.anyMap(),
                Matchers.any(Topodata.TabletType.class))).thenReturn(mockCursor);
        PowerMockito.when(mockVtGateConn
            .executeKeyspaceIds(Matchers.any(Context.class), Matchers.anyString(),
                Matchers.anyString(), Matchers.anyCollection(), Matchers.anyMap(),
                Matchers.any(Topodata.TabletType.class))).thenReturn(mockSqlFutureCursor);
        PowerMockito.when(mockConn.getExecuteTypeParam())
            .thenReturn(Constants.QueryExecuteType.STREAM);
        PowerMockito.when(mockSqlFutureCursor.checkedGet()).thenReturn(mockCursor);
        PowerMockito.when(mockSqlFutureVtGateTx.checkedGet()).thenReturn(mockVtGateTx);
        PowerMockito.when(mockCursor.getFields()).thenReturn(null);

        VitessStatement statement = new VitessStatement(mockConn);
        try {

            //Empty Sql Statement
            try {
                statement.executeQuery("");
                Assert.fail("Should have thrown exception for empty sql");
            } catch (SQLException ex) {
                Assert.assertEquals("SQL statement is not valid", ex.getMessage());
            }

            //select on replica
            PowerMockito.when(mockConn.getTabletType()).thenReturn(Topodata.TabletType.REPLICA);
            ResultSet rs = statement.executeQuery(sqlSelect);
            Assert.assertEquals(-1, statement.getUpdateCount());

            //show query
            rs = statement.executeQuery(sqlShow);
            Assert.assertEquals(-1, statement.getUpdateCount());

            //select on master when tx is null and autocommit is false
            PowerMockito.when(mockConn.getTabletType()).thenReturn(Topodata.TabletType.MASTER);
            PowerMockito.when(mockConn.getVtGateTx()).thenReturn(null);
            PowerMockito.when(mockVtGateConn.begin(Matchers.any(Context.class)))
                .thenReturn(mockSqlFutureVtGateTx);
            PowerMockito.when(mockConn.getAutoCommit()).thenReturn(false);
            rs = statement.executeQuery(sqlSelect);
            Assert.assertEquals(-1, statement.getUpdateCount());

            //when returned cursor is null
            PowerMockito.when(mockSqlFutureCursor.checkedGet()).thenReturn(null);
            try {
                statement.executeQuery(sqlSelect);
                Assert.fail("Should have thrown exception for cursor null");
            } catch (SQLException ex) {
                Assert.assertEquals("Failed to execute this method", ex.getMessage());
            }

        } catch (SQLException e) {
            Assert.fail("Test failed " + e.getMessage());
        }
    }


    @Test public void testExecuteUpdate() throws SQLException {
        VitessConnection mockConn = PowerMockito.mock(VitessConnection.class);
        VTGateConn mockVtGateConn = PowerMockito.mock(VTGateConn.class);
        VTGateTx mockVtGateTx = PowerMockito.mock(VTGateTx.class);
        Cursor mockCursor = PowerMockito.mock(Cursor.class);
        SQLFuture mockSqlFutureCursor = PowerMockito.mock(SQLFuture.class);
        SQLFuture mockSqlFutureVtGateTx = PowerMockito.mock(SQLFuture.class);
        List<Query.Field> fieldList = PowerMockito.mock(ArrayList.class);

        PowerMockito.when(mockConn.getVtGateConn()).thenReturn(mockVtGateConn);
        PowerMockito.when(mockConn.getVtGateTx()).thenReturn(mockVtGateTx);
        PowerMockito.when(mockVtGateTx
            .execute(Matchers.any(Context.class), Matchers.anyString(), Matchers.anyMap(),
                Matchers.any(Topodata.TabletType.class))).thenReturn(mockSqlFutureCursor);
        PowerMockito.when(mockVtGateConn
            .execute(Matchers.any(Context.class), Matchers.anyString(), Matchers.anyMap(),
                Matchers.any(Topodata.TabletType.class))).thenReturn(mockSqlFutureCursor);
        PowerMockito.when(mockVtGateConn
            .executeKeyspaceIds(Matchers.any(Context.class), Matchers.anyString(),
                Matchers.anyString(), Matchers.anyCollection(), Matchers.anyMap(),
                Matchers.any(Topodata.TabletType.class))).thenReturn(mockSqlFutureCursor);
        PowerMockito.when(mockSqlFutureCursor.checkedGet()).thenReturn(mockCursor);
        PowerMockito.when(mockSqlFutureVtGateTx.checkedGet()).thenReturn(mockVtGateTx);
        PowerMockito.when(mockCursor.getFields()).thenReturn(null);

        VitessStatement statement = new VitessStatement(mockConn);
        try {

            //exception on executing dml on non master
            PowerMockito.when(mockConn.getTabletType()).thenReturn(Topodata.TabletType.REPLICA);
            try {
                statement.executeUpdate(sqlUpdate);
                Assert.fail("Should have thrown exception for tablet type not being master");
            } catch (SQLException ex) {
                Assert.assertEquals("DML Statement cannot be executed on non master instance type",
                    ex.getMessage());
            }

            //executing dml on master
            PowerMockito.when(mockConn.getTabletType()).thenReturn(Topodata.TabletType.MASTER);
            int updateCount = statement.executeUpdate(sqlUpdate);
            Assert.assertEquals(0, updateCount);

            //tx is null & autoCommit is true
            PowerMockito.when(mockConn.getVtGateTx()).thenReturn(null);
            PowerMockito.when(mockVtGateConn.begin(Matchers.any(Context.class)))
                .thenReturn(mockSqlFutureVtGateTx);
            PowerMockito.when(mockConn.getAutoCommit()).thenReturn(true);
            PowerMockito.when(mockVtGateTx.commit(Matchers.any(Context.class)))
                .thenReturn(mockSqlFutureCursor);
            updateCount = statement.executeUpdate(sqlUpdate);
            Assert.assertEquals(0, updateCount);

            //cursor fields is not null
            PowerMockito.when(mockCursor.getFields()).thenReturn(fieldList);
            PowerMockito.when(fieldList.isEmpty()).thenReturn(false);
            try {
                statement.executeUpdate(sqlSelect);
                Assert.fail("Should have thrown exception for field not null");
            } catch (SQLException ex) {
                Assert.assertEquals("ResultSet generation is not allowed through this method",
                    ex.getMessage());
            }

            //cursor is null
            PowerMockito.when(mockSqlFutureCursor.checkedGet()).thenReturn(null);
            try {
                statement.executeUpdate(sqlUpdate);
                Assert.fail("Should have thrown exception for cursor null");
            } catch (SQLException ex) {
                Assert.assertEquals("Failed to execute this method", ex.getMessage());
            }

        } catch (SQLException e) {
            Assert.fail("Test failed " + e.getMessage());
        }
    }

    @Test public void testExecute() throws SQLException {
        VitessConnection mockConn = PowerMockito.mock(VitessConnection.class);
        VTGateConn mockVtGateConn = PowerMockito.mock(VTGateConn.class);
        VTGateTx mockVtGateTx = PowerMockito.mock(VTGateTx.class);
        Cursor mockCursor = PowerMockito.mock(Cursor.class);
        SQLFuture mockSqlFutureCursor = PowerMockito.mock(SQLFuture.class);
        SQLFuture mockSqlFutureVtGateTx = PowerMockito.mock(SQLFuture.class);
        List<Query.Field> mockFieldList = PowerMockito.mock(ArrayList.class);

        PowerMockito.when(mockConn.getKeyspace()).thenReturn("test_keyspace");
        PowerMockito.when(mockConn.getVtGateConn()).thenReturn(mockVtGateConn);
        PowerMockito.when(mockConn.getTabletType()).thenReturn(Topodata.TabletType.MASTER);
        PowerMockito.when(mockVtGateConn
            .execute(Matchers.any(Context.class), Matchers.anyString(), Matchers.anyMap(),
                Matchers.any(Topodata.TabletType.class))).thenReturn(mockSqlFutureCursor);
        PowerMockito.when(mockVtGateConn
            .executeKeyspaceIds(Matchers.any(Context.class), Matchers.anyString(),
                Matchers.anyString(), Matchers.anyCollection(), Matchers.anyMap(),
                Matchers.any(Topodata.TabletType.class))).thenReturn(mockSqlFutureCursor);
        PowerMockito.when(mockConn.getVtGateTx()).thenReturn(null);
        PowerMockito.when(mockVtGateConn.begin(Matchers.any(Context.class)))
            .thenReturn(mockSqlFutureVtGateTx);

        PowerMockito.when(mockVtGateTx
            .execute(Matchers.any(Context.class), Matchers.anyString(), Matchers.anyMap(),
                Matchers.any(Topodata.TabletType.class))).thenReturn(mockSqlFutureCursor);
        PowerMockito.when(mockConn.getAutoCommit()).thenReturn(true);
        PowerMockito.when(mockConn.getExecuteTypeParam())
            .thenReturn(Constants.QueryExecuteType.SIMPLE);
        PowerMockito.when(mockVtGateTx.commit(Matchers.any(Context.class)))
            .thenReturn(mockSqlFutureCursor);

        PowerMockito.when(mockSqlFutureCursor.checkedGet()).thenReturn(mockCursor);
        PowerMockito.when(mockSqlFutureVtGateTx.checkedGet()).thenReturn(mockVtGateTx);
        PowerMockito.when(mockCursor.getFields()).thenReturn(mockFieldList);

        VitessStatement statement = new VitessStatement(mockConn);
        try {

            int fieldSize = 5;
            PowerMockito.when(mockCursor.getFields()).thenReturn(mockFieldList);
            PowerMockito.when(mockFieldList.size()).thenReturn(fieldSize);
            boolean hasResultSet = statement.execute(sqlSelect);
            Assert.assertTrue(hasResultSet);
            Assert.assertNotNull(statement.getResultSet());

            hasResultSet = statement.execute(sqlShow);
            Assert.assertTrue(hasResultSet);
            Assert.assertNotNull(statement.getResultSet());

            int mockUpdateCount = 10;
            PowerMockito.when(mockCursor.getFields()).thenReturn(null);
            PowerMockito.when(mockCursor.getRowsAffected()).thenReturn((long) mockUpdateCount);
            hasResultSet = statement.execute(sqlUpdate);
            Assert.assertFalse(hasResultSet);
            Assert.assertNull(statement.getResultSet());
            Assert.assertEquals(mockUpdateCount, statement.getUpdateCount());

            //cursor is null
            PowerMockito.when(mockSqlFutureCursor.checkedGet()).thenReturn(null);
            try {
                statement.execute(sqlUpdate);
                Assert.fail("Should have thrown exception for cursor null");
            } catch (SQLException ex) {
                Assert.assertEquals("Failed to execute this method", ex.getMessage());
            }

        } catch (SQLException e) {
            Assert.fail("Test failed " + e.getMessage());
        }
    }

    @Test public void testGetUpdateCount() throws SQLException {
        VitessConnection mockConn = PowerMockito.mock(VitessConnection.class);
        VTGateConn mockVtGateConn = PowerMockito.mock(VTGateConn.class);
        VTGateTx mockVtGateTx = PowerMockito.mock(VTGateTx.class);
        Cursor mockCursor = PowerMockito.mock(Cursor.class);
        SQLFuture mockSqlFuture = PowerMockito.mock(SQLFuture.class);

        PowerMockito.when(mockConn.getVtGateConn()).thenReturn(mockVtGateConn);
        PowerMockito.when(mockConn.getTabletType()).thenReturn(Topodata.TabletType.MASTER);
        PowerMockito.when(mockConn.getVtGateTx()).thenReturn(mockVtGateTx);
        PowerMockito.when(mockVtGateTx
            .execute(Matchers.any(Context.class), Matchers.anyString(), Matchers.anyMap(),
                Matchers.any(Topodata.TabletType.class))).thenReturn(mockSqlFuture);
        PowerMockito.when(mockSqlFuture.checkedGet()).thenReturn(mockCursor);
        PowerMockito.when(mockCursor.getFields()).thenReturn(null);

        VitessStatement statement = new VitessStatement(mockConn);
        try {

            PowerMockito.when(mockCursor.getRowsAffected()).thenReturn(10L);
            int updateCount = statement.executeUpdate(sqlUpdate);
            Assert.assertEquals(10L, updateCount);
            Assert.assertEquals(10L, statement.getUpdateCount());

            // Truncated Update Count
            PowerMockito.when(mockCursor.getRowsAffected())
                .thenReturn((long) Integer.MAX_VALUE + 10);
            updateCount = statement.executeUpdate(sqlUpdate);
            Assert.assertEquals(Integer.MAX_VALUE, updateCount);
            Assert.assertEquals(Integer.MAX_VALUE, statement.getUpdateCount());

            statement.executeQuery(sqlSelect);
            Assert.assertEquals(-1, statement.getUpdateCount());

        } catch (SQLException e) {
            Assert.fail("Test failed " + e.getMessage());
        }
    }

    @Test public void testClose() throws Exception {
        VitessConnection mockConn = PowerMockito.mock(VitessConnection.class);
        VTGateConn mockVtGateConn = PowerMockito.mock(VTGateConn.class);
        Cursor mockCursor = PowerMockito.mock(Cursor.class);
        SQLFuture mockSqlFutureCursor = PowerMockito.mock(SQLFuture.class);

        PowerMockito.when(mockConn.getVtGateConn()).thenReturn(mockVtGateConn);
        PowerMockito.when(mockConn.getTabletType()).thenReturn(Topodata.TabletType.REPLICA);
        PowerMockito.when(mockVtGateConn
            .execute(Matchers.any(Context.class), Matchers.anyString(), Matchers.anyMap(),
                Matchers.any(Topodata.TabletType.class))).thenReturn(mockSqlFutureCursor);
        PowerMockito.when(mockConn.getExecuteTypeParam())
            .thenReturn(Constants.QueryExecuteType.SIMPLE);
        PowerMockito.when(mockSqlFutureCursor.checkedGet()).thenReturn(mockCursor);

        VitessStatement statement = new VitessStatement(mockConn);
        try {
            ResultSet rs = statement.executeQuery(sqlSelect);
            statement.close();
            try {
                statement.executeQuery(sqlSelect);
                Assert.fail("Should have thrown exception for statement closed");
            } catch (SQLException ex) {
                Assert.assertEquals("Statement is closed", ex.getMessage());
            }
        } catch (SQLException e) {
            Assert.fail("Test failed " + e.getMessage());
        }
    }

    @Test public void testGetMaxFieldSize() throws SQLException {
        VitessConnection mockConn = PowerMockito.mock(VitessConnection.class);

        VitessStatement statement = new VitessStatement(mockConn);
        Assert.assertEquals(65535, statement.getMaxFieldSize());
    }

    @Test public void testGetMaxRows() throws SQLException {
        VitessConnection mockConn = PowerMockito.mock(VitessConnection.class);

        VitessStatement statement = new VitessStatement(mockConn);

        statement.setMaxRows(10);
        Assert.assertEquals(10, statement.getMaxRows());

        try {
            statement.setMaxRows(-1);
            Assert.fail("Should have thrown exception for wrong value");
        } catch (SQLException ex) {
            Assert.assertEquals("Illegal value for max row", ex.getMessage());
        }

    }

    @Test public void testGetQueryTimeout() throws SQLException {
        VitessConnection mockConn = PowerMockito.mock(VitessConnection.class);

        VitessStatement statement = new VitessStatement(mockConn);
        Assert.assertEquals(30, statement.getQueryTimeout());
    }

    @Test public void testSetQueryTimeout() throws SQLException {
        VitessConnection mockConn = PowerMockito.mock(VitessConnection.class);

        VitessStatement statement = new VitessStatement(mockConn);

        int queryTimeout = 10;
        statement.setQueryTimeout(queryTimeout);
        Assert.assertEquals(queryTimeout, statement.getQueryTimeout());
        try {
            queryTimeout = -1;
            statement.setQueryTimeout(queryTimeout);
            Assert.fail("Should have thrown exception for wrong value");
        } catch (SQLException ex) {
            Assert.assertEquals("Illegal value for query timeout", ex.getMessage());
        }
    }

    @Test public void testGetWarnings() throws SQLException {
        VitessConnection mockConn = PowerMockito.mock(VitessConnection.class);

        VitessStatement statement = new VitessStatement(mockConn);
        Assert.assertNull(statement.getWarnings());
    }

    @Test public void testGetFetchDirection() throws SQLException {
        VitessConnection mockConn = PowerMockito.mock(VitessConnection.class);

        VitessStatement statement = new VitessStatement(mockConn);
        Assert.assertEquals(ResultSet.FETCH_FORWARD, statement.getFetchDirection());
    }

    @Test public void testGetFetchSize() throws SQLException {
        VitessConnection mockConn = PowerMockito.mock(VitessConnection.class);

        VitessStatement statement = new VitessStatement(mockConn);
        Assert.assertEquals(0, statement.getFetchSize());
    }

    @Test public void testGetResultSetConcurrency() throws SQLException {
        VitessConnection mockConn = PowerMockito.mock(VitessConnection.class);

        VitessStatement statement = new VitessStatement(mockConn);
        Assert.assertEquals(ResultSet.CONCUR_READ_ONLY, statement.getResultSetConcurrency());
    }

    @Test public void testGetResultSetType() throws SQLException {
        VitessConnection mockConn = PowerMockito.mock(VitessConnection.class);

        VitessStatement statement = new VitessStatement(mockConn);
        Assert.assertEquals(ResultSet.TYPE_FORWARD_ONLY, statement.getResultSetType());
    }

    @Test public void testIsClosed() throws SQLException {
        VitessConnection mockConn = PowerMockito.mock(VitessConnection.class);

        VitessStatement statement = new VitessStatement(mockConn);
        Assert.assertFalse(statement.isClosed());
        statement.close();
        Assert.assertTrue(statement.isClosed());
    }

    @Test public void testAutoGeneratedKeys() throws SQLException {
        VitessConnection mockConn = PowerMockito.mock(VitessConnection.class);
        VTGateConn mockVtGateConn = PowerMockito.mock(VTGateConn.class);
        VTGateTx mockVtGateTx = PowerMockito.mock(VTGateTx.class);
        Cursor mockCursor = PowerMockito.mock(Cursor.class);
        SQLFuture mockSqlFutureCursor = PowerMockito.mock(SQLFuture.class);
        SQLFuture mockSqlFutureVtGateTx = PowerMockito.mock(SQLFuture.class);
        List<Query.Field> fieldList = PowerMockito.mock(ArrayList.class);

        PowerMockito.when(mockConn.getVtGateConn()).thenReturn(mockVtGateConn);
        PowerMockito.when(mockConn.getVtGateTx()).thenReturn(mockVtGateTx);
        PowerMockito.when(mockVtGateTx
            .execute(Matchers.any(Context.class), Matchers.anyString(), Matchers.anyMap(),
                Matchers.any(Topodata.TabletType.class))).thenReturn(mockSqlFutureCursor);
        PowerMockito.when(mockVtGateConn
            .execute(Matchers.any(Context.class), Matchers.anyString(), Matchers.anyMap(),
                Matchers.any(Topodata.TabletType.class))).thenReturn(mockSqlFutureCursor);
        PowerMockito.when(mockVtGateConn
            .executeKeyspaceIds(Matchers.any(Context.class), Matchers.anyString(),
                Matchers.anyString(), Matchers.anyCollection(), Matchers.anyMap(),
                Matchers.any(Topodata.TabletType.class))).thenReturn(mockSqlFutureCursor);
        PowerMockito.when(mockSqlFutureCursor.checkedGet()).thenReturn(mockCursor);
        PowerMockito.when(mockSqlFutureVtGateTx.checkedGet()).thenReturn(mockVtGateTx);
        PowerMockito.when(mockCursor.getFields()).thenReturn(null);
        PowerMockito.when(mockConn.getTabletType()).thenReturn(Topodata.TabletType.MASTER);

        VitessStatement statement = new VitessStatement(mockConn);
        try {

            long expectedGeneratedId = 121;
            int expectedAffectedRows = 1;
            PowerMockito.when(mockCursor.getInsertId()).thenReturn(expectedGeneratedId);
            PowerMockito.when(mockCursor.getRowsAffected())
                .thenReturn(Long.valueOf(expectedAffectedRows));

            //Executing Insert Statement
            int updateCount = statement.executeUpdate(sqlInsert, Statement.RETURN_GENERATED_KEYS);
            Assert.assertEquals(expectedAffectedRows, updateCount);

            ResultSet rs = statement.getGeneratedKeys();
            rs.next();
            long generatedId = rs.getLong(1);
            Assert.assertEquals(expectedGeneratedId, generatedId);

            //Fetching Generated Keys without notifying the driver
            statement.executeUpdate(sqlInsert);
            try {
                statement.getGeneratedKeys();
                Assert.fail("Should have thrown exception for not setting autoGeneratedKey flag");
            } catch (SQLException ex) {
                Assert.assertEquals("Generated keys not requested. You need to specify Statement"
                        + ".RETURN_GENERATED_KEYS to Statement.executeUpdate() or Connection.prepareStatement()",
                    ex.getMessage());
            }

            //Fetching Generated Keys on update query
            expectedGeneratedId = 0;
            PowerMockito.when(mockCursor.getInsertId()).thenReturn(expectedGeneratedId);
            updateCount = statement.executeUpdate(sqlUpdate, Statement.RETURN_GENERATED_KEYS);
            Assert.assertEquals(expectedAffectedRows, updateCount);

            rs = statement.getGeneratedKeys();
            Assert.assertFalse(rs.next());

        } catch (SQLException e) {
            Assert.fail("Test failed " + e.getMessage());
        }
    }
}
