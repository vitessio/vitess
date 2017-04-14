package io.vitess.jdbc;

import io.vitess.client.Context;
import io.vitess.client.SQLFuture;
import io.vitess.client.VTGateConn;
import io.vitess.client.VTGateTx;
import io.vitess.client.cursor.Cursor;
import io.vitess.client.cursor.CursorWithError;
import io.vitess.proto.Query;
import io.vitess.proto.Topodata;
import io.vitess.proto.Vtrpc;
import io.vitess.util.Constants;
import java.lang.reflect.Field;
import java.sql.BatchUpdateException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Created by harshit.gangal on 19/01/16.
 */

@RunWith(PowerMockRunner.class) @PrepareForTest({VTGateConn.class,
    Vtrpc.RPCError.class}) public class VitessStatementTest {

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
                Matchers.any(Topodata.TabletType.class), Matchers.any(Query.ExecuteOptions.IncludedFields.class))).thenReturn(mockSqlFutureCursor);
        PowerMockito.when(mockVtGateConn
            .execute(Matchers.any(Context.class), Matchers.anyString(), Matchers.anyMap(),
                Matchers.any(Topodata.TabletType.class), Matchers.any(Query.ExecuteOptions.IncludedFields.class))).thenReturn(mockSqlFutureCursor);
        PowerMockito.when(mockVtGateConn
            .executeKeyspaceIds(Matchers.any(Context.class), Matchers.anyString(),
                Matchers.anyString(), Matchers.anyCollection(), Matchers.anyMap(),
                Matchers.any(Topodata.TabletType.class), Matchers.any(Query.ExecuteOptions.IncludedFields.class))).thenReturn(mockSqlFutureCursor);
        PowerMockito.when(mockConn.getExecuteType())
            .thenReturn(Constants.QueryExecuteType.SIMPLE);
        PowerMockito.when(mockConn.isSimpleExecute()).thenReturn(true);
        PowerMockito.when(mockSqlFutureCursor.checkedGet()).thenReturn(mockCursor);
        PowerMockito.when(mockSqlFutureVtGateTx.checkedGet()).thenReturn(mockVtGateTx);
        PowerMockito.when(mockCursor.getFields()).thenReturn(Query.QueryResult.getDefaultInstance().getFieldsList());

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
                Matchers.any(Topodata.TabletType.class), Matchers.any(Query.ExecuteOptions.IncludedFields.class))).thenReturn(mockSqlFutureCursor);
        PowerMockito.when(mockVtGateConn
            .streamExecute(Matchers.any(Context.class), Matchers.anyString(), Matchers.anyMap(),
                Matchers.any(Topodata.TabletType.class), Matchers.any(Query.ExecuteOptions.IncludedFields.class))).thenReturn(mockCursor);
        PowerMockito.when(mockVtGateConn
            .executeKeyspaceIds(Matchers.any(Context.class), Matchers.anyString(),
                Matchers.anyString(), Matchers.anyCollection(), Matchers.anyMap(),
                Matchers.any(Topodata.TabletType.class), Matchers.any(Query.ExecuteOptions.IncludedFields.class))).thenReturn(mockSqlFutureCursor);
        PowerMockito.when(mockConn.getExecuteType())
            .thenReturn(Constants.QueryExecuteType.STREAM);
        PowerMockito.when(mockSqlFutureCursor.checkedGet()).thenReturn(mockCursor);
        PowerMockito.when(mockSqlFutureVtGateTx.checkedGet()).thenReturn(mockVtGateTx);
        PowerMockito.when(mockCursor.getFields()).thenReturn(Query.QueryResult.getDefaultInstance().getFieldsList());

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
                Matchers.any(Topodata.TabletType.class), Matchers.any(Query.ExecuteOptions.IncludedFields.class))).thenReturn(mockSqlFutureCursor);
        PowerMockito.when(mockVtGateConn
            .execute(Matchers.any(Context.class), Matchers.anyString(), Matchers.anyMap(),
                Matchers.any(Topodata.TabletType.class), Matchers.any(Query.ExecuteOptions.IncludedFields.class))).thenReturn(mockSqlFutureCursor);
        PowerMockito.when(mockVtGateConn
            .executeKeyspaceIds(Matchers.any(Context.class), Matchers.anyString(),
                Matchers.anyString(), Matchers.anyCollection(), Matchers.anyMap(),
                Matchers.any(Topodata.TabletType.class), Matchers.any(Query.ExecuteOptions.IncludedFields.class))).thenReturn(mockSqlFutureCursor);
        PowerMockito.when(mockSqlFutureCursor.checkedGet()).thenReturn(mockCursor);
        PowerMockito.when(mockSqlFutureVtGateTx.checkedGet()).thenReturn(mockVtGateTx);
        PowerMockito.when(mockCursor.getFields()).thenReturn(Query.QueryResult.getDefaultInstance().getFieldsList());

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
        List<Query.Field> mockFieldList = PowerMockito.spy(new ArrayList<Query.Field>());

        PowerMockito.when(mockConn.getKeyspace()).thenReturn("test_keyspace");
        PowerMockito.when(mockConn.getVtGateConn()).thenReturn(mockVtGateConn);
        PowerMockito.when(mockConn.getTabletType()).thenReturn(Topodata.TabletType.MASTER);
        PowerMockito.when(mockVtGateConn
            .execute(Matchers.any(Context.class), Matchers.anyString(), Matchers.anyMap(),
                Matchers.any(Topodata.TabletType.class), Matchers.any(Query.ExecuteOptions.IncludedFields.class))).thenReturn(mockSqlFutureCursor);
        PowerMockito.when(mockVtGateConn
            .executeKeyspaceIds(Matchers.any(Context.class), Matchers.anyString(),
                Matchers.anyString(), Matchers.anyCollection(), Matchers.anyMap(),
                Matchers.any(Topodata.TabletType.class), Matchers.any(Query.ExecuteOptions.IncludedFields.class))).thenReturn(mockSqlFutureCursor);
        PowerMockito.when(mockConn.getVtGateTx()).thenReturn(null);
        PowerMockito.when(mockVtGateConn.begin(Matchers.any(Context.class)))
            .thenReturn(mockSqlFutureVtGateTx);

        PowerMockito.when(mockVtGateTx
            .execute(Matchers.any(Context.class), Matchers.anyString(), Matchers.anyMap(),
                Matchers.any(Topodata.TabletType.class), Matchers.any(Query.ExecuteOptions.IncludedFields.class))).thenReturn(mockSqlFutureCursor);
        PowerMockito.when(mockConn.getAutoCommit()).thenReturn(true);
        PowerMockito.when(mockConn.getExecuteType())
            .thenReturn(Constants.QueryExecuteType.SIMPLE);
        PowerMockito.when(mockConn.isSimpleExecute()).thenReturn(true);
        PowerMockito.when(mockVtGateTx.commit(Matchers.any(Context.class)))
            .thenReturn(mockSqlFutureCursor);

        PowerMockito.when(mockSqlFutureCursor.checkedGet()).thenReturn(mockCursor);
        PowerMockito.when(mockSqlFutureVtGateTx.checkedGet()).thenReturn(mockVtGateTx);
        PowerMockito.when(mockCursor.getFields()).thenReturn(mockFieldList);

        VitessStatement statement = new VitessStatement(mockConn);
        try {

            int fieldSize = 5;
            PowerMockito.when(mockCursor.getFields()).thenReturn(mockFieldList);
            PowerMockito.doReturn(fieldSize).when(mockFieldList).size();
            PowerMockito.doReturn(false).when(mockFieldList).isEmpty();

            boolean hasResultSet = statement.execute(sqlSelect);
            Assert.assertTrue(hasResultSet);
            Assert.assertNotNull(statement.getResultSet());

            hasResultSet = statement.execute(sqlShow);
            Assert.assertTrue(hasResultSet);
            Assert.assertNotNull(statement.getResultSet());

            int mockUpdateCount = 10;
            PowerMockito.when(mockCursor.getFields()).thenReturn(Query.QueryResult.getDefaultInstance().getFieldsList());
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
                Matchers.any(Topodata.TabletType.class), Matchers.any(Query.ExecuteOptions.IncludedFields.class))).thenReturn(mockSqlFuture);
        PowerMockito.when(mockSqlFuture.checkedGet()).thenReturn(mockCursor);
        PowerMockito.when(mockCursor.getFields()).thenReturn(Query.QueryResult.getDefaultInstance().getFieldsList());

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
                Matchers.any(Topodata.TabletType.class), Matchers.any(Query.ExecuteOptions.IncludedFields.class))).thenReturn(mockSqlFutureCursor);
        PowerMockito.when(mockConn.getExecuteType())
            .thenReturn(Constants.QueryExecuteType.SIMPLE);
        PowerMockito.when(mockConn.isSimpleExecute()).thenReturn(true);
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
                Matchers.any(Topodata.TabletType.class), Matchers.any(Query.ExecuteOptions.IncludedFields.class))).thenReturn(mockSqlFutureCursor);
        PowerMockito.when(mockVtGateConn
            .execute(Matchers.any(Context.class), Matchers.anyString(), Matchers.anyMap(),
                Matchers.any(Topodata.TabletType.class), Matchers.any(Query.ExecuteOptions.IncludedFields.class))).thenReturn(mockSqlFutureCursor);
        PowerMockito.when(mockVtGateConn
            .executeKeyspaceIds(Matchers.any(Context.class), Matchers.anyString(),
                Matchers.anyString(), Matchers.anyCollection(), Matchers.anyMap(),
                Matchers.any(Topodata.TabletType.class), Matchers.any(Query.ExecuteOptions.IncludedFields.class))).thenReturn(mockSqlFutureCursor);
        PowerMockito.when(mockSqlFutureCursor.checkedGet()).thenReturn(mockCursor);
        PowerMockito.when(mockSqlFutureVtGateTx.checkedGet()).thenReturn(mockVtGateTx);
        PowerMockito.when(mockCursor.getFields()).thenReturn(Query.QueryResult.getDefaultInstance().getFieldsList());
        PowerMockito.when(mockConn.getTabletType()).thenReturn(Topodata.TabletType.MASTER);

        VitessStatement statement = new VitessStatement(mockConn);
        try {

            long expectedFirstGeneratedId = 121;
            long[] expectedGeneratedIds = {121, 122, 123, 124, 125};
            int expectedAffectedRows = 5;
            PowerMockito.when(mockCursor.getInsertId()).thenReturn(expectedFirstGeneratedId);
            PowerMockito.when(mockCursor.getRowsAffected())
                .thenReturn(Long.valueOf(expectedAffectedRows));

            //Executing Insert Statement
            int updateCount = statement.executeUpdate(sqlInsert, Statement.RETURN_GENERATED_KEYS);
            Assert.assertEquals(expectedAffectedRows, updateCount);

            ResultSet rs = statement.getGeneratedKeys();
            int i = 0;
            while (rs.next()) {
                long generatedId = rs.getLong(1);
                Assert.assertEquals(expectedGeneratedIds[i++], generatedId);
            }

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
            expectedFirstGeneratedId = 0;
            PowerMockito.when(mockCursor.getInsertId()).thenReturn(expectedFirstGeneratedId);
            updateCount = statement.executeUpdate(sqlUpdate, Statement.RETURN_GENERATED_KEYS);
            Assert.assertEquals(expectedAffectedRows, updateCount);

            rs = statement.getGeneratedKeys();
            Assert.assertFalse(rs.next());

        } catch (SQLException e) {
            Assert.fail("Test failed " + e.getMessage());
        }
    }

    @Test public void testAddBatch() throws SQLException {
        VitessConnection mockConn = PowerMockito.mock(VitessConnection.class);
        VitessStatement statement = new VitessStatement(mockConn);
        statement.addBatch(sqlInsert);
        try {
            Field privateStringField = VitessStatement.class.getDeclaredField("batchedArgs");
            privateStringField.setAccessible(true);
            Assert
                .assertEquals(sqlInsert, ((List<String>) privateStringField.get(statement)).get(0));
        } catch (NoSuchFieldException e) {
            Assert.fail("Private Field should exists: batchedArgs");
        } catch (IllegalAccessException e) {
            Assert.fail("Private Field should be accessible: batchedArgs");
        }
    }

    @Test public void testClearBatch() throws SQLException {
        VitessConnection mockConn = PowerMockito.mock(VitessConnection.class);
        VitessStatement statement = new VitessStatement(mockConn);
        statement.addBatch(sqlInsert);
        statement.clearBatch();
        try {
            Field privateStringField = VitessStatement.class.getDeclaredField("batchedArgs");
            privateStringField.setAccessible(true);
            Assert.assertTrue(((List<String>) privateStringField.get(statement)).isEmpty());
        } catch (NoSuchFieldException e) {
            Assert.fail("Private Field should exists: batchedArgs");
        } catch (IllegalAccessException e) {
            Assert.fail("Private Field should be accessible: batchedArgs");
        }
    }

    @Test public void testExecuteBatch() throws SQLException {
        VitessConnection mockConn = PowerMockito.mock(VitessConnection.class);
        VitessStatement statement = new VitessStatement(mockConn);
        int[] updateCounts = statement.executeBatch();
        Assert.assertEquals(0, updateCounts.length);

        VTGateConn mockVtGateConn = PowerMockito.mock(VTGateConn.class);
        PowerMockito.when(mockConn.getVtGateConn()).thenReturn(mockVtGateConn);
        PowerMockito.when(mockConn.getTabletType()).thenReturn(Topodata.TabletType.MASTER);
        PowerMockito.when(mockConn.getAutoCommit()).thenReturn(true);

        SQLFuture mockSqlFutureCursor = PowerMockito.mock(SQLFuture.class);
        PowerMockito.when(mockVtGateConn
            .executeBatch(Matchers.any(Context.class), Matchers.anyList(), Matchers.anyList(),
                Matchers.any(Topodata.TabletType.class), Matchers.any(Query.ExecuteOptions.IncludedFields.class))).thenReturn(mockSqlFutureCursor);

        List<CursorWithError> mockCursorWithErrorList = new ArrayList<>();
        PowerMockito.when(mockSqlFutureCursor.checkedGet()).thenReturn(mockCursorWithErrorList);

        CursorWithError mockCursorWithError1 = PowerMockito.mock(CursorWithError.class);
        PowerMockito.when(mockCursorWithError1.getError()).thenReturn(null);
        PowerMockito.when(mockCursorWithError1.getCursor())
            .thenReturn(PowerMockito.mock(Cursor.class));
        mockCursorWithErrorList.add(mockCursorWithError1);

        statement.addBatch(sqlUpdate);
        updateCounts = statement.executeBatch();
        Assert.assertEquals(1, updateCounts.length);

        CursorWithError mockCursorWithError2 = PowerMockito.mock(CursorWithError.class);
        Vtrpc.RPCError rpcError = Vtrpc.RPCError.newBuilder().setMessage("statement execute batch error").build();
        PowerMockito.when(mockCursorWithError2.getError())
            .thenReturn(rpcError);
        mockCursorWithErrorList.add(mockCursorWithError2);
        statement.addBatch(sqlUpdate);
        statement.addBatch(sqlUpdate);
        try {
            statement.executeBatch();
            Assert.fail("Should have thrown Exception");
        } catch (BatchUpdateException ex) {
            Assert.assertEquals(rpcError.toString(), ex.getMessage());
            Assert.assertEquals(2, ex.getUpdateCounts().length);
            Assert.assertEquals(Statement.EXECUTE_FAILED, ex.getUpdateCounts()[1]);
        }

    }
}
