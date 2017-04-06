package io.vitess.jdbc;

import com.google.common.collect.ImmutableMap;
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
import java.math.BigDecimal;
import java.sql.BatchUpdateException;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;


/**
 * Created by harshit.gangal on 09/02/16.
 */
@RunWith(PowerMockRunner.class) @PrepareForTest({VTGateConn.class,
    Vtrpc.RPCError.class}) public class VitessPreparedStatementTest {

    private String sqlSelect = "select 1 from test_table";
    private String sqlShow = "show tables";
    private String sqlUpdate = "update test_table set msg = null";
    private String sqlInsert = "insert into test_table(msg) values (?)";

    @Test public void testStatementExecute() throws SQLException {
        VitessConnection mockConn = PowerMockito.mock(VitessConnection.class);
        VitessPreparedStatement preparedStatement;
        try {
            preparedStatement = new VitessPreparedStatement(mockConn, sqlShow);
            preparedStatement.executeQuery(sqlSelect);
            Assert.fail("Should have thrown exception for calling this method");
        } catch (SQLException ex) {
            Assert.assertEquals("This method cannot be called using this class object",
                ex.getMessage());
        }

        try {
            preparedStatement = new VitessPreparedStatement(mockConn, sqlShow);
            preparedStatement.executeUpdate(sqlUpdate);
            Assert.fail("Should have thrown exception for calling this method");
        } catch (SQLException ex) {
            Assert.assertEquals("This method cannot be called using this class object",
                ex.getMessage());
        }

        try {
            preparedStatement = new VitessPreparedStatement(mockConn, sqlShow);
            preparedStatement.execute(sqlShow);
            Assert.fail("Should have thrown exception for calling this method");
        } catch (SQLException ex) {
            Assert.assertEquals("This method cannot be called using this class object",
                ex.getMessage());
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
        PowerMockito.when(mockVtGateConn
            .executeKeyspaceIds(Matchers.any(Context.class), Matchers.anyString(),
                Matchers.anyString(), Matchers.anyCollection(), Matchers.anyMap(),
                Matchers.any(Topodata.TabletType.class), Matchers.any(Query.ExecuteOptions.IncludedFields.class))).thenReturn(mockSqlFutureCursor);
        PowerMockito.when(mockVtGateConn
            .execute(Matchers.any(Context.class), Matchers.anyString(), Matchers.anyMap(),
                Matchers.any(Topodata.TabletType.class), Matchers.any(Query.ExecuteOptions.IncludedFields.class))).thenReturn(mockSqlFutureCursor);
        PowerMockito.when(mockConn.getVtGateTx()).thenReturn(null);
        PowerMockito.when(mockConn.getExecuteType())
            .thenReturn(Constants.QueryExecuteType.SIMPLE);
        PowerMockito.when(mockConn.isSimpleExecute()).thenReturn(true);
        PowerMockito.when(mockVtGateConn.begin(Matchers.any(Context.class)))
            .thenReturn(mockSqlFutureVtGateTx);
        PowerMockito.when(mockVtGateTx
            .execute(Matchers.any(Context.class), Matchers.anyString(), Matchers.anyMap(),
                Matchers.any(Topodata.TabletType.class), Matchers.any(Query.ExecuteOptions.IncludedFields.class))).thenReturn(mockSqlFutureCursor);
        PowerMockito.when(mockSqlFutureCursor.checkedGet()).thenReturn(mockCursor);
        PowerMockito.when(mockSqlFutureVtGateTx.checkedGet()).thenReturn(mockVtGateTx);

        VitessPreparedStatement preparedStatement;
        try {

            //Empty Sql Statement
            try {
                new VitessPreparedStatement(mockConn, "");
                Assert.fail("Should have thrown exception for empty sql");
            } catch (SQLException ex) {
                Assert.assertEquals("SQL statement is not valid", ex.getMessage());
            }

            //show query
            preparedStatement = new VitessPreparedStatement(mockConn, sqlShow);
            ResultSet rs = preparedStatement.executeQuery();
            Assert.assertEquals(-1, preparedStatement.getUpdateCount());

            //select on replica with bind variables
            preparedStatement =
                new VitessPreparedStatement(mockConn, sqlSelect, ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY);
            PowerMockito.when(mockConn.getTabletType()).thenReturn(Topodata.TabletType.REPLICA);
            rs = preparedStatement.executeQuery();
            Assert.assertEquals(-1, preparedStatement.getUpdateCount());

            //select on replica without bind variables
            preparedStatement =
                new VitessPreparedStatement(mockConn, sqlSelect, ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY);
            rs = preparedStatement.executeQuery();
            Assert.assertEquals(-1, preparedStatement.getUpdateCount());


            //select on master
            PowerMockito.when(mockConn.getTabletType()).thenReturn(Topodata.TabletType.MASTER);
            rs = preparedStatement.executeQuery();
            Assert.assertEquals(-1, preparedStatement.getUpdateCount());

            try {
                //when returned cursor is null
                PowerMockito.when(mockSqlFutureCursor.checkedGet()).thenReturn(null);
                preparedStatement.executeQuery();
                Assert.fail("Should have thrown exception for cursor null");
            } catch (SQLException ex) {
                Assert.assertEquals("Failed to execute this method", ex.getMessage());
            }

        } catch (SQLException e) {
            Assert.fail("Test failed " + e.getMessage());
        }
    }

    @Test public void testExecuteQueryWithStream() throws SQLException {
        VitessConnection mockConn = PowerMockito.mock(VitessConnection.class);
        VTGateConn mockVtGateConn = PowerMockito.mock(VTGateConn.class);
        VTGateTx mockVtGateTx = PowerMockito.mock(VTGateTx.class);
        Cursor mockCursor = PowerMockito.mock(Cursor.class);
        SQLFuture mockSqlFutureCursor = PowerMockito.mock(SQLFuture.class);
        SQLFuture mockSqlFutureVtGateTx = PowerMockito.mock(SQLFuture.class);

        PowerMockito.when(mockConn.getKeyspace()).thenReturn("test_keyspace");
        PowerMockito.when(mockConn.getVtGateConn()).thenReturn(mockVtGateConn);
        PowerMockito.when(mockVtGateConn
            .executeKeyspaceIds(Matchers.any(Context.class), Matchers.anyString(),
                Matchers.anyString(), Matchers.anyCollection(), Matchers.anyMap(),
                Matchers.any(Topodata.TabletType.class), Matchers.any(Query.ExecuteOptions.IncludedFields.class))).thenReturn(mockSqlFutureCursor);
        PowerMockito.when(mockVtGateConn
            .streamExecute(Matchers.any(Context.class), Matchers.anyString(), Matchers.anyMap(),
                Matchers.any(Topodata.TabletType.class), Matchers.any(Query.ExecuteOptions.IncludedFields.class))).thenReturn(mockCursor);
        PowerMockito.when(mockVtGateConn
            .execute(Matchers.any(Context.class), Matchers.anyString(), Matchers.anyMap(),
                Matchers.any(Topodata.TabletType.class), Matchers.any(Query.ExecuteOptions.IncludedFields.class))).thenReturn(mockSqlFutureCursor);
        PowerMockito.when(mockConn.getVtGateTx()).thenReturn(null);
        PowerMockito.when(mockVtGateConn.begin(Matchers.any(Context.class)))
            .thenReturn(mockSqlFutureVtGateTx);
        PowerMockito.when(mockVtGateTx
            .execute(Matchers.any(Context.class), Matchers.anyString(), Matchers.anyMap(),
                Matchers.any(Topodata.TabletType.class), Matchers.any(Query.ExecuteOptions.IncludedFields.class))).thenReturn(mockSqlFutureCursor);
        PowerMockito.when(mockSqlFutureCursor.checkedGet()).thenReturn(mockCursor);
        PowerMockito.when(mockSqlFutureVtGateTx.checkedGet()).thenReturn(mockVtGateTx);
        PowerMockito.when(mockConn.getExecuteType())
            .thenReturn(Constants.QueryExecuteType.STREAM);

        VitessPreparedStatement preparedStatement;
        try {

            //Empty Sql Statement
            try {
                new VitessPreparedStatement(mockConn, "");
                Assert.fail("Should have thrown exception for empty sql");
            } catch (SQLException ex) {
                Assert.assertEquals("SQL statement is not valid", ex.getMessage());
            }

            //show query
            preparedStatement = new VitessPreparedStatement(mockConn, sqlShow);
            ResultSet rs = preparedStatement.executeQuery();
            Assert.assertEquals(-1, preparedStatement.getUpdateCount());

            //select on replica with bind variables
            preparedStatement =
                new VitessPreparedStatement(mockConn, sqlSelect, ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY);
            PowerMockito.when(mockConn.getTabletType()).thenReturn(Topodata.TabletType.REPLICA);
            rs = preparedStatement.executeQuery();
            Assert.assertEquals(-1, preparedStatement.getUpdateCount());

            //select on replica without bind variables
            preparedStatement =
                new VitessPreparedStatement(mockConn, sqlSelect, ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY);
            rs = preparedStatement.executeQuery();
            Assert.assertEquals(-1, preparedStatement.getUpdateCount());


            //select on master
            PowerMockito.when(mockConn.getTabletType()).thenReturn(Topodata.TabletType.MASTER);
            rs = preparedStatement.executeQuery();
            Assert.assertEquals(-1, preparedStatement.getUpdateCount());

            try {
                //when returned cursor is null
                PowerMockito.when(mockSqlFutureCursor.checkedGet()).thenReturn(null);
                preparedStatement.executeQuery();
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

        VitessPreparedStatement preparedStatement =
            new VitessPreparedStatement(mockConn, sqlUpdate);
        try {

            //exception on executing dml on non master
            PowerMockito.when(mockConn.getTabletType()).thenReturn(Topodata.TabletType.REPLICA);
            try {
                preparedStatement.executeUpdate();
                Assert.fail("Should have thrown exception for tablet type not being master");
            } catch (SQLException ex) {
                Assert.assertEquals("DML Statement cannot be executed on non master instance type",
                    ex.getMessage());
            }

            //executing dml on master
            PowerMockito.when(mockConn.getTabletType()).thenReturn(Topodata.TabletType.MASTER);
            preparedStatement =
                new VitessPreparedStatement(mockConn, sqlUpdate, ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY);
            int updateCount = preparedStatement.executeUpdate();
            Assert.assertEquals(0, updateCount);

            //tx is null & autoCommit is true
            PowerMockito.when(mockConn.getVtGateTx()).thenReturn(null);
            PowerMockito.when(mockVtGateConn.begin(Matchers.any(Context.class)))
                .thenReturn(mockSqlFutureVtGateTx);
            PowerMockito.when(mockConn.getAutoCommit()).thenReturn(true);
            PowerMockito.when(mockVtGateTx.commit(Matchers.any(Context.class)))
                .thenReturn(mockSqlFutureCursor);
            preparedStatement = new VitessPreparedStatement(mockConn, sqlUpdate);
            updateCount = preparedStatement.executeUpdate();
            Assert.assertEquals(0, updateCount);

            //cursor fields is not null
            PowerMockito.when(mockCursor.getFields()).thenReturn(fieldList);
            PowerMockito.when(fieldList.isEmpty()).thenReturn(false);
            try {
                preparedStatement.executeUpdate();
                Assert.fail("Should have thrown exception for field not null");
            } catch (SQLException ex) {
                Assert.assertEquals("ResultSet generation is not allowed through this method",
                    ex.getMessage());
            }

            //cursor is null
            PowerMockito.when(mockSqlFutureCursor.checkedGet()).thenReturn(null);
            try {
                preparedStatement.executeUpdate();
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
        PowerMockito.when(mockConn.getExecuteType())
            .thenReturn(Constants.QueryExecuteType.SIMPLE);
        PowerMockito.when(mockConn.isSimpleExecute()).thenReturn(true);
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
        PowerMockito.when(mockVtGateTx.commit(Matchers.any(Context.class)))
            .thenReturn(mockSqlFutureCursor);

        PowerMockito.when(mockSqlFutureCursor.checkedGet()).thenReturn(mockCursor);
        PowerMockito.when(mockSqlFutureVtGateTx.checkedGet()).thenReturn(mockVtGateTx);
        PowerMockito.when(mockCursor.getFields()).thenReturn(mockFieldList);

        VitessPreparedStatement preparedStatement =
            new VitessPreparedStatement(mockConn, sqlSelect, ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);
        try {

            int fieldSize = 5;
            PowerMockito.when(mockCursor.getFields()).thenReturn(mockFieldList);
            PowerMockito.doReturn(fieldSize).when(mockFieldList).size();
            PowerMockito.doReturn(false).when(mockFieldList).isEmpty();
            boolean hasResultSet = preparedStatement.execute();
            Assert.assertTrue(hasResultSet);
            Assert.assertNotNull(preparedStatement.getResultSet());

            preparedStatement = new VitessPreparedStatement(mockConn, sqlShow);
            hasResultSet = preparedStatement.execute();
            Assert.assertTrue(hasResultSet);
            Assert.assertNotNull(preparedStatement.getResultSet());

            int mockUpdateCount = 10;
            PowerMockito.when(mockCursor.getFields()).thenReturn(Query.QueryResult.getDefaultInstance().getFieldsList());
            PowerMockito.when(mockCursor.getRowsAffected()).thenReturn((long) mockUpdateCount);
            preparedStatement = new VitessPreparedStatement(mockConn, sqlUpdate);
            hasResultSet = preparedStatement.execute();
            Assert.assertFalse(hasResultSet);
            Assert.assertNull(preparedStatement.getResultSet());
            Assert.assertEquals(mockUpdateCount, preparedStatement.getUpdateCount());

            //cursor is null
            PowerMockito.when(mockSqlFutureCursor.checkedGet()).thenReturn(null);
            try {
                preparedStatement = new VitessPreparedStatement(mockConn, sqlShow);
                preparedStatement.execute();
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

        VitessPreparedStatement preparedStatement =
            new VitessPreparedStatement(mockConn, sqlSelect);
        try {

            PowerMockito.when(mockCursor.getRowsAffected()).thenReturn(10L);
            int updateCount = preparedStatement.executeUpdate();
            Assert.assertEquals(10L, updateCount);
            Assert.assertEquals(10L, preparedStatement.getUpdateCount());

            // Truncated Update Count
            PowerMockito.when(mockCursor.getRowsAffected())
                .thenReturn((long) Integer.MAX_VALUE + 10);
            updateCount = preparedStatement.executeUpdate();
            Assert.assertEquals(Integer.MAX_VALUE, updateCount);
            Assert.assertEquals(Integer.MAX_VALUE, preparedStatement.getUpdateCount());

            preparedStatement.executeQuery();
            Assert.assertEquals(-1, preparedStatement.getUpdateCount());

        } catch (SQLException e) {
            Assert.fail("Test failed " + e.getMessage());
        }
    }

    @Test public void testSetParameters() throws Exception {
        VitessConnection mockConn = PowerMockito.mock(VitessConnection.class);
        VitessPreparedStatement preparedStatement =
            new VitessPreparedStatement(mockConn, sqlSelect);
        Boolean boolValue = Boolean.TRUE;
        Byte byteValue = Byte.MAX_VALUE;
        Short shortValue = Short.MAX_VALUE;
        Integer intValue = Integer.MAX_VALUE;
        Long longValue = Long.MAX_VALUE;
        Float floatValue = Float.MAX_VALUE;
        Double doubleValue = Double.MAX_VALUE;
        BigDecimal bigDecimalValue = new BigDecimal(3.14159265358979323846);
        BigDecimal expectedDecimalValue = new BigDecimal("3.14159");
        String stringValue = "vitess";
        byte[] bytesValue = stringValue.getBytes();
        Date dateValue = new Date(0);
        // Use a time value that won't go negative after adjusting for time zone.
        // The java.sql.Time class does not properly format negative times.
        Time timeValue = new Time(12 * 60 * 60 * 1000);
        Timestamp timestampValue = new Timestamp(0);

        preparedStatement.setNull(1, Types.INTEGER);
        preparedStatement.setBoolean(2, boolValue);
        preparedStatement.setByte(3, byteValue);
        preparedStatement.setShort(4, shortValue);
        preparedStatement.setInt(5, intValue);
        preparedStatement.setLong(6, longValue);
        preparedStatement.setFloat(7, floatValue);
        preparedStatement.setDouble(8, doubleValue);
        preparedStatement.setBigDecimal(9, bigDecimalValue);
        preparedStatement.setString(10, stringValue);
        preparedStatement.setBytes(11, bytesValue);
        preparedStatement.setDate(12, dateValue);
        preparedStatement.setTime(13, timeValue);
        preparedStatement.setTimestamp(14, timestampValue);
        preparedStatement.setDate(15, dateValue, Calendar.getInstance(TimeZone.getDefault()));
        preparedStatement.setTime(16, timeValue, Calendar.getInstance(TimeZone.getDefault()));
        preparedStatement
            .setTimestamp(17, timestampValue, Calendar.getInstance(TimeZone.getDefault()));
        preparedStatement.setObject(18, boolValue);
        preparedStatement.setObject(19, byteValue);
        preparedStatement.setObject(20, shortValue);
        preparedStatement.setObject(21, intValue);
        preparedStatement.setObject(22, longValue);
        preparedStatement.setObject(23, floatValue);
        preparedStatement.setObject(24, doubleValue);
        preparedStatement.setObject(25, bigDecimalValue);
        preparedStatement.setObject(26, stringValue);
        preparedStatement.setObject(27, dateValue);
        preparedStatement.setObject(28, timeValue);
        preparedStatement.setObject(29, timestampValue);
        preparedStatement.setObject(30, 'a');
        preparedStatement.setObject(31, null);
        preparedStatement.setObject(32, boolValue, Types.BOOLEAN, 0);
        preparedStatement.setObject(33, shortValue, Types.SMALLINT, 0);
        preparedStatement.setObject(34, longValue, Types.BIGINT, 0);
        preparedStatement.setObject(35, floatValue, Types.DOUBLE, 2);
        preparedStatement.setObject(36, doubleValue, Types.DOUBLE, 3);
        preparedStatement.setObject(37, bigDecimalValue, Types.DECIMAL, 5);
        preparedStatement.setObject(38, stringValue, Types.VARCHAR, 0);
        preparedStatement.setObject(39, dateValue, Types.DATE, 0);
        preparedStatement.setObject(40, timeValue, Types.TIME, 0);
        preparedStatement.setObject(41, timestampValue, Types.TIMESTAMP, 0);
        try {
            preparedStatement.setObject(42, bytesValue);
            Assert.fail("Shown have thrown exception for not able to set byte[] parameter");
        } catch (SQLException ex) {
            Assert.assertEquals("Cannot infer the SQL type to use for an instance of byte[]",
                ex.getMessage());
        }
        Field bindVariablesMap = preparedStatement.getClass().getDeclaredField("bindVariables");
        bindVariablesMap.setAccessible(true);
        Map<String, Object> bindVariables =
            (Map<String, Object>) bindVariablesMap.get(preparedStatement);
        Assert.assertEquals(null, bindVariables.get("v1"));
        Assert.assertEquals(boolValue, bindVariables.get("v2"));
        Assert.assertEquals(byteValue, bindVariables.get("v3"));
        Assert.assertEquals(shortValue, bindVariables.get("v4"));
        Assert.assertEquals(intValue, bindVariables.get("v5"));
        Assert.assertEquals(longValue, bindVariables.get("v6"));
        Assert.assertEquals(floatValue, bindVariables.get("v7"));
        Assert.assertEquals(doubleValue, bindVariables.get("v8"));
        Assert.assertEquals(bigDecimalValue, bindVariables.get("v9"));
        Assert.assertEquals(stringValue, bindVariables.get("v10"));
        Assert.assertEquals(bytesValue, bindVariables.get("v11"));
        Assert.assertEquals(dateValue.toString(), bindVariables.get("v12"));
        Assert.assertEquals(timeValue.toString(), bindVariables.get("v13"));
        Assert.assertEquals(timestampValue.toString(), bindVariables.get("v14"));
        Assert.assertEquals(dateValue.toString(), bindVariables.get("v15"));
        Assert.assertEquals(timeValue.toString(), bindVariables.get("v16"));
        Assert.assertEquals(timestampValue.toString(), bindVariables.get("v17"));
        Assert.assertEquals(boolValue, bindVariables.get("v18"));
        Assert.assertEquals(byteValue, bindVariables.get("v19"));
        Assert.assertEquals(shortValue, bindVariables.get("v20"));
        Assert.assertEquals(intValue, bindVariables.get("v21"));
        Assert.assertEquals(longValue, bindVariables.get("v22"));
        Assert.assertEquals(floatValue, bindVariables.get("v23"));
        Assert.assertEquals(doubleValue, bindVariables.get("v24"));
        Assert.assertEquals(bigDecimalValue, bindVariables.get("v25"));
        Assert.assertEquals(stringValue, bindVariables.get("v26"));
        Assert.assertEquals(dateValue.toString(), bindVariables.get("v27"));
        Assert.assertEquals(timeValue.toString(), bindVariables.get("v28"));
        Assert.assertEquals(timestampValue.toString(), bindVariables.get("v29"));
        Assert.assertEquals("a", bindVariables.get("v30"));
        Assert.assertEquals(null, bindVariables.get("v31"));
        Assert.assertEquals(boolValue, bindVariables.get("v32"));
        Assert.assertEquals(shortValue.intValue(), bindVariables.get("v33"));
        Assert.assertEquals(longValue, bindVariables.get("v34"));
        Assert.assertEquals((double) floatValue, (double) bindVariables.get("v35"), 0.1);
        Assert.assertEquals(doubleValue, (double) bindVariables.get("v36"), 0.1);
        Assert.assertEquals(expectedDecimalValue, bindVariables.get("v37"));
        Assert.assertEquals(stringValue, bindVariables.get("v38"));
        Assert.assertEquals(dateValue.toString(), bindVariables.get("v39"));
        Assert.assertEquals(timeValue.toString(), bindVariables.get("v40"));
        Assert.assertEquals(timestampValue.toString(), bindVariables.get("v41"));

        preparedStatement.clearParameters();
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

        try {

            long expectedFirstGeneratedId = 121;
            long[] expectedGeneratedIds = {121, 122};
            int expectedAffectedRows = 2;
            PowerMockito.when(mockCursor.getInsertId()).thenReturn(expectedFirstGeneratedId);
            PowerMockito.when(mockCursor.getRowsAffected())
                .thenReturn(Long.valueOf(expectedAffectedRows));

            //Executing Insert Statement
            VitessPreparedStatement preparedStatement =
                new VitessPreparedStatement(mockConn, sqlInsert, Statement.RETURN_GENERATED_KEYS);
            int updateCount = preparedStatement.executeUpdate();
            Assert.assertEquals(expectedAffectedRows, updateCount);

            ResultSet rs = preparedStatement.getGeneratedKeys();
            int i = 0;
            while (rs.next()) {
                long generatedId = rs.getLong(1);
                Assert.assertEquals(expectedGeneratedIds[i++], generatedId);
            }

        } catch (SQLException e) {
            Assert.fail("Test failed " + e.getMessage());
        }
    }

    @Test public void testAddBatch() throws SQLException {
        VitessConnection mockConn = PowerMockito.mock(VitessConnection.class);
        VitessPreparedStatement statement = new VitessPreparedStatement(mockConn, sqlInsert);
        try {
            statement.addBatch(this.sqlInsert);
            Assert.fail("Should have thrown Exception");
        } catch (SQLException ex) {
            Assert.assertEquals(Constants.SQLExceptionMessages.METHOD_NOT_ALLOWED, ex.getMessage());
        }
        statement.setString(1, "string1");
        statement.addBatch();
        try {
            Field privateStringField =
                VitessPreparedStatement.class.getDeclaredField("batchedArgs");
            privateStringField.setAccessible(true);
            Assert.assertEquals("string1",
                (((List<Map<String, Object>>) privateStringField.get(statement)).get(0)).get("v1"));
        } catch (NoSuchFieldException e) {
            Assert.fail("Private Field should exists: batchedArgs");
        } catch (IllegalAccessException e) {
            Assert.fail("Private Field should be accessible: batchedArgs");
        }
    }

    @Test public void testClearBatch() throws SQLException {
        VitessConnection mockConn = PowerMockito.mock(VitessConnection.class);
        VitessPreparedStatement statement = new VitessPreparedStatement(mockConn, sqlInsert);
        statement.setString(1, "string1");
        statement.addBatch();
        statement.clearBatch();
        try {
            Field privateStringField =
                VitessPreparedStatement.class.getDeclaredField("batchedArgs");
            privateStringField.setAccessible(true);
            Assert.assertTrue(
                ((List<Map<String, Object>>) privateStringField.get(statement)).isEmpty());
        } catch (NoSuchFieldException e) {
            Assert.fail("Private Field should exists: batchedArgs");
        } catch (IllegalAccessException e) {
            Assert.fail("Private Field should be accessible: batchedArgs");
        }
    }

    @Test public void testExecuteBatch() throws SQLException {
        VitessConnection mockConn = PowerMockito.mock(VitessConnection.class);
        VitessPreparedStatement statement = new VitessPreparedStatement(mockConn, sqlInsert);
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

        statement.setString(1, "string1");
        statement.addBatch();
        updateCounts = statement.executeBatch();
        Assert.assertEquals(1, updateCounts.length);

        CursorWithError mockCursorWithError2 = PowerMockito.mock(CursorWithError.class);
        Vtrpc.RPCError rpcError = Vtrpc.RPCError.newBuilder().setMessage("preparedStatement execute batch error").build();
        PowerMockito.when(mockCursorWithError2.getError())
            .thenReturn(rpcError);
        mockCursorWithErrorList.add(mockCursorWithError2);
        statement.setString(1, "string1");
        statement.addBatch();
        statement.setString(1, "string2");
        statement.addBatch();
        try {
            statement.executeBatch();
            Assert.fail("Should have thrown Exception");
        } catch (BatchUpdateException ex) {
            Assert.assertEquals(rpcError.toString(), ex.getMessage());
            Assert.assertEquals(2, ex.getUpdateCounts().length);
            Assert.assertEquals(Statement.EXECUTE_FAILED, ex.getUpdateCounts()[1]);
        }
    }

    @Test public void testStatementCount() throws SQLException {
        VitessConnection mockConn = PowerMockito.mock(VitessConnection.class);
        Map<String, Integer> testCases = ImmutableMap.<String, Integer>builder()
            .put("select * from foo where a = ?", 1)
            .put("select * from foo where a = ? and b = ?", 2)
            .put("select * from foo where a = ? and b = \"?\"", 1)
            .put("select * from foo where a = ? and b = '?'", 1)
            .put("select * from foo where a = ? and b = `?`", 1)
            .put("select foo.*, `bar.baz?` from foo, bar where foo.a = ? and bar.b = foo.b", 1)
            .put("select * from foo where a = ? and b = \"`?`\"", 1)
            .put("select * from foo where a = ? --and b = ?", 1)
            .put("select * from foo where a = ? /* and b = ? */ and c = ?", 2)
            .put("/* leading comment? */ select * from foo where a = ? and b = ?", 2)
            .put("select * from foo where a = ? and b = ? and c = 'test' and d = ?", 3)
            .put("select * from foo where a = ? and b = \\`?\\`", 2) // not valid sql but validates escaping
            .put("select * from foo where a = ? and b = \\?", 1) // not valid sql but validates escaping
            .put("update foo set a = ?, b = ? where c = 'test' and d = ?", 3)
            .put("insert into foo (`a`, `b`) values (?, ?), (?, ?) on /* test? */ duplicate key update a = \"?\"", 4)
            .put("delete from foo where a = ? and b = '?'", 1)
            .build();

        for (Map.Entry<String, Integer> testCase : testCases.entrySet()) {
            VitessPreparedStatement statement = new VitessPreparedStatement(mockConn, testCase.getKey());
            Assert.assertEquals(testCase.getKey(), testCase.getValue().longValue(), statement.getParameterMetaData().getParameterCount());
        }
    }
}
