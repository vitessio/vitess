package com.flipkart.vitess.jdbc.test;

import com.flipkart.vitess.jdbc.VitessConnection;
import com.flipkart.vitess.jdbc.VitessStatement;
import com.youtube.vitess.client.Context;
import com.youtube.vitess.client.SQLFuture;
import com.youtube.vitess.client.VTGateConn;
import com.youtube.vitess.client.VTGateTx;
import com.youtube.vitess.client.cursor.Cursor;
import com.youtube.vitess.proto.Topodata;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Matchers;
import org.powermock.api.mockito.PowerMockito;

import java.sql.SQLException;

/**
 * Created by harshit.gangal on 19/01/16.
 */
public class VitessStatementTest {

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
        PowerMockito.when(mockCursor.getRowsAffected()).thenReturn((long) Integer.MAX_VALUE + 1);

        VitessStatement statement = new VitessStatement(mockConn);
        try {
            PowerMockito.when(mockCursor.getRowsAffected())
                .thenReturn((long) Integer.MAX_VALUE + 1);
            statement.executeUpdate("select 1");
            Assert.assertEquals(Integer.MAX_VALUE, statement.executeUpdate("select 1"));
            Assert.assertEquals(Integer.MAX_VALUE, statement.getUpdateCount());

            PowerMockito.when(mockCursor.getRowsAffected())
                .thenReturn((long) Integer.MAX_VALUE - 1);
            statement.executeUpdate("select 1");
            Assert.assertEquals(Integer.MAX_VALUE - 1, statement.executeUpdate("select 1"));
            Assert.assertEquals(Integer.MAX_VALUE - 1, statement.getUpdateCount());

            PowerMockito.when(mockCursor.getRowsAffected()).thenReturn(0L);
            statement.executeUpdate("select 1");
            Assert.assertEquals(0, statement.executeUpdate("select 1"));
            Assert.assertEquals(0, statement.getUpdateCount());


        } catch (SQLException e) {
            Assert.fail("Test failed " + e.getMessage());
        }
    }
}
