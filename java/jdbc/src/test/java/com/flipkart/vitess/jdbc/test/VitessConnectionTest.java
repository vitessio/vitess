package com.flipkart.vitess.jdbc.test;

import com.flipkart.vitess.jdbc.VitessConnection;
import com.flipkart.vitess.util.Constants;
import com.google.common.util.concurrent.Futures;
import com.youtube.vitess.client.Context;
import com.youtube.vitess.client.SQLFuture;
import com.youtube.vitess.client.VTGateTx;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Matchers;
import org.powermock.api.mockito.PowerMockito;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by harshit.gangal on 19/01/16.
 */
public class VitessConnectionTest {

    String dbURL = "jdbc:vitess://locahost:9000/vt_shipment/shipment";

    @BeforeClass public static void setUp() {
        // load Vitess driver
        try {
            Class.forName("com.flipkart.vitess.jdbc.VitessDriver");
        } catch (ClassNotFoundException e) {
            Assert.fail("Driver is not in the CLASSPATH -> " + e.getMessage());
        }
    }

    private VitessConnection getVitessConnection() throws SQLException {
        return new VitessConnection(dbURL, null);
    }

    @Test public void testVitessConnection() throws SQLException {
        VitessConnection vitessConnection = new VitessConnection(dbURL, null);
        Assert.assertEquals(false, vitessConnection.isClosed());
        Assert.assertNull(vitessConnection.getDbProperties());
    }

    @Test public void testCreateStatement() throws SQLException {
        VitessConnection vitessConnection = getVitessConnection();
        Statement statement = vitessConnection.createStatement();
        Assert.assertEquals(vitessConnection, statement.getConnection());
    }


    @Test(expected = SQLException.class) public void testCreateStatementForClose()
        throws SQLException {
        VitessConnection vitessConnection = getVitessConnection();
        vitessConnection.close();
        try {
            Statement statement = vitessConnection.createStatement();
        } catch (SQLException e) {
            throw new SQLException(Constants.SQLExceptionMessages.CONN_CLOSED);
        }
    }

    @Test public void testnativeSQL() throws SQLException {
        VitessConnection vitessConnection = getVitessConnection();
        Assert.assertEquals("query", vitessConnection.nativeSQL("query"));
    }

    @Test public void testCreatePreperedStatement() throws SQLException {
        VitessConnection vitessConnection = getVitessConnection();
        PreparedStatement preparedStatementstatement = vitessConnection.prepareStatement("query");
        Assert.assertEquals(vitessConnection, preparedStatementstatement.getConnection());
    }


    @Test(expected = SQLException.class) public void testCreatePreperedStatementForClose()
        throws SQLException {
        VitessConnection vitessConnection = getVitessConnection();
        vitessConnection.close();
        try {
            PreparedStatement preparedStatementstatement =
                vitessConnection.prepareStatement("query");
        } catch (SQLException e) {
            throw new SQLException(Constants.SQLExceptionMessages.CONN_CLOSED);
        }
    }

    @Test public void testDefaultGetAutoCommit() throws SQLException {
        VitessConnection vitessConnection = getVitessConnection();
        Assert.assertEquals(true, vitessConnection.getAutoCommit());
    }

    @Test(expected = SQLException.class) public void testDefaultGetAutoCommitForClose()
        throws SQLException {
        VitessConnection vitessConnection = getVitessConnection();
        vitessConnection.close();
        try {
            boolean autoCommit = vitessConnection.getAutoCommit();
        } catch (SQLException e) {
            throw new SQLException(Constants.SQLExceptionMessages.CONN_CLOSED);
        }
    }

    @Test public void testDefaultSetAutoCommit() throws SQLException {
        VitessConnection vitessConnection = getVitessConnection();
        vitessConnection.setAutoCommit(false);
        Assert.assertEquals(false, vitessConnection.getAutoCommit());
    }

    @Test(expected = SQLException.class) public void testDefaultSetAutoCommitForClose()
        throws SQLException {
        VitessConnection vitessConnection = getVitessConnection();
        vitessConnection.close();
        try {
            boolean autoCommit = vitessConnection.getAutoCommit();
        } catch (SQLException e) {
            throw new SQLException(Constants.SQLExceptionMessages.CONN_CLOSED);
        }
    }

    @Test public void testCommit() throws SQLException {
        VitessConnection vitessConnection = getVitessConnection();
        vitessConnection.setAutoCommit(false);
        VTGateTx mockVtGateTx = PowerMockito.mock(VTGateTx.class);
        vitessConnection.setVtGateTx(mockVtGateTx);
        SQLFuture<Void> v = new SQLFuture<>(Futures.<Void>immediateFuture(null));
        PowerMockito.when(mockVtGateTx.commit(Matchers.any(Context.class))).thenReturn(v);
        vitessConnection.commit();
        Assert.assertEquals(null, vitessConnection.getVtGateTx());
    }

    @Test(expected = SQLException.class) public void testCommitForException() throws SQLException {
        VitessConnection vitessConnection = getVitessConnection();
        vitessConnection.setAutoCommit(false);
        VTGateTx mockVtGateTx = PowerMockito.mock(VTGateTx.class);
        vitessConnection.setVtGateTx(mockVtGateTx);
        PowerMockito.when(mockVtGateTx.commit(Matchers.any(Context.class)))
            .thenThrow(new SQLException());
        try {
            vitessConnection.commit();
        } catch (SQLException e) {
            throw new SQLException();
        }
    }

    @Test public void testRollback() throws SQLException {
        VitessConnection vitessConnection = getVitessConnection();
        vitessConnection.setAutoCommit(false);
        VTGateTx mockVtGateTx = PowerMockito.mock(VTGateTx.class);
        vitessConnection.setVtGateTx(mockVtGateTx);
        SQLFuture<Void> v = new SQLFuture<>(Futures.<Void>immediateFuture(null));
        PowerMockito.when(mockVtGateTx.commit(Matchers.any(Context.class))).thenReturn(v);
        vitessConnection.commit();
        Assert.assertEquals(null, vitessConnection.getVtGateTx());
    }

    @Test(expected = SQLException.class) public void testRollbackForException()
        throws SQLException {
        VitessConnection vitessConnection = getVitessConnection();
        vitessConnection.setAutoCommit(false);
        VTGateTx mockVtGateTx = PowerMockito.mock(VTGateTx.class);
        vitessConnection.setVtGateTx(mockVtGateTx);
        PowerMockito.when(mockVtGateTx.rollback(Matchers.any(Context.class)))
            .thenThrow(new SQLException());
        try {
            vitessConnection.rollback();
        } catch (SQLException e) {
            throw new SQLException();
        }
    }

    @Test public void testClosed() throws SQLException {
        VitessConnection vitessConnection = getVitessConnection();
        vitessConnection.setAutoCommit(false);
        VTGateTx mockVtGateTx = PowerMockito.mock(VTGateTx.class);
        vitessConnection.setVtGateTx(mockVtGateTx);
        SQLFuture<Void> v = new SQLFuture<>(Futures.<Void>immediateFuture(null));
        PowerMockito.when(mockVtGateTx.rollback(Matchers.any(Context.class))).thenReturn(v);
        vitessConnection.close();
        Assert.assertEquals(true, vitessConnection.isClosed());
    }

    @Test(expected = SQLException.class) public void testClosedForException() throws SQLException {
        VitessConnection vitessConnection = getVitessConnection();
        vitessConnection.setAutoCommit(false);
        VTGateTx mockVtGateTx = PowerMockito.mock(VTGateTx.class);
        vitessConnection.setVtGateTx(mockVtGateTx);
        PowerMockito.when(mockVtGateTx.rollback(Matchers.any(Context.class)))
            .thenThrow(new SQLException());
        try {
            vitessConnection.rollback();
        } catch (SQLException e) {
            throw new SQLException();
        }
    }

    @Test public void testGetCatalog() throws SQLException {
        VitessConnection vitessConnection = getVitessConnection();
        Assert.assertEquals("shipment", vitessConnection.getCatalog());
    }

    @Test public void testSetCatalog() throws SQLException {
        VitessConnection vitessConnection = getVitessConnection();
        vitessConnection.setCatalog("order");
        Assert.assertEquals("order", vitessConnection.getCatalog());
    }

}
