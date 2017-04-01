package io.vitess.jdbc;

import com.google.common.util.concurrent.Futures;
import io.vitess.client.Context;
import io.vitess.client.SQLFuture;
import io.vitess.client.VTGateTx;
import io.vitess.proto.Query;
import io.vitess.proto.Topodata;
import io.vitess.util.Constants;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Matchers;
import org.powermock.api.mockito.PowerMockito;

/**
 * Created by harshit.gangal on 19/01/16.
 */
public class VitessConnectionTest extends BaseTest {

    @Test public void testVitessConnection() throws SQLException {
        VitessConnection vitessConnection = new VitessConnection(dbURL, new Properties());
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
        PowerMockito.when(mockVtGateTx.commit(Matchers.any(Context.class), Matchers.anyBoolean()))
            .thenReturn(v);
        vitessConnection.commit();
        Assert.assertEquals(null, vitessConnection.getVtGateTx());
    }

    @Test(expected = SQLException.class) public void testCommitForException() throws SQLException {
        VitessConnection vitessConnection = getVitessConnection();
        vitessConnection.setAutoCommit(false);
        VTGateTx mockVtGateTx = PowerMockito.mock(VTGateTx.class);
        vitessConnection.setVtGateTx(mockVtGateTx);
        PowerMockito.when(mockVtGateTx.commit(Matchers.any(Context.class), Matchers.anyBoolean()))
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
        PowerMockito.when(mockVtGateTx.commit(Matchers.any(Context.class), Matchers.anyBoolean()))
            .thenReturn(v);
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
        Assert.assertEquals("keyspace", vitessConnection.getCatalog());
    }

    @Test public void testSetCatalog() throws SQLException {
        VitessConnection vitessConnection = getVitessConnection();
        vitessConnection.setCatalog("myDB");
        Assert.assertEquals("myDB", vitessConnection.getCatalog());
    }

    @Test public void testPropertiesFromJdbcUrl() throws SQLException {
        String url = "jdbc:vitess://locahost:9000/vt_keyspace/keyspace?TABLET_TYPE=replica&includedFields=type_and_name&blobsAreStrings=yes";
        VitessConnection conn = new VitessConnection(url, new Properties());

        // Properties from the url should be passed into the connection properties, and override whatever defaults we've defined
        Assert.assertEquals(Query.ExecuteOptions.IncludedFields.TYPE_AND_NAME, conn.getIncludedFields());
        Assert.assertEquals(false, conn.isIncludeAllFields());
        Assert.assertEquals(Topodata.TabletType.REPLICA, conn.getTabletType());
        Assert.assertEquals(true, conn.getBlobsAreStrings());
    }
}
