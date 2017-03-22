package com.flipkart.vitess.jdbc;

import com.flipkart.vitess.util.Constants;
import com.flipkart.vitess.util.MysqlDefs;
import com.flipkart.vitess.util.charset.CharsetMapping;
import com.google.common.util.concurrent.Futures;
import com.youtube.vitess.client.Context;
import com.youtube.vitess.client.SQLFuture;
import com.youtube.vitess.client.VTGateTx;
import com.youtube.vitess.proto.Query;
import com.youtube.vitess.proto.Topodata;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Matchers;
import org.powermock.api.mockito.PowerMockito;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

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

    @Test public void testGetEncodingForIndex() throws SQLException {
        VitessConnection conn = getVitessConnection();

        // No default encoding configured, and passing NO_CHARSET_INFO basically says "mysql doesn't know"
        // which means don't try looking it up
        Assert.assertEquals(null, conn.getEncodingForIndex(MysqlDefs.NO_CHARSET_INFO));
        // Similarly, a null index or one landing out of bounds for the charset index should return null
        Assert.assertEquals(null, conn.getEncodingForIndex(Integer.MAX_VALUE));
        Assert.assertEquals(null, conn.getEncodingForIndex(-123));

        // charsetIndex 25 is MYSQL_CHARSET_NAME_greek, which is a charset with multiple names, ISO8859_7 and greek
        // Without an encoding configured in the connection, we should return the first (default) encoding for a charset,
        // in this case ISO8859_7
        Assert.assertEquals("ISO-8859-7", conn.getEncodingForIndex(25));
        conn.setEncoding("greek");
        // With an encoding configured, we should return that because it matches one of the names for the charset
        Assert.assertEquals("greek", conn.getEncodingForIndex(25));

        conn.setEncoding(null);
        Assert.assertEquals("UTF-8", conn.getEncodingForIndex(33));
        Assert.assertEquals("ISO-8859-1", conn.getEncodingForIndex(63));

        conn.setEncoding("NOT_REAL");
        // Same tests as the first one, but testing that when there is a default configured, it falls back to that regardless
        Assert.assertEquals("NOT_REAL", conn.getEncodingForIndex(MysqlDefs.NO_CHARSET_INFO));
        Assert.assertEquals("NOT_REAL", conn.getEncodingForIndex(Integer.MAX_VALUE));
        Assert.assertEquals("NOT_REAL", conn.getEncodingForIndex(-123));
    }

    @Test public void testGetMaxBytesPerChar() throws SQLException {
        VitessConnection conn = getVitessConnection();

        // Default state when no good info is passed in
        Assert.assertEquals(0, conn.getMaxBytesPerChar(MysqlDefs.NO_CHARSET_INFO, null));
        // use passed collation index
        Assert.assertEquals(3, conn.getMaxBytesPerChar(CharsetMapping.MYSQL_COLLATION_INDEX_utf8, null));
        // use first, if both are passed and valid
        Assert.assertEquals(3, conn.getMaxBytesPerChar(CharsetMapping.MYSQL_COLLATION_INDEX_utf8, "UnicodeBig"));
        // use passed default charset
        Assert.assertEquals(2, conn.getMaxBytesPerChar(MysqlDefs.NO_CHARSET_INFO, "UnicodeBig"));
    }
}
