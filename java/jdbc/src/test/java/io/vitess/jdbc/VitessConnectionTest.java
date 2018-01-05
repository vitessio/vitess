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

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.modules.junit4.PowerMockRunner;

import io.vitess.client.VTSession;
import io.vitess.proto.Query;
import io.vitess.proto.Topodata;
import io.vitess.util.Constants;

/**
 * Created by harshit.gangal on 19/01/16.
 */
@RunWith(PowerMockRunner.class)
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
        VTSession mockSession = PowerMockito.mock(VTSession.class);
        VitessConnection vitessConnection = getVitessConnection();
        try {
            Field privateVTSessionField = VitessConnection.class.getDeclaredField("vtSession");
            privateVTSessionField.setAccessible(true);
            privateVTSessionField.set(vitessConnection, mockSession);
            PowerMockito.when(mockSession.isInTransaction()).thenReturn(false);
            PowerMockito.when(mockSession.isAutoCommit()).thenReturn(false);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            Assert.fail(e.getMessage());
        }
        vitessConnection.commit();
    }

    @Test(expected = SQLException.class) public void testCommitForException() throws SQLException {
        VitessConnection vitessConnection = getVitessConnection();
        vitessConnection.setAutoCommit(true);
        vitessConnection.commit();
    }

    @Test public void testRollback() throws SQLException {
        VitessConnection vitessConnection = getVitessConnection();
        vitessConnection.setAutoCommit(false);
        vitessConnection.rollback();
    }

    @Test(expected = SQLException.class) public void testRollbackForException()
        throws SQLException {
        VitessConnection vitessConnection = getVitessConnection();
        vitessConnection.setAutoCommit(true);
        vitessConnection.rollback();
    }

    @Test public void testClosed() throws SQLException {
        VitessConnection vitessConnection = getVitessConnection();
        vitessConnection.setAutoCommit(false);
        vitessConnection.close();
        Assert.assertEquals(true, vitessConnection.isClosed());
    }

    @Test(expected = SQLException.class) public void testClosedForException() throws SQLException {
        VTSession mockSession = PowerMockito.mock(VTSession.class);
        VitessConnection vitessConnection = getVitessConnection();
        try {
            Field privateVTSessionField = VitessConnection.class.getDeclaredField("vtSession");
            privateVTSessionField.setAccessible(true);
            privateVTSessionField.set(vitessConnection, mockSession);
            //vtSession.setSession(mockSession.getSession());
            PowerMockito.when(mockSession.isInTransaction()).thenReturn(true);
            PowerMockito.when(mockSession.isAutoCommit()).thenReturn(true);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            Assert.fail(e.getMessage());
        }
        vitessConnection.close();
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

    @Test public void testClientFoundRows() throws SQLException {
        String url = "jdbc:vitess://locahost:9000/vt_keyspace/keyspace?TABLET_TYPE=replica&useAffectedRows=true";
        VitessConnection conn = new VitessConnection(url, new Properties());

        Assert.assertEquals(true, conn.getUseAffectedRows());
        Assert.assertEquals(false, conn.getVtSession().getSession().getOptions().getClientFoundRows());

        url = "jdbc:vitess://locahost:9000/vt_keyspace/keyspace?TABLET_TYPE=replica&useAffectedRows=false";
        conn = new VitessConnection(url, new Properties());

        Assert.assertEquals(false, conn.getUseAffectedRows());
        Assert.assertEquals(true, conn.getVtSession().getSession().getOptions().getClientFoundRows());
    }

    @Test public void testWorkload() throws SQLException {
        for (Query.ExecuteOptions.Workload workload : Query.ExecuteOptions.Workload.values()) {
            if (workload == Query.ExecuteOptions.Workload.UNRECOGNIZED) {
                continue;
            }
            String url = "jdbc:vitess://locahost:9000/vt_keyspace/keyspace?TABLET_TYPE=replica&workload=" + workload.toString().toLowerCase();
            VitessConnection conn = new VitessConnection(url, new Properties());
            
            Assert.assertEquals(workload, conn.getWorkload());
            Assert.assertEquals(workload, conn.getVtSession().getSession().getOptions().getWorkload());
        }
    }

    @Test public void testTransactionIsolation() throws SQLException {
        VitessConnection conn = Mockito.spy(getVitessConnection());
        Mockito.doReturn(new DBProperties("random", "random", "random", Connection.TRANSACTION_REPEATABLE_READ, "random"))
            .when(conn)
            .getDbProperties();
        Mockito.doReturn(new VitessMySQLDatabaseMetadata(conn)).when(conn).getMetaData();

        Assert.assertEquals(Query.ExecuteOptions.TransactionIsolation.DEFAULT, conn.getVtSession().getSession().getOptions().getTransactionIsolation());
        Assert.assertEquals(Connection.TRANSACTION_REPEATABLE_READ, conn.getTransactionIsolation());

        conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

        Assert.assertEquals(Query.ExecuteOptions.TransactionIsolation.READ_COMMITTED, conn.getVtSession().getSession().getOptions().getTransactionIsolation());
        Assert.assertEquals(Connection.TRANSACTION_READ_COMMITTED, conn.getTransactionIsolation());

        VitessStatement statement = Mockito.mock(VitessStatement.class);
        Mockito.when(conn.createStatement()).thenReturn(statement);
        Mockito.when(conn.isInTransaction()).thenReturn(true);
        conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);

        Mockito.verify(statement).executeUpdate("rollback");
        Assert.assertEquals(Query.ExecuteOptions.TransactionIsolation.READ_UNCOMMITTED, conn.getVtSession().getSession().getOptions().getTransactionIsolation());
        Assert.assertEquals(Connection.TRANSACTION_READ_UNCOMMITTED, conn.getTransactionIsolation());
    }
}
