package com.flipkart.vitess.jdbc.test;

import com.flipkart.vitess.jdbc.VitessJDBCUrl;
import com.youtube.vitess.proto.Topodata;
import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;

/**
 * Created by naveen.nahata on 18/02/16.
 */
public class VitessJDBCUrlTest {

    @Test public void testURLwithUserNamePwd() throws Exception {
        Properties info = new Properties();
        VitessJDBCUrl vitessJDBCUrl =
            new VitessJDBCUrl("jdbc:vitess://user:password@hostname:15991/keyspace/catalog", info);
        Assert.assertEquals(1, vitessJDBCUrl.getHostInfos().size());
        Assert.assertEquals("hostname", vitessJDBCUrl.getHostInfos().get(0).getHostname());
        Assert.assertEquals(15991, vitessJDBCUrl.getHostInfos().get(0).getPort());
        Assert.assertEquals("user", vitessJDBCUrl.getUsername());
    }


    @Test public void testURLwithoutUserNamePwd() throws Exception {
        Properties info = new Properties();
        VitessJDBCUrl vitessJDBCUrl =
            new VitessJDBCUrl("jdbc:vitess://hostname:15991/keyspace/catalog", info);
        Assert.assertEquals(1, vitessJDBCUrl.getHostInfos().size());
        Assert.assertEquals(vitessJDBCUrl.getHostInfos().get(0).getHostname(), "hostname");
        Assert.assertEquals(15991, vitessJDBCUrl.getHostInfos().get(0).getPort());
        Assert.assertEquals(null, vitessJDBCUrl.getUsername());
    }

    @Test public void testURLwithUserNamePwdinParams() throws Exception {
        Properties info = new Properties();
        VitessJDBCUrl vitessJDBCUrl = new VitessJDBCUrl(
            "jdbc:vitess://hostname:15991/keyspace/catalog?userName=user&password=password", info);
        Assert.assertEquals(1, vitessJDBCUrl.getHostInfos().size());
        Assert.assertEquals("hostname", vitessJDBCUrl.getHostInfos().get(0).getHostname());
        Assert.assertEquals(15991, vitessJDBCUrl.getHostInfos().get(0).getPort());
        Assert.assertEquals("user", vitessJDBCUrl.getUsername());
    }

    @Test public void testURLwithUserNamePwdinProperties() throws Exception {
        Properties info = new Properties();
        info.setProperty("userName", "user");
        info.setProperty("password", "password");
        VitessJDBCUrl vitessJDBCUrl =
            new VitessJDBCUrl("jdbc:vitess://hostname:15991/keyspace/catalog", info);
        Assert.assertEquals(1, vitessJDBCUrl.getHostInfos().size());
        Assert.assertEquals("hostname", vitessJDBCUrl.getHostInfos().get(0).getHostname());
        Assert.assertEquals(15991, vitessJDBCUrl.getHostInfos().get(0).getPort());
        Assert.assertEquals("user", vitessJDBCUrl.getUsername());
    }

    @Test public void testURLwithUserNamePwdMultipleHost() throws Exception {
        Properties info = new Properties();
        info.setProperty("userName", "user");
        info.setProperty("password", "password");
        VitessJDBCUrl vitessJDBCUrl = new VitessJDBCUrl(
            "jdbc:vitess://hostname1:15991,hostname2:15991,"
                + "hostname3:15991/keyspace/catalog?TABLET_TYPE=master", info);
        Assert.assertEquals(3, vitessJDBCUrl.getHostInfos().size());
        Assert.assertEquals("hostname1", vitessJDBCUrl.getHostInfos().get(0).getHostname());
        Assert.assertEquals(15991, vitessJDBCUrl.getHostInfos().get(0).getPort());
        Assert.assertEquals("hostname2", vitessJDBCUrl.getHostInfos().get(1).getHostname());
        Assert.assertEquals(15991, vitessJDBCUrl.getHostInfos().get(1).getPort());
        Assert.assertEquals("hostname3", vitessJDBCUrl.getHostInfos().get(2).getHostname());
        Assert.assertEquals(15991, vitessJDBCUrl.getHostInfos().get(2).getPort());
        Assert.assertEquals(Topodata.TabletType.MASTER, vitessJDBCUrl.getTabletType());
        Assert.assertEquals("user", vitessJDBCUrl.getUsername());
    }

    @Test public void testMulitpleJDBCURlURLwithUserNamePwdMultipleHost() throws Exception {
        Properties info = new Properties();
        info.setProperty("userName", "user");
        info.setProperty("password", "password");
        VitessJDBCUrl vitessJDBCUrl = new VitessJDBCUrl(
            "jdbc:vitess://hostname1:15991,hostname2:15991,hostname3"
                + ":15991/keyspace/catalog?TABLET_TYPE=master", info);
        VitessJDBCUrl vitessJDBCUrl1 = new VitessJDBCUrl(
            "jdbc:vitess://hostname1:15001,hostname2:15001,hostname3"
                + ":15001/keyspace/catalog?TABLET_TYPE=master", info);
        Assert.assertEquals(3, vitessJDBCUrl.getHostInfos().size());
        Assert.assertEquals(3, vitessJDBCUrl1.getHostInfos().size());
        Assert.assertEquals("hostname1", vitessJDBCUrl.getHostInfos().get(0).getHostname());
        Assert.assertEquals(15991, vitessJDBCUrl.getHostInfos().get(0).getPort());
        Assert.assertEquals("hostname1", vitessJDBCUrl1.getHostInfos().get(0).getHostname());
        Assert.assertEquals(15001, vitessJDBCUrl1.getHostInfos().get(0).getPort());
        Assert.assertEquals("hostname2", vitessJDBCUrl.getHostInfos().get(1).getHostname());
        Assert.assertEquals(15991, vitessJDBCUrl.getHostInfos().get(1).getPort());
        Assert.assertEquals("hostname2", vitessJDBCUrl1.getHostInfos().get(1).getHostname());
        Assert.assertEquals(15001, vitessJDBCUrl1.getHostInfos().get(1).getPort());
        Assert.assertEquals("hostname3", vitessJDBCUrl.getHostInfos().get(2).getHostname());
        Assert.assertEquals(15991, vitessJDBCUrl.getHostInfos().get(2).getPort());
        Assert.assertEquals("hostname3", vitessJDBCUrl1.getHostInfos().get(2).getHostname());
        Assert.assertEquals(15001, vitessJDBCUrl1.getHostInfos().get(2).getPort());
        Assert.assertEquals(vitessJDBCUrl.getTabletType(), Topodata.TabletType.MASTER);
        Assert.assertEquals("user", vitessJDBCUrl.getUsername());
    }

    @Test public void testWithKeyspaceandCatalog() throws Exception {
        Properties info = new Properties();
        VitessJDBCUrl vitessJDBCUrl =
            new VitessJDBCUrl("jdbc:vitess://user:password@hostname:port/keyspace/catalog", info);
        Assert.assertEquals(1, vitessJDBCUrl.getHostInfos().size());
        Assert.assertEquals("keyspace", vitessJDBCUrl.getKeyspace());
        Assert.assertEquals("catalog", vitessJDBCUrl.getCatalog());
    }

    @Test public void testWithKeyspace() throws Exception {
        Properties info = new Properties();
        VitessJDBCUrl vitessJDBCUrl =
            new VitessJDBCUrl("jdbc:vitess://user:password@hostname:15991/keyspace", info);
        Assert.assertEquals(1, vitessJDBCUrl.getHostInfos().size());
        Assert.assertEquals("keyspace", vitessJDBCUrl.getKeyspace());
        Assert.assertEquals("keyspace", vitessJDBCUrl.getCatalog());
    }

    @Test public void testWithoutKeyspace() throws Exception {
        Properties info = new Properties();
        VitessJDBCUrl vitessJDBCUrl =
            new VitessJDBCUrl("jdbc:vitess://user:password@hostname:15991", info);
        Assert.assertEquals(1, vitessJDBCUrl.getHostInfos().size());
        Assert.assertEquals(null, vitessJDBCUrl.getKeyspace());
        Assert.assertEquals(null, vitessJDBCUrl.getCatalog());

        vitessJDBCUrl = new VitessJDBCUrl("jdbc:vitess://user:password@hostname:15991/", info);
        Assert.assertEquals(1, vitessJDBCUrl.getHostInfos().size());
        Assert.assertEquals(null, vitessJDBCUrl.getKeyspace());
        Assert.assertEquals(null, vitessJDBCUrl.getCatalog());
    }

    @Test public void testCompleteURL() throws Exception {
        Properties info = new Properties();
        VitessJDBCUrl vitessJDBCUrl = new VitessJDBCUrl(
            "jdbc:vitess://user:pass@hostname1:15991,hostname2:15991/keyspace/catalog?prop1=val1&prop2=val2",
            info);
        Assert.assertEquals("user", vitessJDBCUrl.getUsername());
        Assert.assertEquals("hostname1", vitessJDBCUrl.getHostInfos().get(0).getHostname());
        Assert.assertEquals(15991, vitessJDBCUrl.getHostInfos().get(0).getPort());
        Assert.assertEquals("hostname2", vitessJDBCUrl.getHostInfos().get(1).getHostname());
        Assert.assertEquals(15991, vitessJDBCUrl.getHostInfos().get(1).getPort());
        Assert.assertEquals("keyspace", vitessJDBCUrl.getKeyspace());
        Assert.assertEquals("catalog", vitessJDBCUrl.getCatalog());
        Assert.assertEquals("val1", info.getProperty("prop1"));
        Assert.assertEquals("val2", info.getProperty("prop2"));
    }
}
