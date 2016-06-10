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
            new VitessJDBCUrl("jdbc:vitess://user:password@10.33.17.231:15991/shipment/shipment",
                info);
        Assert.assertEquals(1, vitessJDBCUrl.getHostInfos().size());
        Assert.assertEquals("10.33.17.231", vitessJDBCUrl.getHostInfos().get(0).getHostname());
        Assert.assertEquals(15991, vitessJDBCUrl.getHostInfos().get(0).getPort());
        Assert.assertEquals("user", vitessJDBCUrl.getUsername());
    }


    @Test public void testURLwithoutUserNamePwd() throws Exception {
        Properties info = new Properties();
        VitessJDBCUrl vitessJDBCUrl =
            new VitessJDBCUrl("jdbc:vitess://10.33.17.231:15991/shipment/shipment", info);
        Assert.assertEquals(1, vitessJDBCUrl.getHostInfos().size());
        Assert.assertEquals(vitessJDBCUrl.getHostInfos().get(0).getHostname(), "10.33.17.231");
        Assert.assertEquals(15991, vitessJDBCUrl.getHostInfos().get(0).getPort());
        Assert.assertEquals(null, vitessJDBCUrl.getUsername());
    }

    @Test public void testURLwithUserNamePwdinParams() throws Exception {
        Properties info = new Properties();
        VitessJDBCUrl vitessJDBCUrl = new VitessJDBCUrl(
            "jdbc:vitess://10.33.17.231:15991/shipment/shipment?userName=user&password=password",
            info);
        Assert.assertEquals(1, vitessJDBCUrl.getHostInfos().size());
        Assert.assertEquals("10.33.17.231", vitessJDBCUrl.getHostInfos().get(0).getHostname());
        Assert.assertEquals(15991, vitessJDBCUrl.getHostInfos().get(0).getPort());
        Assert.assertEquals("user", vitessJDBCUrl.getUsername());
    }

    @Test public void testURLwithUserNamePwdinProperties() throws Exception {
        Properties info = new Properties();
        info.setProperty("userName", "user");
        info.setProperty("password", "password");
        VitessJDBCUrl vitessJDBCUrl =
            new VitessJDBCUrl("jdbc:vitess://10.33.17.231:15991/shipment/shipment", info);
        Assert.assertEquals(1, vitessJDBCUrl.getHostInfos().size());
        Assert.assertEquals("10.33.17.231", vitessJDBCUrl.getHostInfos().get(0).getHostname());
        Assert.assertEquals(15991, vitessJDBCUrl.getHostInfos().get(0).getPort());
        Assert.assertEquals("user", vitessJDBCUrl.getUsername());
    }

    @Test public void testURLwithUserNamePwdMultipleHost() throws Exception {
        Properties info = new Properties();
        info.setProperty("userName", "user");
        info.setProperty("password", "password");
        VitessJDBCUrl vitessJDBCUrl = new VitessJDBCUrl(
            "jdbc:vitess://10.33.17.231:15991,10.33.17.232:15991,10.33.17"
                + ".233:15991/shipment/shipment?TABLET_TYPE=master", info);
        Assert.assertEquals(3, vitessJDBCUrl.getHostInfos().size());
        Assert.assertEquals("10.33.17.231", vitessJDBCUrl.getHostInfos().get(0).getHostname());
        Assert.assertEquals(15991, vitessJDBCUrl.getHostInfos().get(0).getPort());
        Assert.assertEquals("10.33.17.232", vitessJDBCUrl.getHostInfos().get(1).getHostname());
        Assert.assertEquals(15991, vitessJDBCUrl.getHostInfos().get(1).getPort());
        Assert.assertEquals("10.33.17.233", vitessJDBCUrl.getHostInfos().get(2).getHostname());
        Assert.assertEquals(15991, vitessJDBCUrl.getHostInfos().get(2).getPort());
        Assert.assertEquals(Topodata.TabletType.MASTER, vitessJDBCUrl.getTabletType());
        Assert.assertEquals("user", vitessJDBCUrl.getUsername());
    }

    @Test public void testMulitpleJDBCURlURLwithUserNamePwdMultipleHost() throws Exception {
        Properties info = new Properties();
        info.setProperty("userName", "user");
        info.setProperty("password", "password");
        VitessJDBCUrl vitessJDBCUrl = new VitessJDBCUrl(
            "jdbc:vitess://10.33.17.231:15991:xyz,10.33.17.232:15991:xyz,10.33.17"
                + ".233:15991:xyz/shipment/shipment?TABLET_TYPE=master", info);
        VitessJDBCUrl vitessJDBCUrl1 = new VitessJDBCUrl(
            "jdbc:vitess://11.33.17.231:15001:xyz,11.33.17.232:15001:xyz,11.33.17"
                + ".233:15001:xyz/shipment/shipment?TABLET_TYPE=master", info);
        Assert.assertEquals(3, vitessJDBCUrl.getHostInfos().size());
        Assert.assertEquals(3, vitessJDBCUrl1.getHostInfos().size());
        Assert.assertEquals("10.33.17.231", vitessJDBCUrl.getHostInfos().get(0).getHostname());
        Assert.assertEquals(15991, vitessJDBCUrl.getHostInfos().get(0).getPort());
        Assert.assertEquals("11.33.17.231", vitessJDBCUrl1.getHostInfos().get(0).getHostname());
        Assert.assertEquals(15001, vitessJDBCUrl1.getHostInfos().get(0).getPort());
        Assert.assertEquals("10.33.17.232", vitessJDBCUrl.getHostInfos().get(1).getHostname());
        Assert.assertEquals(15991, vitessJDBCUrl.getHostInfos().get(1).getPort());
        Assert.assertEquals("11.33.17.232", vitessJDBCUrl1.getHostInfos().get(1).getHostname());
        Assert.assertEquals(15001, vitessJDBCUrl1.getHostInfos().get(1).getPort());
        Assert.assertEquals("10.33.17.233", vitessJDBCUrl.getHostInfos().get(2).getHostname());
        Assert.assertEquals(15991, vitessJDBCUrl.getHostInfos().get(2).getPort());
        Assert.assertEquals("11.33.17.233", vitessJDBCUrl1.getHostInfos().get(2).getHostname());
        Assert.assertEquals(15001, vitessJDBCUrl1.getHostInfos().get(2).getPort());
        Assert.assertEquals(vitessJDBCUrl.getTabletType(), Topodata.TabletType.MASTER);
        Assert.assertEquals("user", vitessJDBCUrl.getUsername());
    }

    @Test public void testWithKeyspaceandCatalog() throws Exception {
        Properties info = new Properties();
        System.out.println("I am here");
        VitessJDBCUrl vitessJDBCUrl =
            new VitessJDBCUrl("jdbc:vitess://user:password@10.33.17.231:15991/vt_shipment/shipment",
                info);
        Assert.assertEquals(1, vitessJDBCUrl.getHostInfos().size());
        Assert.assertEquals("vt_shipment",vitessJDBCUrl.getKeyspace());
        Assert.assertEquals("shipment",vitessJDBCUrl.getCatalog());
    }

    @Test public void testWithKeyspace() throws Exception {
        Properties info = new Properties();
        VitessJDBCUrl vitessJDBCUrl =
            new VitessJDBCUrl("jdbc:vitess://user:password@10.33.17.231:15991/vt_shipment",
                info);
        Assert.assertEquals(1, vitessJDBCUrl.getHostInfos().size());
        Assert.assertEquals("vt_shipment",vitessJDBCUrl.getKeyspace());
        Assert.assertEquals("vt_shipment",vitessJDBCUrl.getCatalog());
    }

}
