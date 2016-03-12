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
        Assert.assertEquals(vitessJDBCUrl.getHostInfos().size(), 1);
        Assert.assertEquals(vitessJDBCUrl.getHostInfos().get(0).getHostname(), "10.33.17.231");
        Assert.assertEquals(vitessJDBCUrl.getHostInfos().get(0).getPort(), 15991);
        Assert.assertEquals(vitessJDBCUrl.getUsername(), "user");
    }


    @Test public void testURLwithoutUserNamePwd() throws Exception {
        Properties info = new Properties();
        VitessJDBCUrl vitessJDBCUrl =
            new VitessJDBCUrl("jdbc:vitess://10.33.17.231:15991/shipment/shipment", info);
        Assert.assertEquals(vitessJDBCUrl.getHostInfos().size(), 1);
        Assert.assertEquals(vitessJDBCUrl.getHostInfos().get(0).getHostname(), "10.33.17.231");
        Assert.assertEquals(vitessJDBCUrl.getHostInfos().get(0).getPort(), 15991);
        Assert.assertEquals(vitessJDBCUrl.getUsername(), null);
    }

    @Test public void testURLwithUserNamePwdinParams() throws Exception {
        Properties info = new Properties();
        VitessJDBCUrl vitessJDBCUrl = new VitessJDBCUrl(
            "jdbc:vitess://10.33.17.231:15991/shipment/shipment?username=user&password=password",
            info);
        Assert.assertEquals(vitessJDBCUrl.getHostInfos().size(), 1);
        Assert.assertEquals(vitessJDBCUrl.getHostInfos().get(0).getHostname(), "10.33.17.231");
        Assert.assertEquals(vitessJDBCUrl.getHostInfos().get(0).getPort(), 15991);
        Assert.assertEquals(vitessJDBCUrl.getUsername(), "user");
    }

    @Test public void testURLwithUserNamePwdinProperties() throws Exception {
        Properties info = new Properties();
        info.setProperty("username", "user");
        info.setProperty("password", "password");
        VitessJDBCUrl vitessJDBCUrl =
            new VitessJDBCUrl("jdbc:vitess://10.33.17.231:15991/shipment/shipment", info);
        Assert.assertEquals(vitessJDBCUrl.getHostInfos().size(), 1);
        Assert.assertEquals(vitessJDBCUrl.getHostInfos().get(0).getHostname(), "10.33.17.231");
        Assert.assertEquals(vitessJDBCUrl.getHostInfos().get(0).getPort(), 15991);
        Assert.assertEquals(vitessJDBCUrl.getUsername(), "user");
    }

    @Test public void testURLwithUserNamePwdMultipleHost() throws Exception {
        Properties info = new Properties();
        info.setProperty("username", "user");
        info.setProperty("password", "password");
        VitessJDBCUrl vitessJDBCUrl = new VitessJDBCUrl(
            "jdbc:vitess://10.33.17.231:15991,10.33.17.232:15991,10.33.17.233:15991/shipment/shipment?tabletType=master",
            info);
        Assert.assertEquals(vitessJDBCUrl.getHostInfos().size(), 3);
        Assert.assertEquals(vitessJDBCUrl.getHostInfos().get(0).getHostname(), "10.33.17.231");
        Assert.assertEquals(vitessJDBCUrl.getHostInfos().get(0).getPort(), 15991);
        Assert.assertEquals(vitessJDBCUrl.getHostInfos().get(1).getHostname(), "10.33.17.232");
        Assert.assertEquals(vitessJDBCUrl.getHostInfos().get(1).getPort(), 15991);
        Assert.assertEquals(vitessJDBCUrl.getHostInfos().get(2).getHostname(), "10.33.17.233");
        Assert.assertEquals(vitessJDBCUrl.getHostInfos().get(2).getPort(), 15991);
        Assert.assertEquals(vitessJDBCUrl.getTabletType(), Topodata.TabletType.MASTER);
        Assert.assertEquals(vitessJDBCUrl.getUsername(), "user");
    }

    @Test public void testMulitpleJDBCURlURLwithUserNamePwdMultipleHost() throws Exception {
        Properties info = new Properties();
        info.setProperty("username", "user");
        info.setProperty("password", "password");
        VitessJDBCUrl vitessJDBCUrl = new VitessJDBCUrl(
            "jdbc:vitess://10.33.17.231:15991:xyz,10.33.17.232:15991:xyz,10.33.17.233:15991:xyz/shipment/shipment?tabletType=master",
            info);
        VitessJDBCUrl vitessJDBCUrl1 = new VitessJDBCUrl(
            "jdbc:vitess://11.33.17.231:15001:xyz,11.33.17.232:15001:xyz,11.33.17.233:15001:xyz/shipment/shipment?tabletType=master",
            info);
        Assert.assertEquals(vitessJDBCUrl.getHostInfos().size(), 3);
        Assert.assertEquals(vitessJDBCUrl1.getHostInfos().size(), 3);
        Assert.assertEquals(vitessJDBCUrl.getHostInfos().get(0).getHostname(), "10.33.17.231");
        Assert.assertEquals(vitessJDBCUrl.getHostInfos().get(0).getPort(), 15991);
        Assert.assertEquals(vitessJDBCUrl1.getHostInfos().get(0).getHostname(), "11.33.17.231");
        Assert.assertEquals(vitessJDBCUrl1.getHostInfos().get(0).getPort(), 15001);
        Assert.assertEquals(vitessJDBCUrl.getHostInfos().get(1).getHostname(), "10.33.17.232");
        Assert.assertEquals(vitessJDBCUrl.getHostInfos().get(1).getPort(), 15991);
        Assert.assertEquals(vitessJDBCUrl1.getHostInfos().get(1).getHostname(), "11.33.17.232");
        Assert.assertEquals(vitessJDBCUrl1.getHostInfos().get(1).getPort(), 15001);
        Assert.assertEquals(vitessJDBCUrl.getHostInfos().get(2).getHostname(), "10.33.17.233");
        Assert.assertEquals(vitessJDBCUrl.getHostInfos().get(2).getPort(), 15991);
        Assert.assertEquals(vitessJDBCUrl1.getHostInfos().get(2).getHostname(), "11.33.17.233");
        Assert.assertEquals(vitessJDBCUrl1.getHostInfos().get(2).getPort(), 15001);
        Assert.assertEquals(vitessJDBCUrl.getTabletType(), Topodata.TabletType.MASTER);
        Assert.assertEquals(vitessJDBCUrl.getUsername(), "user");
    }


}
