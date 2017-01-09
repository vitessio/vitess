package com.flipkart.vitess.jdbc.test;

import com.flipkart.vitess.jdbc.VitessConnection;
import com.flipkart.vitess.jdbc.VitessDriver;
import com.flipkart.vitess.util.Constants;
import com.youtube.vitess.proto.Topodata;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;

/**
 * Created by harshit.gangal on 19/01/16.
 */
public class VitessDriverTest {

    private static VitessDriver driver = new VitessDriver();

    String dbURL =
        "jdbc:vitess://localhost:9000/shipment/vt_shipment?tabletType=master&executeType=stream&userName"
            + "=user";

    @BeforeClass public static void setUp() {
        // load Vitess driver
        try {
            Class.forName("com.flipkart.vitess.jdbc.VitessDriver");
        } catch (ClassNotFoundException e) {
            Assert.fail("Driver is not in the CLASSPATH -> " + e);
        }
    }

    @Test public void testConnect() {
        try {
            VitessConnection connection =
                (VitessConnection) DriverManager.getConnection(dbURL, null);
            Assert.assertEquals(connection.getUrl(), dbURL);
        } catch (SQLException e) {
            Assert.fail("SQLException Not Expected");
        }
    }

    @Test public void testAcceptsURL() {
        try {
            Assert.assertEquals(true, driver.acceptsURL(dbURL));
        } catch (SQLException e) {
            Assert.fail("SQLException Not Expected");
        }
    }

    @Test public void testAcceptsMalformedURL() {
        try {
            String url =
                "jdbc:MalfromdedUrl://localhost:9000/shipment/vt_shipment?tabletType=master";
            Assert.assertEquals(false, driver.acceptsURL(url));
        } catch (SQLException e) {
            Assert.fail("SQLException Not Expected");
        }
    }

    @Test public void testGetPropertyInfo() throws SQLException {
        DriverPropertyInfo[] driverPropertyInfos = driver.getPropertyInfo(dbURL, null);
        Assert.assertEquals(driverPropertyInfos[0].description, Constants.VITESS_HOST);
        Assert.assertEquals(driverPropertyInfos[0].required, true);
        Assert.assertEquals(driverPropertyInfos[0].name, Constants.Property.HOST);
        Assert.assertEquals(driverPropertyInfos[0].value, "localhost");

        Assert.assertEquals(driverPropertyInfos[1].description, Constants.VITESS_PORT);
        Assert.assertEquals(driverPropertyInfos[1].required, false);
        Assert.assertEquals(driverPropertyInfos[1].name, Constants.Property.PORT);
        Assert.assertEquals(driverPropertyInfos[1].value, "9000");

        Assert.assertEquals(driverPropertyInfos[2].description, Constants.VITESS_KEYSPACE);
        Assert.assertEquals(driverPropertyInfos[2].required, true);
        Assert.assertEquals(driverPropertyInfos[2].name, Constants.Property.KEYSPACE);
        Assert.assertEquals(driverPropertyInfos[2].value, "shipment");

        Assert.assertEquals(driverPropertyInfos[3].description, Constants.VITESS_TABLET_TYPE);
        Assert.assertEquals(driverPropertyInfos[3].required, false);
        Assert.assertEquals(driverPropertyInfos[3].name, Constants.Property.TABLET_TYPE);
        Assert.assertEquals(driverPropertyInfos[3].value, Topodata.TabletType.MASTER.toString());

        Assert.assertEquals(driverPropertyInfos[4].description, Constants.EXECUTE_TYPE_DESC);
        Assert.assertEquals(driverPropertyInfos[4].required, false);
        Assert.assertEquals(driverPropertyInfos[4].name, Constants.Property.EXECUTE_TYPE);
        Assert.assertEquals(driverPropertyInfos[4].value,
            Constants.QueryExecuteType.STREAM.toString());

        Assert.assertEquals(driverPropertyInfos[5].description, Constants.VITESS_DB_NAME);
        Assert.assertEquals(driverPropertyInfos[5].required, true);
        Assert.assertEquals(driverPropertyInfos[5].name, Constants.Property.DBNAME);
        Assert.assertEquals(driverPropertyInfos[5].value, "vt_shipment");

        Assert.assertEquals(driverPropertyInfos[6].description, Constants.USERNAME_DESC);
        Assert.assertEquals(driverPropertyInfos[6].required, false);
        Assert.assertEquals(driverPropertyInfos[6].name, Constants.Property.USERNAME);
        Assert.assertEquals(driverPropertyInfos[6].value, "user");


    }

    @Test public void testGetMajorVersion() {
        Assert.assertEquals(driver.getMajorVersion(), Constants.DRIVER_MAJOR_VERSION);
    }

    @Test public void testGetMinorVersion() {
        Assert.assertEquals(driver.getMinorVersion(), Constants.DRIVER_MINOR_VERSION);
    }

    @Test public void testJdbcCompliant() {
        Assert.assertEquals(false, driver.jdbcCompliant());
    }

}
