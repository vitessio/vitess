package com.flipkart.vitess.jdbc.test;

import com.flipkart.vitess.jdbc.VitessDriver;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

/**
 * Created by harshit.gangal on 19/01/16.
 */
public class VitessDriverTest {

    private static VitessDriver driver = new VitessDriver();

    String dbURL = "jdbc:vitess://locahost:9000/shipment/vt_shipment";
    String uName = "vt";
    String pass = "";

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
            DriverManager.getConnection(dbURL, uName, pass);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test public void testAcceptsURL() {
        try {
            Assert.assertEquals(true, driver.acceptsURL(dbURL));
        } catch (SQLException e) {
            Assert.fail("SQLException Not Expected");
        }
    }

    @Test public void testGetPropertyInfo() {

    }

    @Test public void testGetMajorVersion() {

    }

    @Test public void testGetMinorVersion() {

    }

    @Test public void testJdbcCompliant() {
        Assert.assertEquals(false, driver.jdbcCompliant());
    }

    @Test public void testGetParentLogger() {
        try {
            driver.getParentLogger();
            Assert.fail("should have thrown SQLFeatureNotSupportedException");
        } catch (final SQLFeatureNotSupportedException expected) {

        }
    }
}
