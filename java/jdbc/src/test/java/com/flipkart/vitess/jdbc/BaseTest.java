package com.flipkart.vitess.jdbc;

import org.junit.Assert;
import org.junit.BeforeClass;

import java.sql.SQLException;
import java.util.Properties;

public class BaseTest {
    String dbURL = "jdbc:vitess://locahost:9000/vt_keyspace/keyspace";

    @BeforeClass
    public static void setUp() {
        try {
            Class.forName("com.flipkart.vitess.jdbc.VitessDriver");
        } catch (ClassNotFoundException e) {
            Assert.fail("Driver is not in the CLASSPATH -> " + e.getMessage());
        }
    }

    protected VitessConnection getVitessConnection() throws SQLException {
        return new VitessConnection(dbURL, new Properties());
    }
}
