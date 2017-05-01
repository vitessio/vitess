package io.vitess.jdbc;

import java.sql.SQLException;
import java.util.Properties;
import org.junit.Assert;
import org.junit.BeforeClass;

public class BaseTest {
    String dbURL = "jdbc:vitess://locahost:9000/vt_keyspace/keyspace";

    @BeforeClass
    public static void setUp() {
        try {
            Class.forName("io.vitess.jdbc.VitessDriver");
        } catch (ClassNotFoundException e) {
            Assert.fail("Driver is not in the CLASSPATH -> " + e.getMessage());
        }
    }

    protected VitessConnection getVitessConnection() throws SQLException {
        return new VitessConnection(dbURL, new Properties());
    }

    protected VitessStatement getVitessStatement() throws SQLException {
        return new VitessStatement(getVitessConnection());
    }
}
