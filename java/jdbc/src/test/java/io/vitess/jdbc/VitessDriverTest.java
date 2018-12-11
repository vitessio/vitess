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

import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import io.vitess.util.Constants;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Created by harshit.gangal on 19/01/16.
 */
public class VitessDriverTest {

    private static VitessDriver driver = new VitessDriver();

    String dbURL =
            "jdbc:vitess://localhost:9000/shipment/vt_shipment?tabletType=master&executeType=stream&userName"
                    + "=user";

    @BeforeClass
    public static void setUp() {
        // load Vitess driver
        try {
            Class.forName("io.vitess.jdbc.VitessDriver");
        } catch (ClassNotFoundException e) {
            fail("Driver is not in the CLASSPATH -> " + e);
        }
    }

    @Test
    public void testConnect() throws SQLException {
        VitessConnection connection =
                (VitessConnection) DriverManager.getConnection(dbURL, new Properties());
        assertEquals(connection.getUrl().getUrl(), dbURL);
    }

    @Test
    public void testAcceptsURL() {
        assertEquals(true, driver.acceptsURL(dbURL));
    }

    @Test
    public void testAcceptsMalformedURL() {
        String url =
                "jdbc:MalfromdedUrl://localhost:9000/shipment/vt_shipment?tabletType=master";
        assertEquals(false, driver.acceptsURL(url));
    }

    @Test
    public void testGetPropertyInfo() throws SQLException {
        // Used to ensure that we're properly adding the below URL-based properties at the beginning
        // of the full ConnectionProperties configuration
        DriverPropertyInfo[] underlying = ConnectionProperties.exposeAsDriverPropertyInfo(new Properties(), 0);

        int additionalProp = 2;
        DriverPropertyInfo[] driverPropertyInfos = driver.getPropertyInfo(dbURL, null);
        assertEquals(underlying.length + additionalProp, driverPropertyInfos.length);

        assertEquals(driverPropertyInfos[0].description, Constants.VITESS_HOST);
        assertEquals(driverPropertyInfos[0].required, true);
        assertEquals(driverPropertyInfos[0].name, Constants.Property.HOST);
        assertEquals(driverPropertyInfos[0].value, "localhost");

        assertEquals(driverPropertyInfos[1].description, Constants.VITESS_PORT);
        assertEquals(driverPropertyInfos[1].required, false);
        assertEquals(driverPropertyInfos[1].name, Constants.Property.PORT);
        assertEquals(driverPropertyInfos[1].value, "9000");

        // Validate the remainder of the driver properties match up with the underlying
        for (int i = additionalProp; i < driverPropertyInfos.length; i++) {
            assertEquals(underlying[i - additionalProp].description, driverPropertyInfos[i].description);
            assertEquals(underlying[i - additionalProp].required, driverPropertyInfos[i].required);
            assertEquals(underlying[i - additionalProp].name, driverPropertyInfos[i].name);
            assertEquals(underlying[i - additionalProp].value, driverPropertyInfos[i].value);
        }
    }

    @Test
    public void testGetMajorVersion() {
        assertEquals(driver.getMajorVersion(), Constants.DRIVER_MAJOR_VERSION);
    }

    @Test
    public void testGetMinorVersion() {
        assertEquals(driver.getMinorVersion(), Constants.DRIVER_MINOR_VERSION);
    }

    @Test
    public void testJdbcCompliant() {
        assertEquals(false, driver.jdbcCompliant());
    }

}
