package com.flipkart.vitess.jdbc.test;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Created by harshit.gangal on 19/01/16.
 */
public class VitessStatementTest {

    @BeforeClass public static void setUp() {
        // load Vitess driver
        try {
            Class.forName("com.flipkart.vitess.jdbc.VitessDriver");
        } catch (ClassNotFoundException e) {
            Assert.fail("Driver is not in the CLASSPATH -> " + e);
        }
    }

    @Test public void testDriver() {

    }
}
