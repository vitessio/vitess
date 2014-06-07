package com.github.youtube.vitess.jdbc;

import junit.framework.Assert;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests {@link VtoccModule}.
 */
@RunWith(JUnit4.class)
public class VtoccModuleTest {

  @Test
  public void testVtoccJdbcConnectionFactoryCreation() throws Exception {
    Assert.assertNotNull(VtoccJdbcConnectionFactory.getInstance());
  }
}
