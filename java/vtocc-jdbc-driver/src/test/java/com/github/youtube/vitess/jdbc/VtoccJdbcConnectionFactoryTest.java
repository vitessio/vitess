package com.github.youtube.vitess.jdbc;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;

import com.github.youtube.vitess.jdbc.VtoccModule.VtoccConnectionCreationScope;
import com.google.inject.Key;
import com.google.inject.util.Providers;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.MockitoAnnotations;
import org.mockito.MockitoAnnotations.Mock;

/**
 * Tests {@link VtoccJdbcConnectionFactory}. Verifies that connections returned are correctly
 * handling prepared statements and transactions.
 */
@RunWith(JUnit4.class)
public class VtoccJdbcConnectionFactoryTest {

  @Mock
  AcolyteTransactingConnection acolyteTransactingConnection;
  @Mock
  VtoccConnectionCreationScope vtoccConnectionCreationScopeMock;

  @Before
  public void setUpMocks() throws Exception {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testConnectionCreation() throws Exception {
    VtoccJdbcConnectionFactory vtoccJdbcConnectionFactory =
        new VtoccJdbcConnectionFactory(
            Providers.of(acolyteTransactingConnection),
            vtoccConnectionCreationScopeMock);
    Assert.assertSame(acolyteTransactingConnection,
        vtoccJdbcConnectionFactory.create(null, "spec1"));
    Assert.assertSame(acolyteTransactingConnection,
        vtoccJdbcConnectionFactory.create(null, "spec2"));

    verify(vtoccConnectionCreationScopeMock).seed(any(Key.class), eq("spec1"));
    verify(vtoccConnectionCreationScopeMock).seed(any(Key.class), eq("spec2"));
  }
}
