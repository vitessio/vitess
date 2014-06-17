package com.github.youtube.vitess.jdbc.bson;

import static org.junit.Assert.assertNotNull;

import com.google.inject.Guice;
import com.google.protobuf.Message;

import com.github.youtube.vitess.jdbc.bson.VtoccBsonBlockingRpcChannel.Factory;
import com.github.youtube.vitess.jdbc.vtocc.QueryService.SessionParams;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Test for {@link com.github.youtube.vitess.jdbc.bson.VtoccBsonBlockingRpcChannel}.
 */

@RunWith(JUnit4.class)
public class VtoccBsonBlockingRpcChannelTest {

  /**
   * Manual test.
   *
   * Requires {@code vtoccServerSpec} system property and a running vtocc server. Must be manually
   * enabled and run.
   *
   * Example: {@code -DvtoccServerSpec=localhost:1610}
   */
  @Test
  @Ignore
  public void testWithParentServer() throws Exception {
    Factory channelFactory = Guice.createInjector().getInstance(Factory.class);

    VtoccBsonBlockingRpcChannel channel =
        channelFactory.create(System.getProperty("vtoccServerSpec"));

    Message response = channel.callBlockingMethod("GetSessionId",
        SessionParams.newBuilder().setKeyspace("test_keyspace").setShard("0").build());

    assertNotNull(response);
  }

}
