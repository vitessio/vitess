package com.youtube.vitess.gorpc;

import com.youtube.vitess.gorpc.Exceptions.ApplicationException;
import com.youtube.vitess.gorpc.Exceptions.GoRpcException;
import com.youtube.vitess.gorpc.codecs.bson.BsonClientCodecFactory;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class ClientTest {

  @Before
  public void setUp() throws IOException, InterruptedException {
    Util.startFakeGoServer();
  }

  @After
  public void tearDown() throws IOException {
    Util.stopFakeGoServer();
  }

  @Test
  public void testValidCase() throws IOException, GoRpcException, ApplicationException {
    Client client = Client.dialHttp(Util.HOST, Util.PORT, Util.PATH, new BsonClientCodecFactory());
    BSONObject mArgs = new BasicBSONObject();
    mArgs.put("A", 5L);
    mArgs.put("B", 7L);
    Response response = client.call("Arith.Multiply", mArgs);
    Assert.assertNull(response.getError());
    Assert.assertEquals(35L, response.getReply());
    client.close();
  }

  @Test
  public void testInValidHandshake() throws IOException, GoRpcException {
    try {
      Client client =
          Client.dialHttp(Util.HOST, Util.PORT, "/_somerpc_", new BsonClientCodecFactory());
      client.close();
      Assert.fail("did not raise exception");
    } catch (GoRpcException e) {
      Assert.assertTrue("unexpected response for handshake HTTP/1.0 404 Not Found".equals(e
          .getMessage()));
    }
  }

  @Test
  public void testInValidMethodName() throws IOException, GoRpcException {
    Client client = Client.dialHttp(Util.HOST, Util.PORT, Util.PATH, new BsonClientCodecFactory());
    try {
      client.call("Arith.SomeMethod", new BasicBSONObject());
      client.close();
      Assert.fail("did not raise exception");
    } catch (ApplicationException e) {
      Assert.assertTrue(e.getMessage().contains("rpc: can't find method"));
    }

    client.close();
  }

  @Test
  public void testMissingMethodArgs() throws IOException, GoRpcException, ApplicationException {
    Client client = Client.dialHttp(Util.HOST, Util.PORT, Util.PATH, new BsonClientCodecFactory());
    BSONObject mArgs = new BasicBSONObject();
    mArgs.put("A", 5L);
    // incomplete args defaults to zero values
    Response response = client.call("Arith.Multiply", mArgs);
    Assert.assertEquals(0L, response.getReply());
    Assert.assertNull(response.getError());
    client.close();
  }

  @Test
  public void testValidMultipleCalls() throws IOException, GoRpcException, ApplicationException {
    Client client = Client.dialHttp(Util.HOST, Util.PORT, Util.PATH, new BsonClientCodecFactory());

    BSONObject mArgs = new BasicBSONObject();
    mArgs.put("A", 5L);
    mArgs.put("B", 7L);
    Response response = client.call("Arith.Multiply", mArgs);
    Assert.assertEquals(35L, response.getReply());

    mArgs = new BasicBSONObject();
    mArgs.put("A", 2L);
    mArgs.put("B", 3L);
    response = client.call("Arith.Multiply", mArgs);
    Assert.assertEquals(6L, response.getReply());

    client.close();
  }

  @Test
  public void testCallOnClosedClient() throws IOException, GoRpcException, ApplicationException {
    Client client = Client.dialHttp(Util.HOST, Util.PORT, Util.PATH, new BsonClientCodecFactory());

    BSONObject mArgs = new BasicBSONObject();
    mArgs.put("A", 5L);
    mArgs.put("B", 7L);
    Response response = client.call("Arith.Multiply", mArgs);
    Assert.assertEquals(35L, response.getReply());
    client.close();

    try {
      client.call("Arith.Multiply", mArgs);
      Assert.fail("did not raise exception");
    } catch (GoRpcException e) {
      Assert.assertTrue("client is closed".equals(e.getMessage()));
    }

  }
}
