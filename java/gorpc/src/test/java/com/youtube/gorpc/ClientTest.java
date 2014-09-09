package com.youtube.gorpc;

import java.io.IOException;
import java.net.ServerSocket;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.youtube.gorpc.Client.GoRpcException;
import com.youtube.gorpc.codecs.bson.BsonClientCodecFactory;

public class ClientTest {

	static int port = 15773;
	ServerSocket serverSocket;

	@Before
	public void setUp() throws IOException, InterruptedException {
		serverSocket = new ServerSocket(port);
		FakeGoServer server = new FakeGoServer(serverSocket);
		server.start();
		Thread.sleep(100);
	}

	@After
	public void tearDown() throws IOException {
		serverSocket.close();
	}

	@Test
	public void testValidCase() throws IOException, GoRpcException {
		Client client = Client.dialHttp("localhost", port, "/_bson_rpc_",
				new BsonClientCodecFactory());
		BSONObject mArgs = new BasicBSONObject();
		mArgs.put("A", 5);
		mArgs.put("B", 7);
		Response response = client.call("Arith.Multiply", mArgs);
		Assert.assertNull(response.getHeader().getError());
		Assert.assertNull(response.getBody().getError());
		Assert.assertEquals(35, response.getBody().getResult());
		client.close();
	}

	@Test
	public void testInValidHandshake() throws IOException, GoRpcException {
		try {
			Client client = Client.dialHttp("localhost", port, "/_somerpc_",
					new BsonClientCodecFactory());
			client.close();
			Assert.fail("did not raise exception");
		} catch (GoRpcException e) {
			Assert.assertTrue("unexpected response for handshake HTTP/1.0 404 Not Found"
					.equals(e.getMessage()));
		}
	}

	@Test
	public void testInValidMethodName() throws IOException, GoRpcException {
		Client client = Client.dialHttp("localhost", port, "/_bson_rpc_",
				new BsonClientCodecFactory());
		try {
			client.call("Arith.SomeMethod", new BasicBSONObject());
			client.close();
			Assert.fail("did not raise exception");
		} catch (GoRpcException e) {
			Assert.assertTrue("unknown method".equals(e.getMessage()));
		}

		client.close();
	}

	@Test
	public void testInValidMethodArgs() throws IOException, GoRpcException {
		Client client = Client.dialHttp("localhost", port, "/_bson_rpc_",
				new BsonClientCodecFactory());
		BSONObject mArgs = new BasicBSONObject();
		mArgs.put("A", 5);
		Response response = client.call("Arith.Multiply", mArgs);
		Assert.assertEquals(null, response.getBody().getResult());
		Assert.assertNotNull(response.getBody().getError());
		client.close();
	}

	@Test
	public void testValidMultipleCalls() throws IOException, GoRpcException {
		Client client = Client.dialHttp("localhost", port, "/_bson_rpc_",
				new BsonClientCodecFactory());

		BSONObject mArgs = new BasicBSONObject();
		mArgs.put("A", 5);
		mArgs.put("B", 7);
		Response response = client.call("Arith.Multiply", mArgs);
		Assert.assertEquals(35, response.getBody().getResult());

		mArgs = new BasicBSONObject();
		mArgs.put("A", 2);
		mArgs.put("B", 3);
		response = client.call("Arith.Multiply", mArgs);
		Assert.assertEquals(6, response.getBody().getResult());

		client.close();
	}

	@Test
	public void testCallOnClosedClient() throws IOException, GoRpcException {
		Client client = Client.dialHttp("localhost", port, "/_bson_rpc_",
				new BsonClientCodecFactory());

		BSONObject mArgs = new BasicBSONObject();
		mArgs.put("A", 5);
		mArgs.put("B", 7);
		Response response = client.call("Arith.Multiply", mArgs);
		Assert.assertEquals(35, response.getBody().getResult());
		client.close();

		try {
			client.call("Arith.Multiply", mArgs);
			Assert.fail("did not raise exception");
		} catch (GoRpcException e) {
			Assert.assertTrue("cannot call on a closed client".equals(e
					.getMessage()));
		}

	}
}
