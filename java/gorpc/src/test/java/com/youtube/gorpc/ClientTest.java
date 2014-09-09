package com.youtube.gorpc;

import java.io.IOException;
import java.net.ServerSocket;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.youtube.gorpc.Exceptions.ApplicationError;
import com.youtube.gorpc.Exceptions.GoRpcException;
import com.youtube.gorpc.codecs.bson.BsonClientCodecFactory;

public class ClientTest {

	private static ServerSocket serverSocket;
	private static final int PORT = 15773;

	public int getPort() {
		return PORT;
	}

	@Before
	public void setUp() throws IOException, InterruptedException {
		serverSocket = new ServerSocket(PORT);
		FakeGoServer server = new FakeGoServer(serverSocket);
		server.start();
		Thread.sleep(500);
	}

	@After
	public void tearDown() throws IOException {
		serverSocket.close();
	}

	@Test
	public void testValidCase() throws IOException, GoRpcException,
			ApplicationError {
		Client client = Client.dialHttp("localhost", getPort(), "/_bson_rpc_",
				new BsonClientCodecFactory());
		BSONObject mArgs = new BasicBSONObject();
		mArgs.put("A", 5L);
		mArgs.put("B", 7L);
		Response response = client.call("Arith.Multiply", mArgs);
		Assert.assertNull(response.getError());
		Assert.assertEquals(35L, response.getResult());
		client.close();
	}

	@Test
	public void testInValidHandshake() throws IOException, GoRpcException {
		try {
			Client client = Client.dialHttp("localhost", getPort(),
					"/_somerpc_", new BsonClientCodecFactory());
			client.close();
			Assert.fail("did not raise exception");
		} catch (GoRpcException e) {
			Assert.assertTrue("unexpected response for handshake HTTP/1.0 404 Not Found"
					.equals(e.getMessage()));
		}
	}

	@Test
	public void testInValidMethodName() throws IOException, GoRpcException {
		Client client = Client.dialHttp("localhost", getPort(), "/_bson_rpc_",
				new BsonClientCodecFactory());
		try {
			client.call("Arith.SomeMethod", new BasicBSONObject());
			client.close();
			Assert.fail("did not raise exception");
		} catch (ApplicationError e) {
			Assert.assertTrue(e.getMessage().contains("rpc: can't find method"));
		}

		client.close();
	}

	@Test
	public void testMissingMethodArgs() throws IOException, GoRpcException,
			ApplicationError {
		Client client = Client.dialHttp("localhost", getPort(), "/_bson_rpc_",
				new BsonClientCodecFactory());
		BSONObject mArgs = new BasicBSONObject();
		mArgs.put("A", 5L);
		// incomplete args defaults to zero values
		Response response = client.call("Arith.Multiply", mArgs);
		Assert.assertEquals(0L, response.getResult());
		Assert.assertNull(response.getError());
		client.close();
	}

	@Test
	public void testValidMultipleCalls() throws IOException, GoRpcException,
			ApplicationError {
		Client client = Client.dialHttp("localhost", getPort(), "/_bson_rpc_",
				new BsonClientCodecFactory());

		BSONObject mArgs = new BasicBSONObject();
		mArgs.put("A", 5L);
		mArgs.put("B", 7L);
		Response response = client.call("Arith.Multiply", mArgs);
		Assert.assertEquals(35L, response.getResult());

		mArgs = new BasicBSONObject();
		mArgs.put("A", 2L);
		mArgs.put("B", 3L);
		response = client.call("Arith.Multiply", mArgs);
		Assert.assertEquals(6L, response.getResult());

		client.close();
	}

	@Test
	public void testCallOnClosedClient() throws IOException, GoRpcException,
			ApplicationError {
		Client client = Client.dialHttp("localhost", getPort(), "/_bson_rpc_",
				new BsonClientCodecFactory());

		BSONObject mArgs = new BasicBSONObject();
		mArgs.put("A", 5L);
		mArgs.put("B", 7L);
		Response response = client.call("Arith.Multiply", mArgs);
		Assert.assertEquals(35L, response.getResult());
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
