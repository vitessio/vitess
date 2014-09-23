package com.youtube.vitess.gorpc;

import java.io.IOException;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.primitives.UnsignedLong;
import com.youtube.vitess.gorpc.Exceptions.ApplicationException;
import com.youtube.vitess.gorpc.Exceptions.GoRpcException;
import com.youtube.vitess.gorpc.codecs.bson.BsonClientCodecFactory;

/**
 * ClientIT runs the same tests as ClientTest but uses a real Go BSON RPC
 * server.
 */
public class ClientIT extends ClientTest {

	@BeforeClass
	public static void initServer() throws IOException, InterruptedException {
		Util.startRealGoServer();
	}

	@AfterClass
	public static void tearDownServer() throws Exception {
		Util.stopRealGoServer();
	}

	@Before
	@Override
	public void setUp() throws IOException, InterruptedException {
	}

	@After
	@Override
	public void tearDown() throws IOException {
	}

	@Test
	public void testDivide() throws GoRpcException, IOException,
			ApplicationException {
		Client client = Client.dialHttp(Util.HOST, Util.PORT, Util.PATH,
				new BsonClientCodecFactory());
		BSONObject mArgs = new BasicBSONObject();
		mArgs.put("A", 10L);
		mArgs.put("B", 3L);
		Response response = client.call("Arith.Divide", mArgs);
		BSONObject result = (BSONObject) response.getReply();
		Assert.assertEquals(3L, result.get("Quo"));
		Assert.assertEquals(1L, result.get("Rem"));
		client.close();
	}

	@Test
	public void testDivideError() throws GoRpcException, IOException,
			ApplicationException {
		Client client = Client.dialHttp(Util.HOST, Util.PORT, Util.PATH,
				new BsonClientCodecFactory());
		BSONObject mArgs = new BasicBSONObject();
		mArgs.put("A", 10L);
		mArgs.put("B", 0L);
		try {
			client.call("Arith.Divide", mArgs);
			Assert.fail("did not raise application error");
		} catch (ApplicationException e) {
			Assert.assertEquals("divide by zero", e.getMessage());
		}
		client.close();
	}

	/**
	 * Test no exception raised for client with timeouts disabled
	 */
	@Test
	public void testNoTimeoutCase() throws Exception {
		Client client = Client.dialHttp(Util.HOST, Util.PORT, Util.PATH,
				new BsonClientCodecFactory());
		timeoutCheck(client, 500, false);
		client.close();
	}

	/**
	 * Test client with timeout enabled
	 */
	@Test
	public void testTimeout() throws Exception {
		int clientTimeoutMs = 100;
		Client client = Client.dialHttp(Util.HOST, Util.PORT, Util.PATH,
				clientTimeoutMs, new BsonClientCodecFactory());
		timeoutCheck(client, clientTimeoutMs / 2, false);
		timeoutCheck(client, clientTimeoutMs * 2, true);
		client.close();
	}

	private void timeoutCheck(Client client, int timeoutMs,
			boolean shouldTimeout) throws Exception {
		BSONObject mArgs = new BasicBSONObject();
		mArgs.put("Duration", timeoutMs);

		if (shouldTimeout) {
			try {
				client.call("Arith.Sleep", mArgs);
				Assert.fail("did not raise timeout error");
			} catch (GoRpcException e) {
				Assert.assertEquals("connection exception Read timed out",
						e.getMessage());
			}
		} else {
			client.call("Arith.Sleep", mArgs);
		}
	}

	/**
	 * Test unsigned longs are encoded and decoded correctly
	 */
	@Test
	public void testUnsignedLongs() throws Exception {
		UnsignedLong a = UnsignedLong.valueOf("9767889778372766922");
		Client client = Client.dialHttp(Util.HOST, Util.PORT, Util.PATH,
				new BsonClientCodecFactory());
		BSONObject mArgs = new BasicBSONObject();
		mArgs.put("Num", a);
		Response response = client.call("Arith.Increment", mArgs);
		Assert.assertEquals(a.plus(UnsignedLong.ONE),
				(UnsignedLong) response.getReply());
		client.close();
	}
}
