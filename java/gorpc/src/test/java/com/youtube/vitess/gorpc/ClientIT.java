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

import com.youtube.vitess.gorpc.Client;
import com.youtube.vitess.gorpc.Exceptions.ApplicationException;
import com.youtube.vitess.gorpc.Exceptions.GoRpcException;
import com.youtube.vitess.gorpc.Response;
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
	}

}
