package com.youtube.vitess.gorpc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.youtube.vitess.gorpc.Exceptions.ApplicationException;
import com.youtube.vitess.gorpc.Exceptions.GoRpcException;
import com.youtube.vitess.gorpc.codecs.bson.BsonClientCodecFactory;

/**
 * ClientIT runs the same tests as ClientTest but uses a real Go BSON RPC
 * server.
 */
public class ClientIT extends ClientTest {
	private static Process serverProcess;
	private static final int PORT = 1234;

	@Override
	public int getPort() {
		return PORT;
	}

	@Before
	@Override
	public void setUp() throws IOException, InterruptedException {
	}

	@After
	@Override
	public void tearDown() throws IOException {
	}

	@BeforeClass
	public static void initServer() throws IOException, InterruptedException {
		List<String> command = new ArrayList<String>();
		command.add("go");
		command.add("run");
		command.add(ClientIT.class.getResource("/arithserver.go").getPath());
		command.add("-port");
		command.add("" + PORT);
		ProcessBuilder builder = new ProcessBuilder(command);
		serverProcess = builder.start();
		Thread.sleep(TimeUnit.SECONDS.toMillis(1));
	}

	@AfterClass
	public static void tearDownServer() {
		serverProcess.destroy();
	}

	@Test
	public void testDivide() throws GoRpcException, IOException,
			ApplicationException {
		Client client = Client.dialHttp("localhost", getPort(), "/_bson_rpc_",
				new BsonClientCodecFactory());
		BSONObject mArgs = new BasicBSONObject();
		mArgs.put("A", 10L);
		mArgs.put("B", 3L);
		Response response = client.call("Arith.Divide", mArgs);
		BSONObject result = (BSONObject) response.reply;
		Assert.assertEquals(3L, result.get("Quo"));
		Assert.assertEquals(1L, result.get("Rem"));
	}
}
