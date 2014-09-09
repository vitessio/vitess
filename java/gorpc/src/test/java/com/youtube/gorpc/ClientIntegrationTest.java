package com.youtube.gorpc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

/**
 * ClientIntegrationTest runs the same tests as ClientTest but uses a real Go
 * BSON RPC server.
 */
public class ClientIntegrationTest extends ClientTest {
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
		command.add(ClientIntegrationTest.class.getResource("/arithserver.go")
				.getPath());
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
}
