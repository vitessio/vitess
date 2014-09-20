package com.youtube.vitess.gorpc;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.CharEncoding;
import org.junit.Assert;

public class Util {
	public static final int PORT = 15113;
	public static final String HOST = "localhost";
	public static final String PATH = "/_bson_rpc_";

	private static FakeGoServer fakeGoServer;
	private static Process realGoServer;

	public static void startFakeGoServer() throws IOException,
			InterruptedException {
		fakeGoServer = new FakeGoServer(new ServerSocket(PORT));
		fakeGoServer.start();
		Thread.sleep(500);
	}

	public static void stopFakeGoServer() throws IOException {
		fakeGoServer.stopServer();
	}

	public static void startRealGoServer() throws IOException,
			InterruptedException {
		List<String> command = new ArrayList<String>();
		command.add("go");
		command.add("run");
		command.add(ClientIT.class.getResource("/arithserver.go").getPath());
		command.add("-port");
		command.add("" + PORT);
		ProcessBuilder builder = new ProcessBuilder(command);
		realGoServer = builder.start();
		Thread.sleep(TimeUnit.SECONDS.toMillis(1));
	}

	public static void stopRealGoServer() throws Exception {
		Socket s = new Socket(Util.HOST, Util.PORT);
		InputStream in = s.getInputStream();
		OutputStream out = s.getOutputStream();
		out.write(("CONNECT /shutdown HTTP/1.0\n\n")
				.getBytes(CharEncoding.UTF_8));
		BufferedReader reader = new BufferedReader(
				new InputStreamReader(in));
		StringBuilder sb = new StringBuilder();
		String response;
		while ((response = reader.readLine()) != null) {
			sb.append(response);
		}
		s.close();
		Assert.assertTrue("arith server failed to shutdown", sb.toString()
				.contains("shutting down"));
		Assert.assertEquals("arith server failed to shutdown", 0,
				realGoServer.waitFor());
	}
}
