package com.youtube.vitess.gorpc;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.youtube.vitess.gorpc.Client;
import com.youtube.vitess.gorpc.Exceptions.ApplicationException;
import com.youtube.vitess.gorpc.Exceptions.GoRpcException;
import com.youtube.vitess.gorpc.Response;
import com.youtube.vitess.gorpc.codecs.bson.BsonClientCodecFactory;

/**
 * Integration tests for streaming RPC calls.
 * 
 * These tests are mimicking the tests in go/rpcplus/streaming_test.go.
 * Arith.Thrive is a streaming method that returns a specified value "A" over
 * "Count" fetches. Optionally pass in "ErrotAt" and "BadTypeAt" to trigger
 * corresponding errors.
 */
public class StreamClientIT {

	@BeforeClass
	public static void initServer() throws IOException, InterruptedException {
		Util.startRealGoServer();
	}

	@AfterClass
	public static void tearDownServer() throws Exception {
		Util.stopRealGoServer();
	}

	@Test
	public void testStreaming() throws GoRpcException, IOException,
			ApplicationException {
		Client client = Client.dialHttp(Util.HOST, Util.PORT, Util.PATH,
				new BsonClientCodecFactory());
		Map<String, Long> args = new HashMap<>();
		args.put("A", 10L);
		args.put("Count", 20L);
		args.put("ErrorAt", -1L);
		args.put("BadTypeAt", -1L);
		BSONObject mArgs = new BasicBSONObject();
		mArgs.putAll(args);
		client.streamCall("Arith.Thrive", mArgs);
		Response response;

		long index = 0;
		while ((response = client.streamNext()) != null) {
			BSONObject result = (BSONObject) response.getReply();
			Assert.assertEquals(args.get("A"), result.get("C"));
			Assert.assertEquals(index, result.get("Index"));
			index++;
		}
		Assert.assertEquals(args.get("Count").longValue(), index);
		client.close();
	}

	@Test
	public void testStreamingErrorInTheMiddle() throws GoRpcException,
			IOException,
			ApplicationException {
		Client client = Client.dialHttp(Util.HOST, Util.PORT, Util.PATH,
				new BsonClientCodecFactory());
		Map<String, Long> args = new HashMap<>();
		args.put("A", 10L);
		args.put("Count", 20L);
		args.put("ErrorAt", 5L);
		args.put("BadTypeAt", -1L);
		BSONObject mArgs = new BasicBSONObject();
		mArgs.putAll(args);
		client.streamCall("Arith.Thrive", mArgs);
		Response response;
		long index = 0;
		while (index < args.get("ErrorAt")
				&& (response = client.streamNext()) != null) {
			BSONObject result = (BSONObject) response.getReply();
			Assert.assertEquals(args.get("A"), result.get("C"));
			Assert.assertEquals(index, result.get("Index"));
			index++;
		}

		Assert.assertEquals(args.get("ErrorAt").longValue(), index);
		try {
			response = client.streamNext();
			Assert.fail("failed to raise exception");
		} catch (ApplicationException e) {
			Assert.assertEquals("Triggered error in middle", e.getMessage());
		}
		client.close();
	}

	@Test
	public void testStreamingBadTypeError() throws GoRpcException, IOException,
			ApplicationException {
		Client client = Client.dialHttp(Util.HOST, Util.PORT, Util.PATH,
				new BsonClientCodecFactory());
		Map<String, Long> args = new HashMap<>();
		args.put("A", 10L);
		args.put("Count", 20L);
		args.put("ErrorAt", -1L);
		args.put("BadTypeAt", 4L);
		BSONObject mArgs = new BasicBSONObject();
		mArgs.putAll(args);
		client.streamCall("Arith.Thrive", mArgs);
		Response response;
		long index = 0;
		while (index < args.get("BadTypeAt")
				&& (response = client.streamNext()) != null) {
			BSONObject result = (BSONObject) response.getReply();
			Assert.assertEquals(args.get("A"), result.get("C"));
			Assert.assertEquals(index, result.get("Index"));
			index++;
		}

		Assert.assertEquals(args.get("BadTypeAt").longValue(), index);
		try {
			response = client.streamNext();
			Assert.fail("failed to raise exception");
		} catch (ApplicationException e) {
			Assert.assertEquals("rpc: passing wrong type to sendReply",
					e.getMessage());
		}
		client.close();
	}

	/**
	 * Ensure new requests are not allowed while streaming is in progress
	 */
	@Test
	public void testNewRequestWhileStreaming() throws GoRpcException,
			IOException,
			ApplicationException {
		Client client = Client.dialHttp(Util.HOST, Util.PORT, Util.PATH,
				new BsonClientCodecFactory());
		Map<String, Long> args = new HashMap<>();
		args.put("A", 10L);
		args.put("Count", 3L);
		args.put("ErrorAt", -1L);
		args.put("BadTypeAt", -1L);
		BSONObject mArgs = new BasicBSONObject();
		mArgs.putAll(args);
		client.streamCall("Arith.Thrive", mArgs);
		try {
			client.call("Arith.Multiply", mArgs);
			Assert.fail("failed to raise exception");
		} catch (GoRpcException e) {
			Assert.assertEquals(
					"request not allowed as client is in the middle of streaming",
					e.getMessage());
		}
		client.close();
	}

	/**
	 * Ensure error is raised for fetching after stream has ended
	 */
	@Test
	public void testFetchNextAfterStreamEnded() throws GoRpcException,
			IOException,
			ApplicationException {
		Client client = Client.dialHttp(Util.HOST, Util.PORT, Util.PATH,
				new BsonClientCodecFactory());
		Map<String, Long> args = new HashMap<>();
		args.put("A", 10L);
		args.put("Count", 3L);
		args.put("ErrorAt", -1L);
		args.put("BadTypeAt", -1L);
		BSONObject mArgs = new BasicBSONObject();
		mArgs.putAll(args);
		client.streamCall("Arith.Thrive", mArgs);
		while (client.streamNext() != null) {
		}

		try {
			client.streamNext();
			Assert.fail("failed to raise exception");
		} catch (GoRpcException e) {
			Assert.assertEquals("no streaming requests pending", e.getMessage());
		}
		client.close();
	}
}
