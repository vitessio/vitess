package com.youtube.vitess.gorpc;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;

import org.apache.commons.lang.CharEncoding;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.google.common.primitives.UnsignedLong;
import com.youtube.vitess.gorpc.Exceptions.ApplicationException;
import com.youtube.vitess.gorpc.Exceptions.GoRpcException;
import com.youtube.vitess.gorpc.codecs.ClientCodec;
import com.youtube.vitess.gorpc.codecs.ClientCodecFactory;

/**
 * Client represents an RPC client that can communicate with a Go RPC server.
 * Same client can be used for making multiple calls but concurrent calls are
 * not supported. Multiple codecs can be supported by extending ClientCodec
 * interface.
 *
 * TODO(anandhenry): authentication
 */
public class Client {
	public static final String CONNECTED = "HTTP/1.0 200 Connected to Go RPC";
	private static final String LAST_STREAM_RESPONSE_ERROR = "EOS";
	static final Logger logger = LogManager.getLogger(Client.class.getName());

	private ClientCodec codec;
	private boolean closed;
	private UnsignedLong seq;
	private boolean isStreaming;

	public Client(ClientCodec codec) {
		this.codec = codec;
		closed = false;
		seq = UnsignedLong.ZERO;
	}

	/**
	 * call makes a synchronous RPC call using the specified serviceMethod and
	 * args. It waits for a response from the server indefinitely.
	 */
	public Response call(String serviceMethod, Object args)
			throws GoRpcException, ApplicationException {
		writeRequest(serviceMethod, args);
		return readResponse();
	}

	/**
	 * Streaming equivalent of call(). This only initiates the request. Use
	 * streamNext() to fetch the results.
	 */
	public void streamCall(String serviceMethod, Object args)
			throws GoRpcException {
		writeRequest(serviceMethod, args);
		isStreaming = true;
	}

	/**
	 * Fetches streaming results. Should be invoked only after streamCall().
	 * Returns Response until the stream has ended, which is marked by a null.
	 */
	public Response streamNext() throws GoRpcException,
			ApplicationException {
		if (!isStreaming) {
			throw new GoRpcException("no streaming requests pending");
		}
		try {
			return readResponse();
		} catch (ApplicationException e) {
			if (e.getMessage().equals(LAST_STREAM_RESPONSE_ERROR)) {
				isStreaming = false;
				return null;
			}
			throw e;
		}
	}

	public void close() throws GoRpcException {
		if (closed) {
			throw new GoRpcException("closing a closed connection");
		}
		try {
			codec.close();
		} catch (IOException e) {
			logger.error("connection exception", e);
			throw new GoRpcException("connection exception" + e.getMessage());
		}
		closed = true;
	}

	private void writeRequest(String serviceMethod, Object args)
			throws GoRpcException {
		if (closed) {
			throw new GoRpcException("client is closed");
		}
		if (isStreaming) {
			throw new GoRpcException(
					"request not allowed as client is in the middle of streaming");
		}
		seq = seq.plus(UnsignedLong.ONE);
		Request request = new Request(serviceMethod, seq);
		try {
			codec.WriteRequest(request, args);
		} catch (IOException e) {
			logger.error("connection exception", e);
			throw new GoRpcException("connection exception" + e.getMessage());
		}
	}

	private Response readResponse() throws GoRpcException, ApplicationException {
		Response response = new Response();
		try {
			codec.ReadResponseHeader(response);
			codec.ReadResponseBody(response);
		} catch (IOException e) {
			logger.error("connection exception", e);
			throw new GoRpcException("connection exception " + e.getMessage());
		}
		if (response.getSeq().compareTo(seq) != 0) {
			throw new GoRpcException("sequence number mismatch");
		}
		if (response.getError() != null) {
			throw new ApplicationException(response.getError());
		}
		return response;
	}

	public static Client dialHttp(String host, int port, String path,
			ClientCodecFactory cFactory)
			throws GoRpcException {
		return dialHttp(host, port, path, 0, cFactory);
	}

	public static Client dialHttp(String host, int port, String path,
			int socketTimeoutMs, ClientCodecFactory cFactory)
			throws GoRpcException {
		Socket s = null;
		try {
			s = new Socket(host, port);
			s.setSoTimeout(socketTimeoutMs);
			InputStream in = s.getInputStream();
			OutputStream out = s.getOutputStream();
			out.write(("CONNECT " + path + " HTTP/1.0\n\n")
					.getBytes(CharEncoding.UTF_8));
			BufferedReader reader = new BufferedReader(
					new InputStreamReader(in));
			String response = reader.readLine();
			if (!CONNECTED.equals(response)) {
				s.close();
				throw new GoRpcException("unexpected response for handshake "
						+ response);
			}
			return new Client(cFactory.createCodec(s));
		} catch (IOException e) {
			if (s != null) {
				try {
					s.close();
				} catch (IOException e1) {
					logger.error("unable to close socket", e1);
					throw new GoRpcException("unable to close socket"
							+ e1.getMessage());
				}
			}
			logger.error("connection exception", e);
			throw new GoRpcException("connection exception" + e.getMessage());
		}
	}
}
