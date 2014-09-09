package com.youtube.gorpc;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;

import org.apache.commons.lang.CharEncoding;

import com.youtube.gorpc.Exceptions.ApplicationError;
import com.youtube.gorpc.Exceptions.GoRpcException;
import com.youtube.gorpc.codecs.ClientCodec;
import com.youtube.gorpc.codecs.ClientCodecFactory;

/**
 * Client represents an RPC client that can communicate with a Go RPC server.
 * Same client can be used for making multiple calls but concurrent calls are
 * not supported. Multiple codecs can be supported by extending the ClientCodec
 * implementation.
 *
 * TODO(anandhenry): deadlines, authentication, streaming RPCs
 */
public class Client {
	public static final String CONNECTED = "HTTP/1.0 200 Connected to Go RPC";

	private ClientCodec codec;
	private boolean closed;
	private long seq;

	public Client(ClientCodec codec) {
		this.codec = codec;
		closed = false;
		seq = 0;
	}

	public Response call(String serviceMethod, Object args) throws IOException,
			GoRpcException, ApplicationError {
		if (closed) {
			throw new GoRpcException("cannot call on a closed client");
		}
		seq++;
		Request request = new Request(serviceMethod, seq);
		codec.WriteRequest(request, args);
		Response response = new Response();
		codec.ReadResponseHeader(response);
		codec.ReadResponseBody(response);
		if (response.getSeq() != request.getSeq()) {
			throw new GoRpcException("sequence number mismatch");
		}
		if (response.getError() != null) {
			throw new ApplicationError(response.getError());
		}
		return response;
	}

	public void close() throws IOException {
		codec.close();
		closed = true;
	}

	public static Client dialHttp(String host, int port, String path,
			ClientCodecFactory cFactory) throws GoRpcException, IOException {
		Socket s = new Socket(host, port);
		InputStream in = s.getInputStream();
		OutputStream out = s.getOutputStream();
		out.write(("CONNECT " + path + " HTTP/1.0\n\n")
				.getBytes(CharEncoding.UTF_8));
		BufferedReader reader = new BufferedReader(new InputStreamReader(in));
		String response = reader.readLine();
		if (!CONNECTED.equals(response)) {
			s.close();
			throw new GoRpcException("unexpected response for handshake "
					+ response);
		}
		return new Client(cFactory.createCodec(s));
	}
}
