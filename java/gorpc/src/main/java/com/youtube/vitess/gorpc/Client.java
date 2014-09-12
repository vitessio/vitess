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

import com.youtube.vitess.gorpc.Exceptions.ApplicationException;
import com.youtube.vitess.gorpc.Exceptions.GoRpcException;
import com.youtube.vitess.gorpc.codecs.ClientCodec;
import com.youtube.vitess.gorpc.codecs.ClientCodecFactory;

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
	static final Logger logger = LogManager.getLogger(Client.class.getName());

	private ClientCodec codec;
	private boolean closed;
	private long seq;

	public Client(ClientCodec codec) {
		this.codec = codec;
		closed = false;
		seq = 0;
	}

	public Response call(String serviceMethod, Object args)
			throws GoRpcException, ApplicationException {
		if (closed) {
			throw new GoRpcException("cannot call on a closed client");
		}
		seq++;
		Request request = new Request(serviceMethod, seq);
		Response response = new Response();
		try {
			codec.WriteRequest(request, args);
			codec.ReadResponseHeader(response);
			codec.ReadResponseBody(response);
		} catch (IOException e) {
			logger.error("connection exception", e);
			throw new GoRpcException("connection exception" + e.getMessage());
		}
		if (response.getSeq() != request.getSeq()) {
			throw new GoRpcException("sequence number mismatch");
		}
		if (response.getError() != null) {
			throw new ApplicationException(response.getError());
		}
		return response;
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

	public static Client dialHttp(String host, int port, String path,
			ClientCodecFactory cFactory) throws GoRpcException {
		Socket s = null;
		try {
			s = new Socket(host, port);
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
