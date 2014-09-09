package com.youtube.gorpc.codecs.bson;

import java.io.IOException;
import java.net.Socket;

import org.apache.commons.lang.CharEncoding;
import org.bson.BSONDecoder;
import org.bson.BSONEncoder;
import org.bson.BSONObject;
import org.bson.BasicBSONEncoder;
import org.bson.BasicBSONObject;

import com.youtube.gorpc.Request;
import com.youtube.gorpc.Response;
import com.youtube.gorpc.codecs.ClientCodec;

public class BsonClientCodec implements ClientCodec {
	private Socket socket;
	private BSONEncoder encoder;
	private BSONDecoder decoder;

	public BsonClientCodec(Socket socket) {
		this.socket = socket;
		encoder = new BasicBSONEncoder();
		decoder = new GoRpcBsonDecoder();
	}

	public void WriteRequest(Request request, Object args) throws IOException {
		BSONObject header = new BasicBSONObject();
		header.put(Request.SERVICE_METHOD, request.getServiceMethod());
		header.put(Request.SEQ, request.getSeq());
		byte[] headerBytes = encoder.encode(header);
		byte[] bodyBytes = encoder.encode((BSONObject) args);
		byte[] bytes = new byte[headerBytes.length + bodyBytes.length];
		System.arraycopy(headerBytes, 0, bytes, 0, headerBytes.length);
		System.arraycopy(bodyBytes, 0, bytes, headerBytes.length,
				bodyBytes.length);
		socket.getOutputStream().write(bytes);
	}

	public void ReadResponseHeader(Response response) throws IOException {
		BSONObject headerBson = decoder.readObject(socket.getInputStream());
		Response.Header header = response.new Header();
		header.setServiceMethod(new String((byte[]) headerBson
				.get(Response.Header.SERVICE_METHOD)));
		header.setSeq((Long) headerBson.get(Response.Header.SEQ));
		if (headerBson.containsField(Response.Header.ERROR)) {
			Object error = headerBson.get(Response.Header.ERROR);
			if (error instanceof byte[] && ((byte[]) error).length != 0) {
				String errorMsg = new String((byte[]) error, CharEncoding.UTF_8);
				header.setError(errorMsg);
			}
		}
		response.setHeader(header);
	}

	public void ReadResponseBody(Response response) throws IOException {
		BSONObject bodyBson = decoder.readObject(socket.getInputStream());
		Response.Body body = response.new Body();

		if (bodyBson.containsField(Response.Body.ERROR)) {
			Object error = bodyBson.get(Response.Body.ERROR);
			if (error instanceof byte[] && ((byte[]) error).length != 0) {
				String errorMsg = new String((byte[]) error, CharEncoding.UTF_8);
				body.setError(errorMsg);
			}
		}
		body.setResult(bodyBson.get(Response.Body.RESULT));
		response.setBody(body);
	}

	public void close() throws IOException {
		if (socket != null) {
			socket.close();
			socket = null;
		}
	}

}
