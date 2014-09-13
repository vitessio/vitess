package com.youtube.vitess.gorpc.codecs.bson;

import java.io.IOException;
import java.net.Socket;

import org.apache.commons.lang.CharEncoding;
import org.bson.BSONDecoder;
import org.bson.BSONEncoder;
import org.bson.BSONObject;
import org.bson.BasicBSONEncoder;
import org.bson.BasicBSONObject;

import com.youtube.vitess.gorpc.Constants;
import com.youtube.vitess.gorpc.Request;
import com.youtube.vitess.gorpc.Response;
import com.youtube.vitess.gorpc.codecs.ClientCodec;

public class BsonClientCodec implements ClientCodec {
	// Server uses MAGIC_TAG to embed simple types inside BSON document
	public static final String MAGIC_TAG = "_Val_";

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
		header.put(Constants.SERVICE_METHOD, request.getServiceMethod());
		header.put(Constants.SEQ, request.getSeq());
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
		response.setServiceMethod(new String((byte[]) headerBson
				.get(Constants.SERVICE_METHOD)));
		response.setSeq((Long) headerBson.get(Constants.SEQ));
		if (headerBson.containsField(Constants.ERROR)) {
			Object error = headerBson.get(Constants.ERROR);
			if (error instanceof byte[] && ((byte[]) error).length != 0) {
				String errorMsg = new String((byte[]) error, CharEncoding.UTF_8);
				response.setError(errorMsg);
			}
		}
	}

	public void ReadResponseBody(Response response) throws IOException {
		BSONObject bodyBson = decoder.readObject(socket.getInputStream());
		if (bodyBson.containsField(MAGIC_TAG)) {
			response.setReply(bodyBson.get(MAGIC_TAG));
		} else {
			response.setReply(bodyBson);
		}
	}

	public void close() throws IOException {
		if (socket != null) {
			socket.close();
			socket = null;
		}
	}

}
