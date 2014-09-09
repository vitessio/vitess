package com.youtube.gorpc.codecs;

import java.io.IOException;

import com.youtube.gorpc.Request;
import com.youtube.gorpc.Response;

/**
 * Mirrors rpc.ClientCodec in Golang net/rpc package
 * 
 */
public interface ClientCodec {
	void WriteRequest(Request request, Object args) throws IOException;

	void ReadResponseHeader(Response response) throws IOException;

	void ReadResponseBody(Response response) throws IOException;

	void close() throws IOException;

}
