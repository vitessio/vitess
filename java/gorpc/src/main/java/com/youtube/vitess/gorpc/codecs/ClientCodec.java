package com.youtube.vitess.gorpc.codecs;

import com.youtube.vitess.gorpc.Request;
import com.youtube.vitess.gorpc.Response;

import java.io.IOException;

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
