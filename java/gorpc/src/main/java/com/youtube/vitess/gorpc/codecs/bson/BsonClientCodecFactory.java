package com.youtube.vitess.gorpc.codecs.bson;

import com.youtube.vitess.gorpc.codecs.ClientCodec;
import com.youtube.vitess.gorpc.codecs.ClientCodecFactory;

import java.net.Socket;

public class BsonClientCodecFactory implements ClientCodecFactory {
  public ClientCodec createCodec(Socket socket) {
    return new BsonClientCodec(socket);
  }
}
