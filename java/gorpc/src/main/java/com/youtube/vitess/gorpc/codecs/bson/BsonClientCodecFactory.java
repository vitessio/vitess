package com.youtube.vitess.gorpc.codecs.bson;

import java.net.Socket;

import com.youtube.vitess.gorpc.codecs.ClientCodec;
import com.youtube.vitess.gorpc.codecs.ClientCodecFactory;

public class BsonClientCodecFactory implements ClientCodecFactory {
	public ClientCodec createCodec(Socket socket) {
		return new BsonClientCodec(socket);
	}
}
