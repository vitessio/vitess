package com.youtube.gorpc.codecs.bson;

import java.net.Socket;

import com.youtube.gorpc.codecs.ClientCodec;
import com.youtube.gorpc.codecs.ClientCodecFactory;

public class BsonClientCodecFactory implements ClientCodecFactory {
	public ClientCodec createCodec(Socket socket) {
		return new BsonClientCodec(socket);
	}
}
