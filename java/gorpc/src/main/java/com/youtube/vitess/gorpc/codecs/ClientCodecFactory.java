package com.youtube.vitess.gorpc.codecs;

import java.net.Socket;

public interface ClientCodecFactory {
	public ClientCodec createCodec(Socket socket);
}
