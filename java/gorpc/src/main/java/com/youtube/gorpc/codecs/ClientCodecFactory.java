package com.youtube.gorpc.codecs;

import java.net.Socket;

public interface ClientCodecFactory {
	public ClientCodec createCodec(Socket socket);
}
