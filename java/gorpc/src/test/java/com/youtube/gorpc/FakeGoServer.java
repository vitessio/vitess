package com.youtube.gorpc;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;

import org.bson.BSONDecoder;
import org.bson.BSONEncoder;
import org.bson.BSONObject;
import org.bson.BasicBSONEncoder;
import org.bson.BasicBSONObject;

import com.youtube.gorpc.codecs.bson.BsonClientCodec;
import com.youtube.gorpc.codecs.bson.GoRpcBsonDecoder;

/**
 * FakeGoServer emulates a Go rpc server using bson codec. It does minimal error
 * checking with hardcoded path names, methods, error messages etc.
 */
public class FakeGoServer extends Thread {
	ServerSocket serverSocket;

	FakeGoServer(ServerSocket serverSocket) {
		this.serverSocket = serverSocket;
	}

	public void run() {
		try {
			while (true) {
				Socket clientSocket = serverSocket.accept();
				new ArithThread(clientSocket).start();
			}
		} catch (SocketException e) {
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static class ArithThread extends Thread {
		private Socket clientSocket;
		private BSONDecoder decoder = new GoRpcBsonDecoder();
		private BSONEncoder encoder = new BasicBSONEncoder();

		ArithThread(Socket clientSocket) {
			this.clientSocket = clientSocket;
		}

		public void run() {
			try {
				PrintWriter out = new PrintWriter(
						clientSocket.getOutputStream(), true);
				BufferedReader in = new BufferedReader(new InputStreamReader(
						clientSocket.getInputStream()));
				String input = in.readLine();
				if (!input.equals("CONNECT /_bson_rpc_ HTTP/1.0")) {
					out.println("HTTP/1.0 404 Not Found");
					return;
				}

				out.println(Client.CONNECTED);
				out.println();

				while (true) {
					serveRequest();
				}

			} catch (Exception e) {
			}
		}

		private void serveRequest() throws IOException {
			BSONObject reqHead = decoder.readObject(clientSocket
					.getInputStream());
			BSONObject reqBody = decoder.readObject(clientSocket
					.getInputStream());

			BSONObject respHead = new BasicBSONObject();
			respHead.put(Constants.SERVICE_METHOD,
					((String) reqHead.get(Constants.SERVICE_METHOD)).getBytes());
			respHead.put(Constants.SEQ, (Long) reqHead.get(Constants.SEQ));
			if (!"Arith.Multiply".equals((String) reqHead
					.get(Constants.SERVICE_METHOD))) {
				respHead.put(Constants.ERROR,
						("rpc: can't find method " + (String) reqHead
								.get(Constants.SERVICE_METHOD)).getBytes());
			}

			BSONObject respBody = new BasicBSONObject();
			if (!respHead.containsField(Constants.ERROR)) {
				long A = 0;
				long B = 0;
				if (reqBody.containsField("A")) {
					A = (long) reqBody.get("A");
				}
				if (reqBody.containsField("B")) {
					B = (long) reqBody.get("B");
				}
				respBody.put(BsonClientCodec.MAGIC_TAG, A * B);
			}

			clientSocket.getOutputStream().write(encoder.encode(respHead));
			clientSocket.getOutputStream().write(encoder.encode(respBody));
		}
	}
}