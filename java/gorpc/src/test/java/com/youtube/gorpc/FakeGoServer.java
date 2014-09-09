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

				while (true) {
					BSONObject requestHeader = decoder.readObject(clientSocket
							.getInputStream());
					BSONObject requestBody = decoder.readObject(clientSocket
							.getInputStream());

					BSONObject responseHeader = new BasicBSONObject();
					responseHeader
							.put(Response.Header.SERVICE_METHOD,
									((String) requestHeader
											.get(Request.SERVICE_METHOD))
											.getBytes());
					responseHeader.put(Response.Header.SEQ,
							(Long) requestHeader.get(Request.SEQ));
					if (!"Arith.Multiply".equals((String) requestHeader
							.get(Request.SERVICE_METHOD))) {
						responseHeader.put(Response.Header.ERROR,
								"unknown method".getBytes());
					}

					clientSocket.getOutputStream().write(
							encoder.encode(responseHeader));

					if (!responseHeader.containsField(Response.Header.ERROR)) {
						BSONObject responseBody = new BasicBSONObject();
						if (requestBody.containsField("A")
								&& requestBody.containsField("B")) {
							int A = (Integer) requestBody.get("A");
							int B = (Integer) requestBody.get("B");
							responseBody.put(Response.Body.ERROR, "");
							responseBody.put(Response.Body.RESULT, A * B);
						} else {
							responseBody.put(Response.Body.ERROR,
									"invalid method args".getBytes());
						}

						clientSocket.getOutputStream().write(
								encoder.encode(responseBody));
					}
				}
			} catch (Exception e) {
			}
		}
	}
}
