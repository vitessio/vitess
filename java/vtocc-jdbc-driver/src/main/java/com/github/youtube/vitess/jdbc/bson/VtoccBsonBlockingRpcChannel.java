package com.github.youtube.vitess.jdbc.bson;

import com.google.inject.Inject;
import com.google.protobuf.BlockingRpcChannel;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/**
 * Implementation of BlockingRpcChannel to connect to vtocc using Bson.
 */
public class VtoccBsonBlockingRpcChannel implements BlockingRpcChannel {

  private static final Logger LOGGER = LoggerFactory.getLogger(
      VtoccBsonBlockingRpcChannel.class);
  private final String vtoccServerSpec;
  private final GoRpcBsonSerializer goRpcBsonSerializer;
  private final SocketFactory socketFactory;
  private int sequence = 0;

  protected VtoccBsonBlockingRpcChannel(String vtoccServerSpec,
      GoRpcBsonSerializer goRpcBsonSerializer,
      SocketFactory socketFactory) {
    this.vtoccServerSpec = vtoccServerSpec;
    this.goRpcBsonSerializer = goRpcBsonSerializer;
    this.socketFactory = socketFactory;
  }

  /**
   * Delegates the RPC to making testing easier.
   *
   * @see #callBlockingMethod(String, com.google.protobuf.Message)
   */
  @Override
  public Message callBlockingMethod(MethodDescriptor methodDescriptor, RpcController rpcController,
      Message request, Message responseProto) throws ServiceException {
    //TODO(gco): Change to not reconnect with each channel to save resources

    try {
      return callBlockingMethod(methodDescriptor.getName(), request);
    } catch (IOException e) {
      rpcController.setFailed(e.getMessage());
      throw new ServiceException(e);
    }
  }

  /**
   * Performs RPC and converts response to a {@link com.google.protobuf.Message} response based on
   * the method being called.
   */
  protected Message callBlockingMethod(String methodName, Message request) throws IOException {
    LOGGER.debug("Connecting to on {}. {}", vtoccServerSpec, methodName);

    try (Socket socket = socketFactory.connect(vtoccServerSpec);
        InputStream in = socket.getInputStream();
        OutputStream out = socket.getOutputStream()) {

      out.write("CONNECT /_bson_rpc_ HTTP/1.0\n\n".getBytes(StandardCharsets.UTF_8));
      String connectResponse = new BufferedReader(new InputStreamReader(in)).readLine();
      LOGGER.debug("Connection response {}", connectResponse);
      out.write(goRpcBsonSerializer.encode(methodName, sequence, request));

      goRpcBsonSerializer.checkHeader(methodName, in);

      return goRpcBsonSerializer.decodeBody(methodName, in);
    }
  }

  /**
   * Factory for creating a new VtoccBsonBlockingRpcChannel
   */
  public static class Factory {

    private final GoRpcBsonSerializer goRpcBsonSerializer;
    private final SocketFactory socketFactory;

    @Inject
    public Factory(GoRpcBsonSerializer goRpcBsonSerializer,
        SocketFactory socketFactory) {
      this.goRpcBsonSerializer = goRpcBsonSerializer;
      this.socketFactory = socketFactory;
    }

    public VtoccBsonBlockingRpcChannel create(String vtoccServerSpec) {
      return new VtoccBsonBlockingRpcChannel(vtoccServerSpec, goRpcBsonSerializer,
          socketFactory);
    }
  }


}

