package com.youtube.vitess.vtgate.rpcclient;

import com.google.common.net.HostAndPort;

import com.youtube.vitess.gorpc.Client;
import com.youtube.vitess.gorpc.Exceptions.GoRpcException;
import com.youtube.vitess.gorpc.codecs.bson.BsonClientCodecFactory;
import com.youtube.vitess.vtgate.Exceptions.ConnectionException;
import com.youtube.vitess.vtgate.rpcclient.gorpc.GoRpcClient;

public class RpcClientFactory {
  public static RpcClient get(String address, int timeoutMs) throws ConnectionException {
    try {
      HostAndPort hostAndPort = HostAndPort.fromString(address);
      Client client =
          Client.dialHttp(hostAndPort.getHostText(), hostAndPort.getPort(),
              GoRpcClient.BSON_RPC_PATH, timeoutMs, new BsonClientCodecFactory());
      return new GoRpcClient(client);
    } catch (GoRpcException e) {
      GoRpcClient.LOGGER.error("vtgate connection exception: ", e);
      throw new ConnectionException(e.getMessage());
    }
  }
}
