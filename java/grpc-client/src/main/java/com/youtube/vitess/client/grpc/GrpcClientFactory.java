package com.youtube.vitess.client.grpc;

import com.youtube.vitess.client.Context;
import com.youtube.vitess.client.RpcClient;
import com.youtube.vitess.client.RpcClientFactory;

import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;

import java.net.InetSocketAddress;

/**
 * GrpcClientFactory creates RpcClients with the gRPC implemenation.
 */
public class GrpcClientFactory implements RpcClientFactory {
  @Override
  public RpcClient create(Context ctx, InetSocketAddress address) {
    return new GrpcClient(
        NettyChannelBuilder.forAddress(address).negotiationType(NegotiationType.PLAINTEXT).build());
  }
}
