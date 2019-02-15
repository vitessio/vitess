package io.vitess.client.grpc.netty;

import io.grpc.netty.NettyChannelBuilder;

public class SimpleChannelBuilderProvider implements NettyChannelBuilderProvider {

  @Override
  public NettyChannelBuilder getChannelBuilder(String target) {
    return NettyChannelBuilder.forTarget(target);
  }
}
