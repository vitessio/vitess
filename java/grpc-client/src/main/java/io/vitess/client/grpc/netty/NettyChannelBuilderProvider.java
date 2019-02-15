package io.vitess.client.grpc.netty;

import io.grpc.netty.NettyChannelBuilder;

public interface NettyChannelBuilderProvider {

  NettyChannelBuilder getChannelBuilder(String target);
}
