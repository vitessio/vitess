package io.vitess.client.grpc.netty;

import io.grpc.netty.NettyChannelBuilder;
import io.vitess.client.grpc.RetryingInterceptor;
import io.vitess.client.grpc.RetryingInterceptorConfig;

public class DefaultChannelBuilderProvider implements NettyChannelBuilderProvider {
  private final RetryingInterceptorConfig config;

  public DefaultChannelBuilderProvider(RetryingInterceptorConfig config) {
    this.config = config;
  }

  @Override
  public NettyChannelBuilder getChannelBuilder(String target) {
    return NettyChannelBuilder.forTarget(target)
        .intercept(new RetryingInterceptor(config));
  }
}
