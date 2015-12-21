package com.youtube.vitess.client;

import java.net.InetSocketAddress;

/**
 * RpcClientFactory creates a concrete RpcClient.
 */
public interface RpcClientFactory {
  RpcClient create(Context ctx, InetSocketAddress address);
}
