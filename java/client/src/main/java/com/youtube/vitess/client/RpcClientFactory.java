package com.youtube.vitess.client;

import java.net.SocketAddress;

/**
 * RpcClientFactory creates a concrete RpcClient.
 */
public interface RpcClientFactory {
  RpcClient create(Context ctx, SocketAddress address);
}
