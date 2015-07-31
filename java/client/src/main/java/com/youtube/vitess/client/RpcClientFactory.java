package com.youtube.vitess.client;

import org.joda.time.Duration;

import java.net.InetAddress;

/**
 * RpcClientFactory creates a concrete RpcClient.
 */
public interface RpcClientFactory {
  RpcClient create(InetAddress address, Duration deadline);
}
