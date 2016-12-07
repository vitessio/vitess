package com.youtube.vitess.client;

import com.youtube.vitess.client.grpc.tls.TlsOptions;

import java.net.InetSocketAddress;

/**
 * RpcClientFactory creates a concrete RpcClient.
 */
public interface RpcClientFactory {

  RpcClient create(Context ctx, InetSocketAddress address);

  RpcClient createTls(Context ctx, InetSocketAddress address, TlsOptions tlsOptions);

}

