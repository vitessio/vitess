package io.vitess.client;

import io.vitess.client.grpc.tls.TlsOptions;

/**
 * RpcClientFactory creates a concrete RpcClient.
 */
public interface RpcClientFactory {

  RpcClient create(Context ctx, String target);

  RpcClient createTls(Context ctx, String target, TlsOptions tlsOptions);

}

