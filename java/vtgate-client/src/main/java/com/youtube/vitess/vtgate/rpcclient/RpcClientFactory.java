package com.youtube.vitess.vtgate.rpcclient;

import com.youtube.vitess.vtgate.Exceptions.ConnectionException;

public interface RpcClientFactory {
  RpcClient create(String address, int timeoutMs) throws ConnectionException;
}
