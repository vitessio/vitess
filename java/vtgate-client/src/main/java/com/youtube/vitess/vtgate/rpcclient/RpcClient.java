package com.youtube.vitess.vtgate.rpcclient;

import java.util.Map;

import com.youtube.vitess.vtgate.Exceptions.ConnectionException;
import com.youtube.vitess.vtgate.Exceptions.DatabaseException;

public interface RpcClient {

	public Object begin() throws ConnectionException;

	public Map<String, Object> executeKeyspaceIds(Map<String, Object> args)
			throws DatabaseException, ConnectionException;

	public Map<String, Object> executeKeyRanges(Map<String, Object> args)
			throws DatabaseException, ConnectionException;

	public Map<String, Object> streamExecuteKeyspaceIds(Map<String, Object> args)
			throws DatabaseException, ConnectionException;

	public Map<String, Object> streamNext() throws ConnectionException;

	public void commit(Object session) throws ConnectionException;

	public void rollback(Object session) throws ConnectionException;

	public void close() throws ConnectionException;

	public static interface RpcClientFactory {
		public RpcClient connect(String host, int port, int timeoutMs)
				throws ConnectionException;
	}
}