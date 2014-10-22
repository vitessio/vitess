package com.youtube.vitess.vtgate.rpcclient.gorpc;

import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;

import com.youtube.vitess.gorpc.Client;
import com.youtube.vitess.gorpc.Exceptions.ApplicationException;
import com.youtube.vitess.gorpc.Exceptions.GoRpcException;
import com.youtube.vitess.gorpc.Response;
import com.youtube.vitess.gorpc.codecs.bson.BsonClientCodecFactory;
import com.youtube.vitess.vtgate.Exceptions.ConnectionException;
import com.youtube.vitess.vtgate.Exceptions.DatabaseException;
import com.youtube.vitess.vtgate.rpcclient.RpcClient;

public class GoRpcClient implements RpcClient {
	static final Logger logger = LogManager.getLogger(GoRpcClient.class
			.getName());

	public static final String BSON_RPC_PATH = "/_bson_rpc_";
	private Client client;

	private GoRpcClient(Client client) {
		this.client = client;
	}

	@Override
	public Object begin() throws ConnectionException {
		Response response = call("VTGate.Begin", new BasicBSONObject());
		return response.getReply();
	}

	@SuppressWarnings("unchecked")
	@Override
	public Map<String, Object> executeKeyspaceIds(Map<String, Object> args)
			throws ConnectionException {
		BSONObject params = new BasicBSONObject();
		params.putAll(args);
		Response response = call("VTGate.ExecuteKeyspaceIds", params);
		BSONObject reply = (BSONObject) response.getReply();
		return reply.toMap();
	}

	@SuppressWarnings("unchecked")
	@Override
	public Map<String, Object> executeKeyRanges(Map<String, Object> args)
			throws ConnectionException {
		BSONObject params = new BasicBSONObject();
		params.putAll(args);
		Response response = call("VTGate.ExecuteKeyRanges", params);
		BSONObject reply = (BSONObject) response.getReply();
		return reply.toMap();
	}

	@SuppressWarnings("unchecked")
	@Override
	public Map<String, Object> streamExecuteKeyspaceIds(Map<String, Object> args)
			throws DatabaseException, ConnectionException {
		BSONObject params = new BasicBSONObject();
		params.putAll(args);
		Response response = streamCall("VTGate.StreamExecuteKeyspaceIds",
				params);
		BSONObject reply = (BSONObject) response.getReply();
		return reply.toMap();
	}

	@SuppressWarnings("unchecked")
	@Override
	public Map<String, Object> streamExecuteKeyRanges(Map<String, Object> args)
			throws DatabaseException, ConnectionException {
		BSONObject params = new BasicBSONObject();
		params.putAll(args);
		Response response = streamCall("VTGate.StreamExecuteKeyRanges",
				params);
		BSONObject reply = (BSONObject) response.getReply();
		return reply.toMap();
	}

	@SuppressWarnings("unchecked")
	@Override
	public Map<String, Object> streamNext() throws ConnectionException {
		Response response;
		try {
			response = client.streamNext();
		} catch (GoRpcException | ApplicationException e) {
			logger.error("vtgate exception", e);
			throw new ConnectionException("vtgate exception: " + e.getMessage());
		}

		if (response == null) {
			return null;
		}

		BSONObject reply = (BSONObject) response.getReply();
		return reply.toMap();
	}

	@SuppressWarnings("unchecked")
	@Override
	public Map<String, Object> batchExecuteKeyspaceIds(Map<String, Object> args)
			throws ConnectionException {
		BSONObject params = new BasicBSONObject();
		params.putAll(args);
		Response response = call("VTGate.ExecuteBatchKeyspaceIds", params);
		BSONObject reply = (BSONObject) response.getReply();
		return reply.toMap();
	}

	@Override
	public void commit(Object session) throws ConnectionException {
		call("VTGate.Commit", session);
	}

	@Override
	public void rollback(Object session) throws ConnectionException {
		call("VTGate.Rollback", session);
	}

	@Override
	public void close() throws ConnectionException {
		try {
			client.close();
		} catch (GoRpcException e) {
			logger.error("vtgate exception", e);
			throw new ConnectionException("vtgate exception: " + e.getMessage());
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public Map<String, Object> getMRSplits(Map<String, Object> args)
			throws ConnectionException {
		BSONObject params = new BasicBSONObject();
		params.putAll(args);
		Response response = call("VTGate.GetMRSplits",
				params);
		BSONObject reply = (BSONObject) response.getReply();
		return reply.toMap();

	}

	private Response call(String methodName, Object args)
			throws ConnectionException {
		try {
			Response response = client.call(methodName, args);
			return response;
		} catch (GoRpcException | ApplicationException e) {
			logger.error("vtgate exception", e);
			throw new ConnectionException("vtgate exception: " + e.getMessage());
		}
	}

	private Response streamCall(String methodName, Object args)
			throws ConnectionException {
		try {
			client.streamCall(methodName, args);
			return client.streamNext();
		} catch (GoRpcException | ApplicationException e) {
			logger.error("vtgate exception", e);
			throw new ConnectionException("vtgate exception: " + e.getMessage());
		}
	}

	public static class GoRpcClientFactory implements RpcClientFactory {

		@Override
		public RpcClient connect(String host, int port, int timeoutMs)
				throws ConnectionException {
			Client client;
			try {
				client = Client.dialHttp(host, port, BSON_RPC_PATH, timeoutMs,
						new BsonClientCodecFactory());
				return new GoRpcClient(client);
			} catch (GoRpcException e) {
				logger.error("vtgate connection exception: ", e);
				throw new ConnectionException(e.getMessage());
			}
		}
	}
}
