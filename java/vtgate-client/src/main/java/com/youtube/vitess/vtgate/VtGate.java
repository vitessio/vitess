package com.youtube.vitess.vtgate;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;

import com.google.common.net.HostAndPort;
import com.youtube.vitess.gorpc.Client;
import com.youtube.vitess.gorpc.Exceptions.ApplicationException;
import com.youtube.vitess.gorpc.Exceptions.GoRpcException;
import com.youtube.vitess.gorpc.Response;
import com.youtube.vitess.gorpc.codecs.bson.BsonClientCodecFactory;
import com.youtube.vitess.vtgate.Exceptions.ConnectionException;
import com.youtube.vitess.vtgate.Exceptions.DatabaseException;

/**
 * A single threaded VtGate client
 * 
 * Usage:
 * 
 * <pre>
 * VtGate vtGate = VtGate.connect(addresses);
 * Query query = new QueryBuilder()...add params...build();
 * Cursor cursor = vtGate.execute(query);
 * for(Row row : cursor) {
 * 		processRow(row);
 * }
 * 
 * For DMLs
 * vtgate.begin();
 * Query query = new QueryBuilder()...add params...build();
 * vtgate.execute(query);
 * vtgate.commit();
 * 
 * vtgate.close();
 * </pre>
 *
 * TODO: Currently only ExecuteKeyspaceIds is supported, add the rest.
 */
public class VtGate {
	static final Logger logger = LogManager.getLogger(VtGate.class.getName());
	public static final String EXECUTE_KEYSPACE_IDS = "VTGate.ExecuteKeyspaceIds";
	public static final String EXECUTE_KEYRANGES = "VTGate.ExecuteKeyRanges";
	public static final String BSON_RPC_PATH = "/_bson_rpc_";

	private Client rpcClient;
	private BSONObject session;

	/**
	 * Opens connection to a VtGate server. Connection remains open until
	 * close() is called.
	 * 
	 * @param addresses
	 *            comma separated list of host:port pairs
	 * @throws ConnectionException
	 * @throws GoRpcException
	 */
	public static VtGate connect(String addresses) throws ConnectionException {
		List<String> addressList = Arrays.asList(addresses.split(","));
		int index = new Random().nextInt(addressList.size());
		HostAndPort hostAndPort = HostAndPort
				.fromString(addressList.get(index));
		try {
			Client rpcClient = Client.dialHttp(hostAndPort.getHostText(),
					hostAndPort.getPort(), BSON_RPC_PATH,
					new BsonClientCodecFactory());
			return new VtGate(rpcClient);
		} catch (GoRpcException e) {
			throw new ConnectionException("");
		}
	}

	private VtGate(Client rpcClient) {
		this.rpcClient = rpcClient;
	}

	public void begin() throws ConnectionException {
		try {
			Response response = rpcClient.call("VTGate.Begin",
					new BasicBSONObject());
			session = (BSONObject) response.getReply();
		} catch (GoRpcException | ApplicationException e) {
			logger.error("vtgate exception", e);
			throw new ConnectionException("vtgate exception: " + e.getMessage());
		}
	}

	public Cursor execute(Query query) throws DatabaseException,
			ConnectionException {
		String methodName = getMethodName(query);
		BSONObject args = buildRequestArgs(query);
		Response response = null;
		try {
			response = rpcClient.call(methodName, args);
		} catch (ApplicationException | GoRpcException e) {
			logger.error("rpc exception for:" + methodName, e);
			throw new ConnectionException("vtgate exception: " + e.getMessage());
		}
		BSONObject reply = (BSONObject) response.getReply();
		if (reply.containsField("Error")) {
			byte[] err = (byte[]) reply.get("Error");
			if (err.length > 0) {
				throw new DatabaseException(new String(err));
			}
		}
		BSONObject result = (BSONObject) reply.get("Result");
		if (reply.containsField("Session")) {
			session = (BSONObject) reply.get("Session");
		}
		QueryResult qr = QueryResult.parse(result);
		Cursor cursor = new Cursor(qr);
		return cursor;
	}

	public void commit() throws ConnectionException {
		try {
			rpcClient.call("VTGate.Commit", session);
		} catch (GoRpcException | ApplicationException e) {
			logger.error("vtgate exception", e);
			throw new ConnectionException("vtgate exception: " + e.getMessage());
		} finally {
			session = null;
		}
	}

	public void rollback() throws ConnectionException {
		try {
			rpcClient.call("VTGate.Rollback", session);
		} catch (GoRpcException | ApplicationException e) {
			logger.error("vtgate exception", e);
			throw new ConnectionException("vtgate exception: " + e.getMessage());
		} finally {
			session = null;
		}
	}

	public void close() throws ConnectionException {
		try {
			if (session != null) {
				rollback();
			}
			this.rpcClient.close();
		} catch (GoRpcException e) {
			logger.error("vtgate exception", e);
			throw new ConnectionException("vtgate exception: " + e.getMessage());
		}
	}

	private BSONObject buildRequestArgs(Query query) {
		BSONObject args = new BasicBSONObject();
		args.put("Sql", query.getSql());
		args.put("BindVariables", query.getBindVars());
		args.put("Keyspace", query.getKeyspace());
		args.put("TabletType", query.getTabletType());
		if (query.getKeyspaceIds() != null) {
			args.put("KeyspaceIds", query.getKeyspaceIds());
		} else {
			// TODO: Set keyranges query
		}
		if (session != null) {
			args.put("Session", session);
		}
		return args;
	}

	private String getMethodName(Query query) {
		return query.getKeyspaceIds() != null ? EXECUTE_KEYSPACE_IDS
				: EXECUTE_KEYRANGES;
	}

}
