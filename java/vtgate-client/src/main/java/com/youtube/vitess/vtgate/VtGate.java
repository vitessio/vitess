package com.youtube.vitess.vtgate;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.mapreduce.InputSplit;

import com.google.common.net.HostAndPort;
import com.youtube.vitess.gorpc.Exceptions.GoRpcException;
import com.youtube.vitess.vtgate.Exceptions.ConnectionException;
import com.youtube.vitess.vtgate.Exceptions.DatabaseException;
import com.youtube.vitess.vtgate.cursor.Cursor;
import com.youtube.vitess.vtgate.cursor.CursorImpl;
import com.youtube.vitess.vtgate.cursor.StreamCursor;
import com.youtube.vitess.vtgate.hadoop.VitessInputSplit;
import com.youtube.vitess.vtgate.rpcclient.RpcClient;
import com.youtube.vitess.vtgate.rpcclient.gorpc.GoRpcClient.GoRpcClientFactory;

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

	private RpcClient client;
	private Object session;

	/**
	 * Opens connection to a VtGate server. Connection remains open until
	 * close() is called.
	 * 
	 * @param addresses
	 *            comma separated list of host:port pairs
	 * @params timeoutMs connection timeout in ms, 0 for no timeout
	 * @throws ConnectionException
	 * @throws GoRpcException
	 */
	public static VtGate connect(String addresses, int timeoutMs)
			throws ConnectionException {
		List<String> addressList = Arrays.asList(addresses.split(","));
		int index = new Random().nextInt(addressList.size());
		HostAndPort hostAndPort = HostAndPort
				.fromString(addressList.get(index));
		RpcClient client = new GoRpcClientFactory().connect(
				hostAndPort.getHostText(), hostAndPort.getPort(), timeoutMs);
		return new VtGate(client);
	}

	private VtGate(RpcClient client) {
		this.client = client;
	}

	public void begin() throws ConnectionException {
		session = client.begin();
	}

	public Cursor execute(Query query) throws DatabaseException,
			ConnectionException {
		Map<String, Object> params = new HashMap<>();
		query.populate(params);
		if (session != null) {
			params.put("Session", session);
		}

		Map<String, Object> reply = null;
		if (query.getKeyspaceIds() != null) {
			if (query.isStreaming()) {
				reply = client.streamExecuteKeyspaceIds(params);
			} else {
				reply = client.executeKeyspaceIds(params);
			}
		} else {
			if (query.isStreaming()) {
				reply = client.streamExecuteKeyRanges(params);
			} else {
				reply = client.executeKeyRanges(params);
			}
		}

		if (reply.containsKey("Error")) {
			byte[] err = (byte[]) reply.get("Error");
			if (err.length > 0) {
				throw new DatabaseException(new String(err));
			}
		}
		Map<String, Object> result = (Map<String, Object>) reply.get("Result");
		QueryResult qr = QueryResult.parse(result);
		if (query.isStreaming()) {
			return new StreamCursor(qr, client);
		}

		if (reply.containsKey("Session")) {
			session = reply.get("Session");
		}
		return new CursorImpl(qr);
	}

	/**
	 * Get {@link InputSplit}s for a MapReduce job using the specified table as
	 * input. Use splitsPerShard to control how many splits to generate per
	 * shard. Throws {@link DatabaseException} if an rdonly instance is not
	 * available.
	 */
	public List<InputSplit> getMRSplits(String keyspace, String table,
			List<String> columns, int splitsPerShard)
			throws ConnectionException, DatabaseException {
		Map<String, Object> params = new HashMap<>();
		params.put("Keyspace", keyspace);
		if (!columns.contains(KeyspaceId.COL_NAME)) {
			columns.add(KeyspaceId.COL_NAME);
		}
		String sql = "select " + StringUtils.join(columns, ',') + " from "
				+ table;
		Map<String, Object> query = new HashMap<>();
		query.put("Sql", sql);
		params.put("Query", query);
		params.put("SplitsPerShard", splitsPerShard);
		Map<String, Object> reply = client.getMRSplits(params);
		if (reply.containsKey("Error")) {
			byte[] err = (byte[]) reply.get("Error");
			if (err.length > 0) {
				throw new DatabaseException(new String(err));
			}
		}
		List<Map<String, Object>> result = (List<Map<String, Object>>) reply
				.get("Splits");
		List<InputSplit> splits = new LinkedList<>();
		for (Map<String, Object> split : result) {
			splits.add(VitessInputSplit.from(split));
		}

		return splits;
	}

	public void commit() throws ConnectionException {
		try {
			client.commit(session);
		} finally {
			session = null;
		}
	}

	public void rollback() throws ConnectionException {
		try {
			client.rollback(session);
		} finally {
			session = null;
		}
	}

	public void close() throws ConnectionException {
		if (session != null) {
			rollback();
		}
		client.close();
	}
}
