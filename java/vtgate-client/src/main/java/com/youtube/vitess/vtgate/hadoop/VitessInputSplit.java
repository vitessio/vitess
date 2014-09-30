package com.youtube.vitess.vtgate.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;

import com.google.gson.Gson;
import com.youtube.vitess.vtgate.KeyRange;
import com.youtube.vitess.vtgate.KeyspaceId;
import com.youtube.vitess.vtgate.Query;
import com.youtube.vitess.vtgate.Query.QueryBuilder;

/**
 * {@link VitessInputSplit} has a Query corresponding to a set of rows from a
 * Vitess table input source. Locations point to the same VtGate hosts used to
 * fetch the splits. Length information is only approximate.
 *
 */
public class VitessInputSplit extends InputSplit implements Writable {
	private String[] locations;
	private Query query;
	private long length;
	private final Gson gson = new Gson();

	public VitessInputSplit(Query query, long length) {
		this.query = query;
		this.length = length;
	}

	public VitessInputSplit() {

	}

	public Query getQuery() {
		return query;
	}

	public void setLocations(String[] locations) {
		this.locations = locations;
	}

	@Override
	public long getLength() throws IOException, InterruptedException {
		return length;
	}

	@Override
	public String[] getLocations() throws IOException, InterruptedException {
		return locations;
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		query = gson.fromJson(input.readUTF(), Query.class);
		length = input.readLong();
	}

	@Override
	public void write(DataOutput output) throws IOException {
		output.writeUTF(gson.toJson(query));
		output.writeLong(length);
	}

	/**
	 * Parse the result from VtGate RPC and create {@link VitessInputSplit}
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static VitessInputSplit from(Map<String, Object> result) {
		Map<String, Object> query = (Map<String, Object>) result
				.get("Query");
		String sql = new String((byte[]) query.get("Sql"));
		Map<String, Object> bindVars = (Map<String, Object>) query
				.get("BindVariables");
		String keyspace = new String((byte[]) query.get("Keyspace"));
		String tabletType = new String((byte[]) query.get("TabletType"));
		List<KeyRange> keyranges = new ArrayList<>();
		for (Object o : (List) query.get("KeyRanges")) {
			BSONObject keyrange = (BasicBSONObject) o;
			String start = Hex.encodeHexString((byte[]) keyrange
					.get("Start"));
			String end = Hex.encodeHexString((byte[]) keyrange.get("End"));
			KeyRange kr = new KeyRange(KeyspaceId.valueOf(start),
					KeyspaceId.valueOf(end));
			keyranges.add(kr);
		}

		Query q = new QueryBuilder(sql, keyspace, tabletType)
				.withKeyRanges(keyranges)
				.withBindVars(bindVars)
				.withStreaming(true)
				.build();
		long size = (long) result.get("Size");

		return new VitessInputSplit(q, size);
	}
}
