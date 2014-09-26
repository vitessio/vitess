package com.youtube.vitess.vtgate.integration.util;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.junit.Assert;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import com.youtube.vitess.vtgate.Exceptions.ConnectionException;
import com.youtube.vitess.vtgate.Exceptions.DatabaseException;
import com.youtube.vitess.vtgate.KeyspaceId;
import com.youtube.vitess.vtgate.Query;
import com.youtube.vitess.vtgate.Query.QueryBuilder;
import com.youtube.vitess.vtgate.VtGate;

public class Util {
	/**
	 * Setup MySQL, Vttablet and VtGate instances required for the tests. This
	 * uses a Python helper script to start and stop instances.
	 */
	public static VtGateParams runVtGate(boolean setUp) throws Exception {
		String vtTop = System.getenv("VTTOP");
		if (vtTop == null) {
			Assert.fail("VTTOP is not set");
		}

		VtGateParams params = getParams();
		List<String> command = new ArrayList<String>();
		command.add("python");
		command.add(vtTop + "/test/java_vtgate_test_helper.py");
		command.add("--shards");
		command.add(params.getShardNames());
		command.add("--tablet-config");
		command.add(params.getTabletConfig());
		command.add("--keyspace");
		command.add(params.keyspace_name);
		if (setUp) {
			command.add("setup");
		} else {
			command.add("teardown");
		}

		ProcessBuilder pb = new ProcessBuilder(command);
		pb.redirectErrorStream(true);
		Process p = pb.start();
		BufferedReader br = new BufferedReader(new InputStreamReader(
				p.getInputStream()));

		int exitValue = p.waitFor();
		if (exitValue != 0) {
			Assert.fail("script failed, setUp:" + setUp);
		}

		if (setUp) {
			// The port for VtGate is dynamically assigned and written to
			// stdout as a JSON string.
			String line;
			while ((line = br.readLine()) != null) {
				try {
					Type mapType = new TypeToken<Map<String, Integer>>() {
					}.getType();
					Map<String, Integer> map = new Gson().fromJson(line,
							mapType);
					params.port = map.get("port");
					return params;
				} catch (JsonSyntaxException e) {
				}
			}
		}

		return null;
	}

	public static void insertRows(VtGateParams params, int startId, int count)
			throws ConnectionException, DatabaseException {
		insertRows(params, startId, count, new Date());
	}

	public static void insertRows(VtGateParams params, int startId, int count,
			Date date) throws ConnectionException, DatabaseException {
		VtGate vtgate = VtGate.connect("localhost:" + params.port, 0);

		vtgate.begin();
		String insertSql = "insert into vtgate_test "
				+ "(id, name, age, percent, datetime_col, timestamp_col, date_col, time_col, keyspace_id) "
				+ "values (:id, :name, :age, :percent, :datetime_col, :timestamp_col, :date_col, :time_col, :keyspace_id)";
		for (int i = startId; i < startId + count; i++) {
			KeyspaceId kid = params.getAllKeyspaceIds().get(
					i % params.getAllKeyspaceIds().size());
			Map<String, Object> bindVars = new ImmutableMap.Builder<String, Object>()
					.put("id", i)
					.put("name", "name_" + i)
					.put("age", i * 2)
					.put("percent", new Double(i / 100.0))
					.put("keyspace_id", kid.getId())
					.put("datetime_col", date)
					.put("timestamp_col", date)
					.put("date_col", date)
					.put("time_col", date)
					.build();
			Query query = new QueryBuilder(insertSql,
					params.keyspace_name, "master")
					.withBindVars(bindVars)
					.withAddedKeyspaceId(kid)
					.build();
			vtgate.execute(query);
		}
		vtgate.commit();
		vtgate.close();
	}

	/**
	 * Insert rows to a specific shard using ExecuteKeyspaceIds
	 */
	public static void insertRowsInShard(VtGateParams params, String shardName,
			int count) throws DatabaseException, ConnectionException {
		VtGate vtgate = VtGate.connect("localhost:" + params.port, 0);
		vtgate.begin();
		String insertSql = "insert into vtgate_test "
				+ "(id, name, keyspace_id) "
				+ "values (:id, :name, :keyspace_id)";
		List<KeyspaceId> kids = params.getKeyspaceIds(shardName);
		Random random = new Random();
		for (int i = 0; i < count; i++) {
			KeyspaceId kid = kids.get(i % kids.size());
			Map<String, Object> bindVars = new ImmutableMap.Builder<String, Object>()
					.put("id", random.nextInt())
					.put("name", "name_" + i)
					.put("keyspace_id", kid.getId())
					.build();
			Query query = new QueryBuilder(insertSql,
					params.keyspace_name, "master")
					.withBindVars(bindVars)
					.withAddedKeyspaceId(kid)
					.build();
			vtgate.execute(query);
		}
		vtgate.commit();
		vtgate.close();
	}

	public static void truncateTable(VtGateParams params) throws Exception {
		VtGate vtgate = VtGate.connect("localhost:" + params.port, 0);
		vtgate.begin();
		vtgate.execute(new QueryBuilder("delete from vtgate_test",
				params.keyspace_name, "master").withKeyspaceIds(
				params.getAllKeyspaceIds()).build());
		vtgate.commit();
		vtgate.close();
	}

	/**
	 * Create a VtGate env with two shards
	 */
	private static VtGateParams getParams() {
		Map<String, List<String>> shardKidMap = new HashMap<>();
		shardKidMap.put("-80",
				Lists.newArrayList("527875958493693904", "626750931627689502",
						"345387386794260318"));
		shardKidMap.put("80-", Lists.newArrayList("9767889778372766922",
				"9742070682920810358", "10296850775085416642"));
		VtGateParams env = new VtGateParams(shardKidMap, "test_keyspace");
		env.addTablet("replica", 1);
		return env;
	}
}
