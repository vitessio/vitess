package com.youtube.vitess.vtgate.integration.util;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.junit.Assert;

import com.google.common.collect.ImmutableMap;
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
	 * uses a Python helper script to start and stop instances. Use the setUp
	 * flag to indicate setUp or teardown.
	 */
	public static void setupTestEnv(TestEnv testEnv, boolean setUp)
			throws Exception {
		String vtTop = System.getenv("VTTOP");
		if (vtTop == null) {
			Assert.fail("VTTOP is not set");
		}

		List<String> command = new ArrayList<String>();
		command.add("python");
		command.add(vtTop + "/test/java_vtgate_test_helper.py");
		command.add("--shards");
		command.add(testEnv.getShardNames());
		command.add("--tablet-config");
		command.add(testEnv.getTabletConfig());
		command.add("--keyspace");
		command.add(testEnv.keyspace);
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
					testEnv.port = map.get("port");
					return;
				} catch (JsonSyntaxException e) {
				}
			}
			Assert.fail("setup script failed to parse vtgate port");
		}
	}

	public static void insertRows(TestEnv testEnv, int startId, int count)
			throws ConnectionException, DatabaseException {
		insertRows(testEnv, startId, count, new Date());
	}

	public static void insertRows(TestEnv testEnv, int startId, int count,
			Date date) throws ConnectionException, DatabaseException {
		VtGate vtgate = VtGate.connect("localhost:" + testEnv.port, 0);

		vtgate.begin();
		String insertSql = "insert into vtgate_test "
				+ "(id, name, age, percent, datetime_col, timestamp_col, date_col, time_col, keyspace_id) "
				+ "values (:id, :name, :age, :percent, :datetime_col, :timestamp_col, :date_col, :time_col, :keyspace_id)";
		for (int i = startId; i < startId + count; i++) {
			KeyspaceId kid = testEnv.getAllKeyspaceIds().get(
					i % testEnv.getAllKeyspaceIds().size());
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
					testEnv.keyspace, "master")
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
	public static void insertRowsInShard(TestEnv testEnv, String shardName,
			int count) throws DatabaseException, ConnectionException {
		VtGate vtgate = VtGate.connect("localhost:" + testEnv.port, 0);
		vtgate.begin();
		String insertSql = "insert into vtgate_test "
				+ "(id, name, keyspace_id) "
				+ "values (:id, :name, :keyspace_id)";
		List<KeyspaceId> kids = testEnv.getKeyspaceIds(shardName);
		Random random = new Random();
		for (int i = 0; i < count; i++) {
			KeyspaceId kid = kids.get(i % kids.size());
			Map<String, Object> bindVars = new ImmutableMap.Builder<String, Object>()
					.put("id", random.nextInt())
					.put("name", "name_" + i)
					.put("keyspace_id", kid.getId())
					.build();
			Query query = new QueryBuilder(insertSql,
					testEnv.keyspace, "master")
					.withBindVars(bindVars)
					.withAddedKeyspaceId(kid)
					.build();
			vtgate.execute(query);
		}
		vtgate.commit();
		vtgate.close();
	}

	public static void truncateTable(TestEnv testEnv) throws Exception {
		VtGate vtgate = VtGate.connect("localhost:" + testEnv.port, 0);
		vtgate.begin();
		vtgate.execute(new QueryBuilder("delete from vtgate_test",
				testEnv.keyspace, "master").withKeyspaceIds(
				testEnv.getAllKeyspaceIds()).build());
		vtgate.commit();
		vtgate.close();
	}
}
