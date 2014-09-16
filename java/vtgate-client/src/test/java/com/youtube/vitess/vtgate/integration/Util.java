package com.youtube.vitess.vtgate.integration;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Assert;

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.youtube.vitess.vtgate.Exceptions.ConnectionException;
import com.youtube.vitess.vtgate.Exceptions.DatabaseException;
import com.youtube.vitess.vtgate.Query;
import com.youtube.vitess.vtgate.Query.QueryBuilder;
import com.youtube.vitess.vtgate.VtGate;

public class Util {
	static VtGateParams runVtGate(boolean setUp) throws Exception {
		String vtTop = System.getenv("VTTOP");
		if (vtTop == null) {
			Assert.fail("VTTOP is not set");
		}

		List<String> command = new ArrayList<String>();
		command.add("python");
		command.add(System.getenv("VTTOP") + "/test/java_vtgate_test_helper.py");
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
			// Fetch VtGate connection params written to stdout by setup script
			String line;
			while ((line = br.readLine()) != null) {
				try {
					return new Gson().fromJson(line, VtGateParams.class);
				} catch (JsonSyntaxException e) {
				}
			}
		}

		return null;
	}

	static void insertRows(VtGateParams params, int startId, int count)
			throws ConnectionException, DatabaseException {
		VtGate vtgate = VtGate.connect("localhost:" + params.port);

		vtgate.begin();
		String insertSql = "insert into vtgate_test "
				+ "(id, name, age, percent, keyspace_id) "
				+ "values (:id, :name, :age, :percent, :keyspace_id)";
		for (int i = startId; i < startId + count; i++) {
			String kid = params.getKeyspaceIds().get(
					i % params.getKeyspaceIds().size());
			Map<String, Object> bindVars = new ImmutableMap.Builder<String, Object>()
					.put("id", i)
					.put("name", "name_" + i)
					.put("age", i * 2)
					.put("percent", new Double(i / 100.0))
					.put("keyspace_id", kid)
					.build();
			Query query = new QueryBuilder(insertSql,
					params.keyspace_name, "master")
					.withBindVars(bindVars)
					.withAddedKeyspaceId(kid)
					.build();
			vtgate.execute(query);
		}
		vtgate.commit();
	}

	static void truncateTable(VtGateParams params) throws Exception {
		VtGate vtgate = VtGate.connect("localhost:" + params.port);
		vtgate.begin();
		vtgate.execute(new QueryBuilder("delete from vtgate_test",
				params.keyspace_name, "master").withKeyspaceIds(
				params.getKeyspaceIds()).build());
		vtgate.commit();
		vtgate.close();
	}

	class VtGateParams {
		String keyspace_name;
		int port;
		Map<String, List<String>> shard_kid_map;
		List<String> kids;

		List<String> getKeyspaceIds() {
			if (kids != null) {
				return kids;
			}

			kids = new ArrayList<>();
			for (List<String> ids : shard_kid_map.values()) {
				kids.addAll(ids);
			}

			return kids;
		}
	}
}
