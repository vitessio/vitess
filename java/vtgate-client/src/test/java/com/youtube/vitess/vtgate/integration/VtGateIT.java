package com.youtube.vitess.vtgate.integration;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.youtube.vitess.vtgate.Cursor;
import com.youtube.vitess.vtgate.Query;
import com.youtube.vitess.vtgate.Row;
import com.youtube.vitess.vtgate.VtGate;
import com.youtube.vitess.vtgate.Exceptions.ConnectionException;
import com.youtube.vitess.vtgate.Exceptions.DatabaseException;
import com.youtube.vitess.vtgate.Query.QueryBuilder;
import com.youtube.vitess.vtgate.Row.Cell;

public class VtGateIT {

	static VtGateParams params;

	@BeforeClass
	public static void setUpVtGate() throws Exception {
		runVtGate(true);
	}

	@AfterClass
	public static void tearDownVtGate() throws Exception {
		runVtGate(false);
	}

	private static void runVtGate(boolean setUp) throws Exception {
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
					params = new Gson().fromJson(line, VtGateParams.class);
				} catch (JsonSyntaxException e) {
				}
			}
		}

	}

	@Before
	public void truncateTable() throws Exception {
		VtGate vtgate = VtGate.connect("localhost:" + params.port);
		vtgate.begin();
		vtgate.execute(new QueryBuilder("delete from vtgate_test",
				params.keyspace_name, "master").withKeyspaceIds(
				params.getKeyspaceIds()).build());
		vtgate.commit();
		vtgate.close();
	}

	@Test
	public void testDMLOutsideTransaction() throws ConnectionException {
		VtGate vtgate = VtGate.connect("localhost:" + params.port);
		String deleteSql = "delete from vtgate_test";
		try {
			vtgate.execute(new QueryBuilder(deleteSql, params.keyspace_name,
					"master").withAddedKeyspaceId(
					params.getKeyspaceIds().get(0)).build());
			Assert.fail("did not raise DatabaseException");
		} catch (DatabaseException e) {
			Assert.assertTrue(e.getMessage().contains("not_in_tx"));
		}
	}

	@Test
	public void testReadsAndWrites() throws Exception {
		VtGate vtgate = VtGate.connect("localhost:" + params.port);
		String selectSql = "select * from vtgate_test";
		Query allRowsQuery = new QueryBuilder(selectSql,
				params.keyspace_name, "master").withKeyspaceIds(
				params.getKeyspaceIds()).build();
		Cursor cursor = vtgate.execute(allRowsQuery);
		Assert.assertEquals(0, cursor.getRowsAffected());
		Assert.assertEquals(0, cursor.getLastRowId());
		Assert.assertFalse(cursor.hasNext());

		String insertSql = "insert into vtgate_test "
				+ "(id, name, age, percent, keyspace_id) "
				+ "values (:id, :name, :age, :percent, :keyspace_id)";

		vtgate.begin();
		for (int i = 1000; i < 1100; i++) {
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

		cursor = vtgate.execute(allRowsQuery);
		Assert.assertEquals(100, cursor.getRowsAffected());
		Assert.assertEquals(0, cursor.getLastRowId());
		Assert.assertTrue(cursor.hasNext());

		Row row = cursor.next();

		Cell idCell = row.next();
		Assert.assertEquals("id", idCell.getName());
		Assert.assertEquals(BigInteger.class, idCell.getType());
		Assert.assertEquals(BigInteger.valueOf(1000),
				row.getBigInt(idCell.getName()));

		Cell nameCell = row.next();
		Assert.assertEquals("name", nameCell.getName());
		Assert.assertEquals(String.class, nameCell.getType());
		Assert.assertEquals("name_1000", row.getString(nameCell.getName()));

		Cell ageCell = row.next();
		Assert.assertEquals("age", ageCell.getName());
		Assert.assertEquals(Integer.class, ageCell.getType());
		Assert.assertEquals(Integer.valueOf(2000),
				row.getInt(ageCell.getName()));
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
