package com.youtube.vitess.vtgate.integration;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.youtube.vitess.vtgate.Exceptions.ConnectionException;
import com.youtube.vitess.vtgate.KeyRange;
import com.youtube.vitess.vtgate.KeyspaceId;
import com.youtube.vitess.vtgate.Query;
import com.youtube.vitess.vtgate.Query.QueryBuilder;
import com.youtube.vitess.vtgate.Row;
import com.youtube.vitess.vtgate.VtGate;
import com.youtube.vitess.vtgate.cursor.Cursor;
import com.youtube.vitess.vtgate.cursor.StreamCursor;
import com.youtube.vitess.vtgate.integration.util.TestEnv;
import com.youtube.vitess.vtgate.integration.util.Util;

/**
 * Test cases for streaming queries in VtGate
 *
 */
public class StreamingVtGateIT {
	public static TestEnv testEnv = VtGateIT.getTestEnv();

	@BeforeClass
	public static void setUpVtGate() throws Exception {
		Util.setupTestEnv(testEnv, true);
	}

	@AfterClass
	public static void tearDownVtGate() throws Exception {
		Util.setupTestEnv(testEnv, false);
	}

	@Before
	public void truncateTable() throws Exception {
		Util.truncateTable(testEnv);
	}

	@Test
	public void testStreamCursorType() throws Exception {
		VtGate vtgate = VtGate.connect("localhost:" + testEnv.port, 0);
		String selectSql = "select * from vtgate_test";
		Query allRowsQuery = new QueryBuilder(selectSql,
				testEnv.keyspace, "master").withKeyspaceIds(
				testEnv.getAllKeyspaceIds()).withStreaming(true).build();
		Cursor cursor = vtgate.execute(allRowsQuery);
		Assert.assertEquals(StreamCursor.class, cursor.getClass());
		vtgate.close();
	}

	/**
	 * Test StreamExecuteKeyspaceIds query on a single shard
	 */
	@Test
	public void testStreamExecuteKeyspaceIds() throws Exception {
		// Insert 100 rows per shard
		int rowCount = 100;
		for (String shardName : testEnv.shardKidMap.keySet()) {
			Util.insertRowsInShard(testEnv, shardName, rowCount);
		}
		VtGate vtgate = VtGate.connect("localhost:" + testEnv.port, 0);
		for (String shardName : testEnv.shardKidMap.keySet()) {
			// Do a self join within each shard
			String selectSql = "select A.* from vtgate_test A join vtgate_test B";
			Query query = new QueryBuilder(selectSql, testEnv.keyspace,
					"master")
					.withKeyspaceIds(testEnv.getKeyspaceIds(shardName))
					.withStreaming(true)
					.build();
			Cursor cursor = vtgate.execute(query);
			int count = 0;
			while (cursor.hasNext()) {
				cursor.next();
				count++;
			}
			// Assert 10000 rows fetched
			Assert.assertEquals(rowCount * rowCount, count);
		}
		vtgate.close();
	}

	/**
	 * Same as testStreamExecuteKeyspaceIds but for StreamExecuteKeyRanges
	 */
	@Test
	public void testStreamExecuteKeyRanges() throws Exception {
		VtGate vtgate = VtGate.connect("localhost:" + testEnv.port, 0);
		int rowCount = 100;
		for (String shardName : testEnv.shardKidMap.keySet()) {
			Util.insertRowsInShard(testEnv, shardName, rowCount);
		}
		for (String shardName : testEnv.shardKidMap.keySet()) {
			List<KeyspaceId> kids = testEnv.getKeyspaceIds(shardName);
			KeyRange kr = new KeyRange(Collections.min(kids),
					Collections.max(kids));
			String selectSql = "select A.* from vtgate_test A join vtgate_test B";
			Query query = new QueryBuilder(selectSql, testEnv.keyspace,
					"master").withAddedKeyRange(kr)
					.withStreaming(true)
					.build();
			Cursor cursor = vtgate.execute(query);
			int count = 0;
			while (cursor.hasNext()) {
				cursor.next();
				count++;
			}
			Assert.assertEquals(rowCount * rowCount, count);
		}
		vtgate.close();
	}

	/**
	 * Test scatter streaming queries fetch rows from all shards
	 */
	@Test
	public void testScatterStreamingQuery() throws Exception {
		// Insert 100 rows per shard
		int rowCount = 100;
		for (String shardName : testEnv.shardKidMap.keySet()) {
			Util.insertRowsInShard(testEnv, shardName, rowCount);
		}
		// Run a self join query
		String selectSql = "select A.* from vtgate_test A join vtgate_test B";
		Query query = new QueryBuilder(selectSql, testEnv.keyspace,
				"master")
				.withKeyspaceIds(testEnv.getAllKeyspaceIds())
				.withStreaming(true)
				.build();
		VtGate vtgate = VtGate.connect("localhost:" + testEnv.port, 0);
		Cursor cursor = vtgate.execute(query);
		int count = 0;
		for (Row row : cursor) {
			count++;
		}
		// Check 10000 rows were fetched from each shard
		Assert.assertEquals(2 * rowCount * rowCount, count);
		vtgate.close();
	}

	@Test
	@Ignore("currently failing as vtgate doesn't set the error")
	public void testStreamingWrites() throws Exception {
		VtGate vtgate = VtGate.connect("localhost:" + testEnv.port, 0);

		vtgate.begin();
		String insertSql = "insert into vtgate_test "
				+ "(id, name, age, percent, keyspace_id) "
				+ "values (:id, :name, :age, :percent, :keyspace_id)";
		KeyspaceId kid = testEnv.getAllKeyspaceIds().get(0);
		Map<String, Object> bindVars = new ImmutableMap.Builder<String, Object>()
				.put("id", 1)
				.put("name", "name_" + 1)
				.put("age", 2)
				.put("percent", new Double(1.0))
				.put("keyspace_id", kid)
				.build();
		Query query = new QueryBuilder(insertSql,
				testEnv.keyspace, "master")
				.withBindVars(bindVars)
				.withAddedKeyspaceId(kid)
				.withStreaming(true)
				.build();
		vtgate.execute(query);
		vtgate.commit();
		vtgate.close();
	}

	/**
	 * Test no new queries are allowed when client is in the middle of streaming
	 */
	@Test
	public void testNewQueryWhileStreaming() throws Exception {
		Util.insertRows(testEnv, 1, 10);
		VtGate vtgate = VtGate.connect("localhost:" + testEnv.port, 0);
		String selectSql = "select * from vtgate_test";
		Query query = new QueryBuilder(selectSql,
				testEnv.keyspace, "master").withKeyspaceIds(
				testEnv.getAllKeyspaceIds()).withStreaming(true).build();
		vtgate.execute(query);
		try {
			vtgate.execute(query);
		} catch (ConnectionException e) {
			Assert.assertEquals(
					"vtgate exception: request not allowed as client is in the middle of streaming",
					e.getMessage());
		} finally {
			vtgate.close();
		}
	}
}
