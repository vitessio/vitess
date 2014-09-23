package com.youtube.vitess.vtgate.integration;

import java.math.BigInteger;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TimeZone;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.youtube.vitess.vtgate.Exceptions.ConnectionException;
import com.youtube.vitess.vtgate.Exceptions.DatabaseException;
import com.youtube.vitess.vtgate.KeyRange;
import com.youtube.vitess.vtgate.KeyspaceId;
import com.youtube.vitess.vtgate.Query;
import com.youtube.vitess.vtgate.Query.QueryBuilder;
import com.youtube.vitess.vtgate.Row;
import com.youtube.vitess.vtgate.Row.Cell;
import com.youtube.vitess.vtgate.VtGate;
import com.youtube.vitess.vtgate.cursor.Cursor;
import com.youtube.vitess.vtgate.cursor.CursorImpl;
import com.youtube.vitess.vtgate.integration.util.Util;
import com.youtube.vitess.vtgate.integration.util.VtGateParams;

public class VtGateIT {

	public static VtGateParams params;

	@BeforeClass
	public static void setUpVtGate() throws Exception {
		params = Util.runVtGate(true);

	}

	@AfterClass
	public static void tearDownVtGate() throws Exception {
		Util.runVtGate(false);
	}

	@Before
	public void truncateTable() throws Exception {
		Util.truncateTable(params);
	}

	/**
	 * Test DMLs are not allowed outside a transaction
	 */
	@Test
	public void testDMLOutsideTransaction() throws ConnectionException {
		VtGate vtgate = VtGate.connect("localhost:" + params.port, 0);
		String deleteSql = "delete from vtgate_test";
		try {
			vtgate.execute(new QueryBuilder(deleteSql, params.keyspace_name,
					"master").withAddedKeyspaceId(
					params.getAllKeyspaceIds().get(0)).build());
			Assert.fail("did not raise DatabaseException");
		} catch (DatabaseException e) {
			Assert.assertTrue(e.getMessage().contains("not_in_tx"));
		} finally {
			vtgate.close();
		}
	}

	/**
	 * Test selects using ExecuteKeyspaceIds
	 */
	@Test
	public void testExecuteKeyspaceIds() throws Exception {
		VtGate vtgate = VtGate.connect("localhost:" + params.port, 0);

		// Ensure empty table
		String selectSql = "select * from vtgate_test";
		Query allRowsQuery = new QueryBuilder(selectSql,
				params.keyspace_name, "master").withKeyspaceIds(
				params.getAllKeyspaceIds()).build();
		Cursor cursor = vtgate.execute(allRowsQuery);
		Assert.assertEquals(CursorImpl.class, cursor.getClass());
		Assert.assertEquals(0, cursor.getRowsAffected());
		Assert.assertEquals(0, cursor.getLastRowId());
		Assert.assertFalse(cursor.hasNext());
		vtgate.close();

		// Insert 100 rows
		Util.insertRows(params, 1000, 100);

		vtgate = VtGate.connect("localhost:" + params.port, 0);
		cursor = vtgate.execute(allRowsQuery);
		Assert.assertEquals(100, cursor.getRowsAffected());
		Assert.assertEquals(0, cursor.getLastRowId());
		Assert.assertTrue(cursor.hasNext());

		// Fetch all rows from the first shard
		KeyspaceId firstKid = params.getAllKeyspaceIds().get(0);
		Query query = new QueryBuilder(selectSql,
				params.keyspace_name, "master")
				.withAddedKeyspaceId(firstKid)
				.build();
		cursor = vtgate.execute(query);

		// Check field types and values
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

		vtgate.close();
	}

	/**
	 * Test queries are routed to the right shard based on based on keyspace ids
	 */
	@Test
	public void testQueryRouting() throws Exception {
		for (String shardName : params.shard_kid_map.keySet()) {
			Util.insertRowsInShard(params, shardName, 10);
		}

		VtGate vtgate = VtGate.connect("localhost:" + params.port, 0);
		String allRowsSql = "select * from vtgate_test";

		for (String shardName : params.shard_kid_map.keySet()) {
			Query shardRows = new QueryBuilder(allRowsSql,
					params.keyspace_name, "master")
					.withKeyspaceIds(params.getKeyspaceIds(shardName))
					.build();
			Cursor cursor = vtgate.execute(shardRows);
			Assert.assertEquals(10, cursor.getRowsAffected());
		}
		vtgate.close();
	}

	@Test
	public void testDateFieldTypes() throws Exception {
		Date date = new Date();
		Util.insertRows(params, 100, 1, date);
		VtGate vtgate = VtGate.connect("localhost:" + params.port, 0);
		Query allRowsQuery = new QueryBuilder(
				"select * from vtgate_test",
				params.keyspace_name, "master").withKeyspaceIds(
				params.getAllKeyspaceIds()).build();
		Row row = vtgate.execute(allRowsQuery).next();
		Date expected = convertToGMT(date);
		Assert.assertEquals(expected, row.getDate("timestamp_col"));
		Assert.assertEquals(expected, row.getDate("datetime_col"));

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		Date dateWithoutTime = sdf.parse(sdf.format(expected));
		Assert.assertEquals(dateWithoutTime, row.getDate("date_col"));

		sdf = new SimpleDateFormat("HH:mm:ss");
		Date dateWithoutDay = sdf.parse(sdf.format(expected));
		Assert.assertEquals(dateWithoutDay, row.getDate("time_col"));
		vtgate.close();
	}

	private Date convertToGMT(Date d) throws ParseException {
		DateFormat gmtFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		gmtFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
		return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(gmtFormat
				.format(d));
	}

	@Test
	public void testTimeout() throws ConnectionException, DatabaseException {
		VtGate vtgate = VtGate.connect("localhost:" + params.port, 200);

		// Check timeout error raised for slow query
		Query sleepQuery = new QueryBuilder("select sleep(0.5) from dual",
				params.keyspace_name, "master").withKeyspaceIds(
				params.getAllKeyspaceIds()).build();
		try {
			vtgate.execute(sleepQuery);
			Assert.fail("did not raise timeout exception");
		} catch (ConnectionException e) {
			Assert.assertEquals(
					"vtgate exception: connection exception Read timed out",
					e.getMessage());
		}

		// Check no timeout error for fast query
		sleepQuery = new QueryBuilder("select sleep(0.01) from dual",
				params.keyspace_name, "master").withKeyspaceIds(
				params.getAllKeyspaceIds()).build();
		vtgate.execute(sleepQuery);
		vtgate.close();
	}

	/**
	 * Test ALL keyrange fetches rows from all shards
	 */
	public void testAllKeyRange() throws Exception {
		// Insert 100 rows across the shards
		Util.insertRows(params, 1000, 100);
		VtGate vtgate = VtGate.connect("localhost:" + params.port, 0);
		String selectSql = "select * from vtgate_test";
		Query allRowsQuery = new QueryBuilder(selectSql, params.keyspace_name,
				"master").withAddedKeyRange(KeyRange.ALL).build();
		Cursor cursor = vtgate.execute(allRowsQuery);
		// Verify all rows returned
		Assert.assertEquals(100, cursor.getRowsAffected());
		vtgate.close();
	}

	/**
	 * Test reads using Keyrange query
	 */
	@Test
	public void testKeyRangeReads() throws Exception {
		int rowsPerShard = 100;
		// insert rows in each shard using ExecuteKeyspaceIds
		for (String shardName : params.shard_kid_map.keySet()) {
			Util.insertRowsInShard(params, shardName, rowsPerShard);
		}

		VtGate vtgate = VtGate.connect("localhost:" + params.port, 0);
		String selectSql = "select * from vtgate_test";

		// Check ALL KeyRange query returns rows from both shards
		Query allRangeQuery = new QueryBuilder(selectSql, params.keyspace_name,
				"master").withAddedKeyRange(KeyRange.ALL).build();
		Cursor cursor = vtgate.execute(allRangeQuery);
		Assert.assertEquals(rowsPerShard * 2, cursor.getRowsAffected());

		// Check KeyRange query limited to a single shard returns 100 rows each
		for (String shardName : params.shard_kid_map.keySet()) {
			List<KeyspaceId> shardKids = params.getKeyspaceIds(shardName);
			KeyspaceId minKid = Collections.min(shardKids);
			KeyspaceId maxKid = Collections.max(shardKids);
			KeyRange shardKeyRange = new KeyRange(minKid, maxKid);
			Query shardRangeQuery = new QueryBuilder(selectSql,
					params.keyspace_name, "master")
					.withAddedKeyRange(shardKeyRange).build();
			cursor = vtgate.execute(shardRangeQuery);
			Assert.assertEquals(rowsPerShard, cursor.getRowsAffected());
		}

		// Now make a cross-shard KeyRange and check all rows are returned
		Iterator<String> shardNameIter = params.shard_kid_map.keySet()
				.iterator();
		KeyspaceId kidShard1 = params.getKeyspaceIds(shardNameIter.next())
				.get(2);
		KeyspaceId kidShard2 = params.getKeyspaceIds(shardNameIter.next())
				.get(2);
		KeyRange crossShardKeyrange;
		if (kidShard1.compareTo(kidShard2) < 0) {
			crossShardKeyrange = new KeyRange(kidShard1, kidShard2);
		} else {
			crossShardKeyrange = new KeyRange(kidShard2, kidShard1);
		}
		Query shardRangeQuery = new QueryBuilder(selectSql,
				params.keyspace_name, "master")
				.withAddedKeyRange(crossShardKeyrange).build();
		cursor = vtgate.execute(shardRangeQuery);
		Assert.assertEquals(rowsPerShard * 2, cursor.getRowsAffected());
		vtgate.close();
	}

	/**
	 * Test inserts using KeyRange query
	 */
	@Test
	public void testKeyRangeWrites() throws Exception {
		Random random = new Random();
		VtGate vtgate = VtGate.connect("localhost:" + params.port, 0);
		vtgate.begin();
		String insertSql = "insert into vtgate_test "
				+ "(id, name, keyspace_id) "
				+ "values (:id, :name, :keyspace_id)";
		int count = 20;
		// Insert 20 rows per shard
		for (String shardName : params.shard_kid_map.keySet()) {
			List<KeyspaceId> kids = params.getKeyspaceIds(shardName);
			KeyspaceId minKid = Collections.min(kids);
			KeyspaceId maxKid = Collections.max(kids);
			KeyRange kr = new KeyRange(minKid, maxKid);
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
						.withAddedKeyRange(kr)
						.build();
				vtgate.execute(query);
			}
		}
		vtgate.commit();
		vtgate.close();

		// Check 40 rows exist in total
		vtgate = VtGate.connect("localhost:" + params.port, 0);
		String selectSql = "select * from vtgate_test";
		Query allRowsQuery = new QueryBuilder(selectSql,
				params.keyspace_name, "master").withKeyspaceIds(
				params.getAllKeyspaceIds()).build();
		Cursor cursor = vtgate.execute(allRowsQuery);
		Assert.assertEquals(count * 2, cursor.getRowsAffected());

		// Check 20 rows exist per shard
		for (String shardName : params.shard_kid_map.keySet()) {
			Query shardRows = new QueryBuilder(selectSql,
					params.keyspace_name, "master").withKeyspaceIds(
					params.getKeyspaceIds(shardName)).build();
			cursor = vtgate.execute(shardRows);
			Assert.assertEquals(count, cursor.getRowsAffected());
		}

		vtgate.close();
	}
}
