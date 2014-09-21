package com.youtube.vitess.vtgate.integration;

import java.math.BigInteger;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.youtube.vitess.vtgate.Exceptions.ConnectionException;
import com.youtube.vitess.vtgate.Exceptions.DatabaseException;
import com.youtube.vitess.vtgate.KeyspaceId;
import com.youtube.vitess.vtgate.Query;
import com.youtube.vitess.vtgate.Query.QueryBuilder;
import com.youtube.vitess.vtgate.Row;
import com.youtube.vitess.vtgate.Row.Cell;
import com.youtube.vitess.vtgate.VtGate;
import com.youtube.vitess.vtgate.cursor.Cursor;
import com.youtube.vitess.vtgate.cursor.CursorImpl;
import com.youtube.vitess.vtgate.cursor.StreamCursor;

public class VtGateIT {

	static VtGateParams params;

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

	@Test
	public void testReadsAndWrites() throws Exception {
		VtGate vtgate = VtGate.connect("localhost:" + params.port, 0);
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

		Util.insertRows(params, 1000, 100);

		vtgate = VtGate.connect("localhost:" + params.port, 0);
		cursor = vtgate.execute(allRowsQuery);
		Assert.assertEquals(100, cursor.getRowsAffected());
		Assert.assertEquals(0, cursor.getLastRowId());
		Assert.assertTrue(cursor.hasNext());

		KeyspaceId firstKid = params.getAllKeyspaceIds().get(0);
		Query query = new QueryBuilder(selectSql,
				params.keyspace_name, "master")
				.withAddedKeyspaceId(firstKid)
				.build();
		cursor = vtgate.execute(query);
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

	@Test
	public void testQueryRouting() throws Exception {
		for (String shardName : params.shard_kid_map.keySet()) {
			Util.insertRowsInShard(params, shardName, 10);
		}

		VtGate vtgate = VtGate.connect("localhost:" + params.port, 0);
		String allRowsSql = "select * from vtgate_test";

		for (String shardName : params.shard_kid_map.keySet()) {
			Query shardRows = new QueryBuilder(allRowsSql,
					params.keyspace_name, "master").withKeyspaceIds(
					params.getKeyspaceIds(shardName)).build();
			Cursor cursor = vtgate.execute(shardRows);
			Assert.assertEquals(10, cursor.getRowsAffected());
		}

		vtgate.close();
	}

	@Test
	public void testStreamCursorType() throws Exception {
		VtGate vtgate = VtGate.connect("localhost:" + params.port, 0);
		String selectSql = "select * from vtgate_test";
		Query allRowsQuery = new QueryBuilder(selectSql,
				params.keyspace_name, "master").withKeyspaceIds(
				params.getAllKeyspaceIds()).withStream(true).build();
		Cursor cursor = vtgate.execute(allRowsQuery);
		Assert.assertEquals(StreamCursor.class, cursor.getClass());
		vtgate.close();
	}

	@Test
	public void testStreamingReads() throws Exception {
		Util.insertRows(params, 1, 200);
		VtGate vtgate = VtGate.connect("localhost:" + params.port, 0);
		String selectSql = "select A.* from vtgate_test A join vtgate_test B";
		Query joinQuery = new QueryBuilder(selectSql, params.keyspace_name,
				"master")
				.withKeyspaceIds(params.getAllKeyspaceIds())
				.withStream(true)
				.build();
		Cursor cursor = vtgate.execute(joinQuery);

		int count = 0;
		for (Row row : cursor) {
			count++;
			Cell idCell = row.next();
			Assert.assertEquals("id", idCell.getName());
			Assert.assertEquals(BigInteger.class, idCell.getType());
		}
		Assert.assertEquals(40000, count);
		vtgate.close();
	}

	@Test
	@Ignore("currently failing as vtgate doesn't set the error")
	public void testStreamingWritesThrowsError() throws Exception {
		VtGate vtgate = VtGate.connect("localhost:" + params.port, 0);

		vtgate.begin();
		String insertSql = "insert into vtgate_test "
				+ "(id, name, age, percent, keyspace_id) "
				+ "values (:id, :name, :age, :percent, :keyspace_id)";
		KeyspaceId kid = params.getAllKeyspaceIds().get(0);
		Map<String, Object> bindVars = new ImmutableMap.Builder<String, Object>()
				.put("id", 1)
				.put("name", "name_" + 1)
				.put("age", 2)
				.put("percent", new Double(1.0))
				.put("keyspace_id", kid)
				.build();
		Query query = new QueryBuilder(insertSql,
				params.keyspace_name, "master")
				.withBindVars(bindVars)
				.withAddedKeyspaceId(kid)
				.withStream(true)
				.build();
		vtgate.execute(query);
		vtgate.commit();
		vtgate.close();
	}

	@Test
	public void testNewQueryWhileStreaming() throws Exception {
		Util.insertRows(params, 1, 10);
		VtGate vtgate = VtGate.connect("localhost:" + params.port, 0);
		String selectSql = "select * from vtgate_test";
		Query query = new QueryBuilder(selectSql,
				params.keyspace_name, "master").withKeyspaceIds(
				params.getAllKeyspaceIds()).withStream(true).build();
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
}
