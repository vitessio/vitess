package com.youtube.vitess.vtgate.integration;

import java.math.BigInteger;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.youtube.vitess.vtgate.Exceptions.ConnectionException;
import com.youtube.vitess.vtgate.Exceptions.DatabaseException;
import com.youtube.vitess.vtgate.Query;
import com.youtube.vitess.vtgate.Query.QueryBuilder;
import com.youtube.vitess.vtgate.Row;
import com.youtube.vitess.vtgate.Row.Cell;
import com.youtube.vitess.vtgate.VtGate;
import com.youtube.vitess.vtgate.cursor.Cursor;
import com.youtube.vitess.vtgate.cursor.CursorImpl;
import com.youtube.vitess.vtgate.cursor.StreamCursor;
import com.youtube.vitess.vtgate.integration.Util.VtGateParams;

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
		Assert.assertEquals(CursorImpl.class, cursor.getClass());
		Assert.assertEquals(0, cursor.getRowsAffected());
		Assert.assertEquals(0, cursor.getLastRowId());
		Assert.assertFalse(cursor.hasNext());
		vtgate.close();

		Util.insertRows(params, 1000, 100);

		vtgate = VtGate.connect("localhost:" + params.port);
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

	@Test
	public void testStreamCursorType() throws Exception {
		VtGate vtgate = VtGate.connect("localhost:" + params.port);
		String selectSql = "select * from vtgate_test";
		Query allRowsQuery = new QueryBuilder(selectSql,
				params.keyspace_name, "master").withKeyspaceIds(
				params.getKeyspaceIds()).withStream(true).build();
		Cursor cursor = vtgate.execute(allRowsQuery);
		Assert.assertEquals(StreamCursor.class, cursor.getClass());
		vtgate.close();
	}

	@Test
	public void testStreamingReads() throws Exception {
		Util.insertRows(params, 1, 200);
		VtGate vtgate = VtGate.connect("localhost:" + params.port);
		String selectSql = "select A.* from vtgate_test A join vtgate_test B";
		Query joinQuery = new QueryBuilder(selectSql,
				params.keyspace_name, "master").withKeyspaceIds(
				params.getKeyspaceIds()).withStream(true).build();
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
		VtGate vtgate = VtGate.connect("localhost:" + params.port);

		vtgate.begin();
		String insertSql = "insert into vtgate_test "
				+ "(id, name, age, percent, keyspace_id) "
				+ "values (:id, :name, :age, :percent, :keyspace_id)";
		String kid = params.getKeyspaceIds().get(0);
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
	}

	@Test
	public void testNewQueryWhileStreaming() throws Exception {
		Util.insertRows(params, 1, 10);
		VtGate vtgate = VtGate.connect("localhost:" + params.port);
		String selectSql = "select * from vtgate_test";
		Query query = new QueryBuilder(selectSql,
				params.keyspace_name, "master").withKeyspaceIds(
				params.getKeyspaceIds()).withStream(true).build();
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
