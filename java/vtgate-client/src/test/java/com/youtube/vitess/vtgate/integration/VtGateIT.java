package com.youtube.vitess.vtgate.integration;

import com.google.common.collect.Lists;
import com.google.common.primitives.UnsignedLong;

import com.youtube.vitess.vtgate.BatchQuery;
import com.youtube.vitess.vtgate.BatchQuery.BatchQueryBuilder;
import com.youtube.vitess.vtgate.BindVariable;
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
import com.youtube.vitess.vtgate.integration.util.TestEnv;
import com.youtube.vitess.vtgate.integration.util.Util;

import org.joda.time.DateTime;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

@RunWith(JUnit4.class)
public class VtGateIT {

  public static TestEnv testEnv = getTestEnv();

  @BeforeClass
  public static void setUpVtGate() throws Exception {
    Util.setupTestEnv(testEnv, true);
  }

  @AfterClass
  public static void tearDownVtGate() throws Exception {
    Util.setupTestEnv(testEnv, false);
  }

  @Before
  public void createTable() throws Exception {
    Util.createTable(testEnv);
  }

  /**
   * Test DMLs are not allowed outside a transaction
   */
  @Test
  public void testDMLOutsideTransaction() throws ConnectionException {
    VtGate vtgate = VtGate.connect("localhost:" + testEnv.port, 0);
    String deleteSql = "delete from vtgate_test";
    try {
      vtgate.execute(new QueryBuilder(deleteSql, testEnv.keyspace, "master").addKeyspaceId(
          testEnv.getAllKeyspaceIds().get(0)).build());
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
    VtGate vtgate = VtGate.connect("localhost:" + testEnv.port, 0);

    // Ensure empty table
    String selectSql = "select * from vtgate_test";
    Query allRowsQuery = new QueryBuilder(selectSql, testEnv.keyspace, "master").setKeyspaceIds(
        testEnv.getAllKeyspaceIds()).build();
    Cursor cursor = vtgate.execute(allRowsQuery);
    Assert.assertEquals(CursorImpl.class, cursor.getClass());
    Assert.assertEquals(0, cursor.getRowsAffected());
    Assert.assertEquals(0, cursor.getLastRowId());
    Assert.assertFalse(cursor.hasNext());
    vtgate.close();

    // Insert 10 rows
    Util.insertRows(testEnv, 1000, 10);

    vtgate = VtGate.connect("localhost:" + testEnv.port, 0);
    cursor = vtgate.execute(allRowsQuery);
    Assert.assertEquals(10, cursor.getRowsAffected());
    Assert.assertEquals(0, cursor.getLastRowId());
    Assert.assertTrue(cursor.hasNext());

    // Fetch all rows from the first shard
    KeyspaceId firstKid = testEnv.getAllKeyspaceIds().get(0);
    Query query =
        new QueryBuilder(selectSql, testEnv.keyspace, "master").addKeyspaceId(firstKid).build();
    cursor = vtgate.execute(query);

    // Check field types and values
    Row row = cursor.next();
    Cell idCell = row.next();
    Assert.assertEquals("id", idCell.getName());
    Assert.assertEquals(UnsignedLong.class, idCell.getType());
    UnsignedLong id = row.getULong(idCell.getName());

    Cell nameCell = row.next();
    Assert.assertEquals("name", nameCell.getName());
    Assert.assertEquals(byte[].class, nameCell.getType());
    Assert.assertTrue(
        Arrays.equals(("name_" + id.toString()).getBytes(), row.getBytes(nameCell.getName())));

    Cell ageCell = row.next();
    Assert.assertEquals("age", ageCell.getName());
    Assert.assertEquals(Integer.class, ageCell.getType());
    Assert.assertEquals(Integer.valueOf(2 * id.intValue()), row.getInt(ageCell.getName()));

    vtgate.close();
  }

  /**
   * Test queries are routed to the right shard based on based on keyspace ids
   */
  @Test
  public void testQueryRouting() throws Exception {
    for (String shardName : testEnv.shardKidMap.keySet()) {
      Util.insertRowsInShard(testEnv, shardName, 10);
    }

    VtGate vtgate = VtGate.connect("localhost:" + testEnv.port, 0);
    String allRowsSql = "select * from vtgate_test";

    for (String shardName : testEnv.shardKidMap.keySet()) {
      Query shardRows = new QueryBuilder(allRowsSql, testEnv.keyspace, "master").setKeyspaceIds(
          testEnv.getKeyspaceIds(shardName)).build();
      Cursor cursor = vtgate.execute(shardRows);
      Assert.assertEquals(10, cursor.getRowsAffected());
    }
    vtgate.close();
  }

  @Test
  public void testDateFieldTypes() throws Exception {
    DateTime dt = DateTime.now().minusDays(2).withMillisOfSecond(0);
    Util.insertRows(testEnv, 10, 1, dt);
    VtGate vtgate = VtGate.connect("localhost:" + testEnv.port, 0);
    Query allRowsQuery = new QueryBuilder("select * from vtgate_test", testEnv.keyspace, "master")
        .setKeyspaceIds(testEnv.getAllKeyspaceIds()).build();
    Row row = vtgate.execute(allRowsQuery).next();
    Assert.assertTrue(dt.equals(row.getDateTime("timestamp_col")));
    Assert.assertTrue(dt.equals(row.getDateTime("datetime_col")));
    Assert.assertTrue(dt.withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0)
        .equals(row.getDateTime("date_col")));
    Assert.assertTrue(
        dt.withYear(1970).withMonthOfYear(1).withDayOfMonth(1).equals(row.getDateTime("time_col")));

    vtgate.close();
  }

  /**
   * Test ALL keyrange fetches rows from all shards
   */
  @Test
  public void testAllKeyRange() throws Exception {
    // Insert 10 rows across the shards
    Util.insertRows(testEnv, 1000, 10);
    VtGate vtgate = VtGate.connect("localhost:" + testEnv.port, 0);
    String selectSql = "select * from vtgate_test";
    Query allRowsQuery =
        new QueryBuilder(selectSql, testEnv.keyspace, "master").addKeyRange(KeyRange.ALL).build();
    Cursor cursor = vtgate.execute(allRowsQuery);
    // Verify all rows returned
    Assert.assertEquals(10, cursor.getRowsAffected());
    vtgate.close();
  }

  /**
   * Test reads using Keyrange query
   */
  @Test
  public void testKeyRangeReads() throws Exception {
    int rowsPerShard = 10;
    // insert rows in each shard using ExecuteKeyspaceIds
    for (String shardName : testEnv.shardKidMap.keySet()) {
      Util.insertRowsInShard(testEnv, shardName, rowsPerShard);
    }

    VtGate vtgate = VtGate.connect("localhost:" + testEnv.port, 0);
    String selectSql = "select * from vtgate_test";

    // Check ALL KeyRange query returns rows from both shards
    Query allRangeQuery =
        new QueryBuilder(selectSql, testEnv.keyspace, "master").addKeyRange(KeyRange.ALL).build();
    Cursor cursor = vtgate.execute(allRangeQuery);
    Assert.assertEquals(rowsPerShard * 2, cursor.getRowsAffected());

    // Check KeyRange query limited to a single shard returns 10 rows each
    for (String shardName : testEnv.shardKidMap.keySet()) {
      List<KeyspaceId> shardKids = testEnv.getKeyspaceIds(shardName);
      KeyspaceId minKid = Collections.min(shardKids);
      KeyspaceId maxKid = Collections.max(shardKids);
      KeyRange shardKeyRange = new KeyRange(minKid, maxKid);
      Query shardRangeQuery = new QueryBuilder(selectSql, testEnv.keyspace, "master").addKeyRange(
          shardKeyRange).build();
      cursor = vtgate.execute(shardRangeQuery);
      Assert.assertEquals(rowsPerShard, cursor.getRowsAffected());
    }

    // Now make a cross-shard KeyRange and check all rows are returned
    Iterator<String> shardNameIter = testEnv.shardKidMap.keySet().iterator();
    KeyspaceId kidShard1 = testEnv.getKeyspaceIds(shardNameIter.next()).get(2);
    KeyspaceId kidShard2 = testEnv.getKeyspaceIds(shardNameIter.next()).get(2);
    KeyRange crossShardKeyrange;
    if (kidShard1.compareTo(kidShard2) < 0) {
      crossShardKeyrange = new KeyRange(kidShard1, kidShard2);
    } else {
      crossShardKeyrange = new KeyRange(kidShard2, kidShard1);
    }
    Query shardRangeQuery = new QueryBuilder(selectSql, testEnv.keyspace, "master").addKeyRange(
        crossShardKeyrange).build();
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
    VtGate vtgate = VtGate.connect("localhost:" + testEnv.port, 0);
    vtgate.begin();
    String sql = "insert into vtgate_test " + "(id, name, keyspace_id, age) "
        + "values (:id, :name, :keyspace_id, :age)";
    int count = 20;
    // Insert 20 rows per shard
    for (String shardName : testEnv.shardKidMap.keySet()) {
      List<KeyspaceId> kids = testEnv.getKeyspaceIds(shardName);
      KeyspaceId minKid = Collections.min(kids);
      KeyspaceId maxKid = Collections.max(kids);
      KeyRange kr = new KeyRange(minKid, maxKid);
      for (int i = 0; i < count; i++) {
        KeyspaceId kid = kids.get(i % kids.size());
        Query query = new QueryBuilder(sql, testEnv.keyspace, "master")
            .addBindVar(
                BindVariable.forULong("id", UnsignedLong.valueOf("" + Math.abs(random.nextInt()))))
            .addBindVar(BindVariable.forString("name", ("name_" + i)))
            .addBindVar(BindVariable.forULong("keyspace_id", (UnsignedLong) kid.getId()))
            .addBindVar(BindVariable.forNull("age"))
            .addKeyRange(kr)
            .build();
        vtgate.execute(query);
      }
    }
    vtgate.commit();
    vtgate.close();

    // Check 40 rows exist in total
    vtgate = VtGate.connect("localhost:" + testEnv.port, 0);
    String selectSql = "select * from vtgate_test";
    Query allRowsQuery = new QueryBuilder(selectSql, testEnv.keyspace, "master").setKeyspaceIds(
        testEnv.getAllKeyspaceIds()).build();
    Cursor cursor = vtgate.execute(allRowsQuery);
    Assert.assertEquals(count * 2, cursor.getRowsAffected());

    // Check 20 rows exist per shard
    for (String shardName : testEnv.shardKidMap.keySet()) {
      Query shardRows = new QueryBuilder(selectSql, testEnv.keyspace, "master").setKeyspaceIds(
          testEnv.getKeyspaceIds(shardName)).build();
      cursor = vtgate.execute(shardRows);
      Assert.assertEquals(count, cursor.getRowsAffected());
    }

    vtgate.close();
  }

  @Test
  public void testBatchExecuteKeyspaceIds() throws Exception {
    int rowsPerShard = 5;
    for (String shardName : testEnv.shardKidMap.keySet()) {
      Util.insertRowsInShard(testEnv, shardName, rowsPerShard);
    }
    VtGate vtgate = VtGate.connect("localhost:" + testEnv.port, 0);
    BatchQuery query = new BatchQueryBuilder(testEnv.keyspace, "master")
        .addSqlAndBindVars("select * from vtgate_test where id = 3", null).addSqlAndBindVars(
            "select * from vtgate_test where id = :id",
            Lists.newArrayList(BindVariable.forULong("id", UnsignedLong.valueOf("4"))))
        .withKeyspaceIds(testEnv.getAllKeyspaceIds()).build();
    List<Long> expected = Lists.newArrayList(3L, 3L, 4L, 4L);
    List<Cursor> cursors = vtgate.execute(query);
    List<Long> actual = new ArrayList<>();
    for (Cursor cursor : cursors) {
      for (Row row : cursor) {
        actual.add(row.getULong("id").longValue());
      }
    }
    Assert.assertTrue(expected.equals(actual));
    vtgate.close();
  }


  /**
   * Create env with two shards each having a master and replica
   */
  static TestEnv getTestEnv() {
    Map<String, List<String>> shardKidMap = new HashMap<>();
    shardKidMap.put("-80",
        Lists.newArrayList("527875958493693904", "626750931627689502", "345387386794260318"));
    shardKidMap.put("80-",
        Lists.newArrayList("9767889778372766922", "9742070682920810358", "10296850775085416642"));
    TestEnv env = new TestEnv(shardKidMap, "test_keyspace");
    env.addTablet("replica", 1);
    return env;
  }
}
