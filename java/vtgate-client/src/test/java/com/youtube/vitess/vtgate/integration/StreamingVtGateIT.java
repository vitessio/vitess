package com.youtube.vitess.vtgate.integration;

import com.google.common.primitives.UnsignedLong;

import com.youtube.vitess.vtgate.BindVariable;
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

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Collections;
import java.util.List;

/**
 * Test cases for streaming queries in VtGate
 *
 */
@RunWith(JUnit4.class)
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
    Query allRowsQuery = new QueryBuilder(selectSql, testEnv.keyspace, "master")
        .setKeyspaceIds(testEnv.getAllKeyspaceIds()).setStreaming(true).build();
    Cursor cursor = vtgate.execute(allRowsQuery);
    Assert.assertEquals(StreamCursor.class, cursor.getClass());
    vtgate.close();
  }

  /**
   * Test StreamExecuteKeyspaceIds query on a single shard
   */
  @Test
  public void testStreamExecuteKeyspaceIds() throws Exception {
    int rowCount = 10;
    for (String shardName : testEnv.shardKidMap.keySet()) {
      Util.insertRowsInShard(testEnv, shardName, rowCount);
    }
    VtGate vtgate = VtGate.connect("localhost:" + testEnv.port, 0);
    for (String shardName : testEnv.shardKidMap.keySet()) {
      String selectSql = "select A.* from vtgate_test A join vtgate_test B join vtgate_test C";
      Query query = new QueryBuilder(selectSql, testEnv.keyspace, "master")
          .setKeyspaceIds(testEnv.getKeyspaceIds(shardName)).setStreaming(true).build();
      Cursor cursor = vtgate.execute(query);
      int count = 0;
      while (cursor.hasNext()) {
        cursor.next();
        count++;
      }
      Assert.assertEquals((int) Math.pow(rowCount, 3), count);
    }
    vtgate.close();
  }

  /**
   * Same as testStreamExecuteKeyspaceIds but for StreamExecuteKeyRanges
   */
  @Test
  public void testStreamExecuteKeyRanges() throws Exception {
    VtGate vtgate = VtGate.connect("localhost:" + testEnv.port, 0);
    int rowCount = 10;
    for (String shardName : testEnv.shardKidMap.keySet()) {
      Util.insertRowsInShard(testEnv, shardName, rowCount);
    }
    for (String shardName : testEnv.shardKidMap.keySet()) {
      List<KeyspaceId> kids = testEnv.getKeyspaceIds(shardName);
      KeyRange kr = new KeyRange(Collections.min(kids), Collections.max(kids));
      String selectSql = "select A.* from vtgate_test A join vtgate_test B join vtgate_test C";
      Query query = new QueryBuilder(selectSql, testEnv.keyspace, "master").addKeyRange(kr)
          .setStreaming(true).build();
      Cursor cursor = vtgate.execute(query);
      int count = 0;
      while (cursor.hasNext()) {
        cursor.next();
        count++;
      }
      Assert.assertEquals((int) Math.pow(rowCount, 3), count);
    }
    vtgate.close();
  }

  /**
   * Test scatter streaming queries fetch rows from all shards
   */
  @Test
  public void testScatterStreamingQuery() throws Exception {
    int rowCount = 10;
    for (String shardName : testEnv.shardKidMap.keySet()) {
      Util.insertRowsInShard(testEnv, shardName, rowCount);
    }
    String selectSql = "select A.* from vtgate_test A join vtgate_test B join vtgate_test C";
    Query query = new QueryBuilder(selectSql, testEnv.keyspace, "master")
        .setKeyspaceIds(testEnv.getAllKeyspaceIds()).setStreaming(true).build();
    VtGate vtgate = VtGate.connect("localhost:" + testEnv.port, 0);
    Cursor cursor = vtgate.execute(query);
    int count = 0;
    for (Row row : cursor) {
      count++;
    }
    Assert.assertEquals(2 * (int) Math.pow(rowCount, 3), count);
    vtgate.close();
  }

  @Test
  @Ignore("currently failing as vtgate doesn't set the error")
  public void testStreamingWrites() throws Exception {
    VtGate vtgate = VtGate.connect("localhost:" + testEnv.port, 0);

    vtgate.begin();
    String insertSql = "insert into vtgate_test " + "(id, name, age, percent, keyspace_id) "
        + "values (:id, :name, :age, :percent, :keyspace_id)";
    KeyspaceId kid = testEnv.getAllKeyspaceIds().get(0);
    Query query = new QueryBuilder(insertSql, testEnv.keyspace, "master")
        .addBindVar(BindVariable.forULong("id", UnsignedLong.valueOf("" + 1)))
        .addBindVar(BindVariable.forString("name", ("name_" + 1)))
        .addBindVar(BindVariable.forULong("keyspace_id", (UnsignedLong) kid.getId()))
        .addKeyspaceId(kid)
        .setStreaming(true)
        .build();
    vtgate.execute(query);
    vtgate.commit();
    vtgate.close();
  }
}
