package com.youtube.vitess.vtgate.integration;

import com.google.common.collect.Lists;
import com.google.common.primitives.UnsignedLong;

import com.youtube.vitess.vtgate.BindVariable;
import com.youtube.vitess.vtgate.Exceptions.ConnectionException;
import com.youtube.vitess.vtgate.Exceptions.DatabaseException;
import com.youtube.vitess.vtgate.Exceptions.IntegrityException;
import com.youtube.vitess.vtgate.KeyspaceId;
import com.youtube.vitess.vtgate.Query;
import com.youtube.vitess.vtgate.Query.QueryBuilder;
import com.youtube.vitess.vtgate.VtGate;
import com.youtube.vitess.vtgate.integration.util.TestEnv;
import com.youtube.vitess.vtgate.integration.util.Util;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Test failures and exceptions
 */
public class FailuresIT {
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
  public void truncateTable() throws Exception {
    Util.truncateTable(testEnv);
  }

  @Test
  public void testIntegrityException() throws Exception {
    VtGate vtgate = VtGate.connect("localhost:" + testEnv.port, 0);
    String insertSql = "insert into vtgate_test(id, keyspace_id) values (:id, :keyspace_id)";
    KeyspaceId kid = testEnv.getAllKeyspaceIds().get(0);
    Query insertQuery = new QueryBuilder(insertSql, testEnv.keyspace, "master")
        .addBindVar(BindVariable.forInt("id", 1))
        .addBindVar(BindVariable.forULong("keyspace_id", ((UnsignedLong) kid.getId())))
        .addKeyspaceId(kid).build();
    vtgate.begin();
    vtgate.execute(insertQuery);
    vtgate.commit();
    vtgate.begin();
    try {
      vtgate.execute(insertQuery);
      Assert.fail("failed to throw exception");
    } catch (IntegrityException e) {
    } finally {
      vtgate.rollback();
      vtgate.close();
    }
  }

  @Test
  public void testTimeout() throws ConnectionException, DatabaseException {
    VtGate vtgate = VtGate.connect("localhost:" + testEnv.port, 200);
    // Check timeout error raised for slow query
    Query sleepQuery = new QueryBuilder("select sleep(0.5) from dual", testEnv.keyspace, "master")
        .setKeyspaceIds(testEnv.getAllKeyspaceIds()).build();
    try {
      vtgate.execute(sleepQuery);
      Assert.fail("did not raise timeout exception");
    } catch (ConnectionException e) {
    }
    vtgate.close();
    vtgate = VtGate.connect("localhost:" + testEnv.port, 200);
    // Check no timeout error for fast query
    sleepQuery = new QueryBuilder("select sleep(0.01) from dual", testEnv.keyspace, "master")
        .setKeyspaceIds(testEnv.getAllKeyspaceIds()).build();
    vtgate.execute(sleepQuery);
    vtgate.close();
  }

  /**
   * Create env with two shards each having a master and replica
   */
  static TestEnv getTestEnv() {
    Map<String, List<String>> shardKidMap = new HashMap<>();
    shardKidMap.put("-", Lists.newArrayList("527875958493693904"));
    TestEnv env = new TestEnv(shardKidMap, "test_keyspace");
    env.addTablet("replica", 1);
    return env;
  }
}
