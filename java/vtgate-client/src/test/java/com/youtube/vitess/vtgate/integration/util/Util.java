package com.youtube.vitess.vtgate.integration.util;

import com.google.common.primitives.UnsignedLong;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;

import com.youtube.vitess.vtgate.BindVariable;
import com.youtube.vitess.vtgate.Exceptions.ConnectionException;
import com.youtube.vitess.vtgate.Exceptions.DatabaseException;
import com.youtube.vitess.vtgate.KeyspaceId;
import com.youtube.vitess.vtgate.Query;
import com.youtube.vitess.vtgate.Query.QueryBuilder;
import com.youtube.vitess.vtgate.VtGate;
import com.youtube.vitess.vtgate.cursor.Cursor;

import org.joda.time.DateTime;
import org.junit.Assert;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

public class Util {
  /**
   * Setup MySQL, Vttablet and VtGate instances required for the tests. This uses a Python helper
   * script to start and stop instances. Use the setUp flag to indicate setUp or teardown.
   */
  public static void setupTestEnv(TestEnv testEnv, boolean isSetUp) throws Exception {
    ProcessBuilder pb = new ProcessBuilder(SetupCommand.get(testEnv, isSetUp));
    pb.redirectErrorStream(true);
    Process p = pb.start();
    BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));

    p.waitFor();
    if (isSetUp) {
      // The port for VtGate is dynamically assigned and written to
      // stdout as a JSON string.
      String line;
      while ((line = br.readLine()) != null) {
        try {
          Type mapType = new TypeToken<Map<String, Integer>>() {}.getType();
          Map<String, Integer> map = new Gson().fromJson(line, mapType);
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
    insertRows(testEnv, startId, count, new DateTime());
  }

  public static void insertRows(TestEnv testEnv, int startId, int count, DateTime dateTime)
      throws ConnectionException, DatabaseException {
    VtGate vtgate = VtGate.connect("localhost:" + testEnv.port, 0);

    vtgate.begin();
    String insertSql =
        "insert into vtgate_test "
            + "(id, name, age, percent, datetime_col, timestamp_col, date_col, time_col, keyspace_id) "
            + "values (:id, :name, :age, :percent, :datetime_col, :timestamp_col, :date_col, :time_col, :keyspace_id)";
    for (int i = startId; i < startId + count; i++) {
      KeyspaceId kid = testEnv.getAllKeyspaceIds().get(i % testEnv.getAllKeyspaceIds().size());
      Query query =
          new QueryBuilder(insertSql, testEnv.keyspace, "master")
              .addBindVar(BindVariable.forULong("id", UnsignedLong.valueOf("" + i)))
              .addBindVar(BindVariable.forBytes("name", ("name_" + i).getBytes()))
              .addBindVar(BindVariable.forInt("age", i * 2))
              .addBindVar(BindVariable.forDouble("percent", new Double(i / 100.0)))
              .addBindVar(BindVariable.forULong("keyspace_id", (UnsignedLong) kid.getId()))
              .addBindVar(BindVariable.forDateTime("datetime_col", dateTime))
              .addBindVar(BindVariable.forDateTime("timestamp_col", dateTime))
              .addBindVar(BindVariable.forDate("date_col", dateTime))
              .addBindVar(BindVariable.forTime("time_col", dateTime)).addKeyspaceId(kid).build();
      vtgate.execute(query);
    }
    vtgate.commit();
    vtgate.close();
  }

  /**
   * Insert rows to a specific shard using ExecuteKeyspaceIds
   */
  public static void insertRowsInShard(TestEnv testEnv, String shardName, int count)
      throws DatabaseException, ConnectionException {
    VtGate vtgate = VtGate.connect("localhost:" + testEnv.port, 0);
    vtgate.begin();
    String sql =
        "insert into vtgate_test " + "(id, name, keyspace_id) "
            + "values (:id, :name, :keyspace_id)";
    List<KeyspaceId> kids = testEnv.getKeyspaceIds(shardName);
    for (int i = 1; i <= count; i++) {
      KeyspaceId kid = kids.get(i % kids.size());
      Query query =
          new QueryBuilder(sql, testEnv.keyspace, "master")
              .addBindVar(BindVariable.forULong("id", UnsignedLong.valueOf("" + i)))
              .addBindVar(BindVariable.forBytes("name", ("name_" + i).getBytes()))
              .addBindVar(BindVariable.forULong("keyspace_id", (UnsignedLong) kid.getId()))
              .addKeyspaceId(kid).build();
      vtgate.execute(query);
    }
    vtgate.commit();
    vtgate.close();
  }

  public static void truncateTable(TestEnv testEnv) throws Exception {
    VtGate vtgate = VtGate.connect("localhost:" + testEnv.port, 0);
    vtgate.begin();
    vtgate.execute(new QueryBuilder("delete from vtgate_test", testEnv.keyspace, "master")
        .setKeyspaceIds(testEnv.getAllKeyspaceIds()).build());
    vtgate.commit();
    vtgate.close();
  }

  /**
   * Wait until the specified tablet type has received at least rowCount rows in vtgate_test from
   * the master. If the criteria isn't met after the specified number of attempts raise an
   * exception.
   */
  public static void waitForTablet(String tabletType, int rowCount, int attempts, TestEnv testEnv)
      throws Exception {
    String sql = "select * from vtgate_test";
    VtGate vtgate = VtGate.connect("localhost:" + testEnv.port, 0);
    for (int i = 0; i < attempts; i++) {
      Cursor cursor =
          vtgate.execute(new QueryBuilder(sql, testEnv.keyspace, tabletType).setKeyspaceIds(
              testEnv.getAllKeyspaceIds()).build());
      if (cursor.getRowsAffected() >= rowCount) {
        vtgate.close();
        return;
      }
      Thread.sleep(1000);
    }
    vtgate.close();
    throw new Exception(tabletType + " fails to catch up");
  }
}
