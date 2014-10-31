package com.youtube.vitess.vtgate.integration;

import com.youtube.vitess.vtgate.Query;
import com.youtube.vitess.vtgate.Query.QueryBuilder;
import com.youtube.vitess.vtgate.VtGate;
import com.youtube.vitess.vtgate.cursor.Cursor;
import com.youtube.vitess.vtgate.integration.util.TestEnv;
import com.youtube.vitess.vtgate.integration.util.Util;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class StreamingServerShutdownIT {

  static TestEnv testEnv = VtGateIT.getTestEnv();

  @Before
  public void setUpVtGate() throws Exception {
    Util.setupTestEnv(testEnv, true);
    Util.truncateTable(testEnv);
  }

  @After
  public void tearDownVtGate() throws Exception {
    Util.setupTestEnv(testEnv, false);
  }

  @Test
  public void testShutdownServerWhileStreaming() throws Exception {
    Util.insertRows(testEnv, 1, 2000);
    VtGate vtgate = VtGate.connect("localhost:" + testEnv.port, 0);
    String selectSql = "select A.* from vtgate_test A join vtgate_test B";
    Query joinQuery = new QueryBuilder(selectSql, testEnv.keyspace, "master")
        .setKeyspaceIds(testEnv.getAllKeyspaceIds()).setStreaming(true).build();
    Cursor cursor = vtgate.execute(joinQuery);

    int count = 0;
    try {
      while (cursor.hasNext()) {
        count++;
        if (count == 1) {
          Util.setupTestEnv(testEnv, false);
        }
        cursor.next();
      }
      vtgate.close();
      Assert.fail("failed to raise exception");
    } catch (RuntimeException e) {
      Assert.assertTrue(e.getMessage().contains("vtgate exception: connection exception"));
    }
  }
}
