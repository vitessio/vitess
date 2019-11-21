/*
 * Copyright 2019 The Vitess Authors.

 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

 *     http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.vitess.client;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;

import binlogdata.Binlogdata.FieldEvent;
import binlogdata.Binlogdata.RowEvent;
import binlogdata.Binlogdata.ShardGtid;
import binlogdata.Binlogdata.VEvent;
import binlogdata.Binlogdata.VEventType;
import binlogdata.Binlogdata.VGtid;
import io.vitess.client.cursor.Cursor;
import io.vitess.client.cursor.Row;
import io.vitess.proto.Query;
import io.vitess.proto.Query.Field;
import io.vitess.proto.Query.SplitQueryRequest.Algorithm;
import io.vitess.proto.Topodata.KeyRange;
import io.vitess.proto.Topodata.KeyspaceIdType;
import io.vitess.proto.Topodata.ShardReference;
import io.vitess.proto.Topodata.SrvKeyspace;
import io.vitess.proto.Topodata.SrvKeyspace.KeyspacePartition;
import io.vitess.proto.Topodata.TabletType;
import io.vitess.proto.Vtgate.SplitQueryResponse;
import io.vitess.proto.Vtgate.VStreamRequest;
import io.vitess.proto.Vtgate.VStreamResponse;
import io.vitess.proto.Vtrpc.CallerID;

import java.nio.charset.StandardCharsets;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.SQLInvalidAuthorizationSpecException;
import java.sql.SQLNonTransientException;
import java.sql.SQLRecoverableException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLTimeoutException;
import java.sql.SQLTransientException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * RpcClientTest tests a given implementation of RpcClient against a mock vtgate server
 * (go/cmd/vtgateclienttest).
 *
 * Each implementation should extend this class, add a @BeforeClass method that starts the
 * vtgateclienttest server with the necessary parameters and then set 'client'.
 */
public abstract class RpcClientTest {

  protected static RpcClient client;
  // ready is true when "vtgateclienttest" can accept RPCs. It is set by "waitForVtgateclienttest"
  // and reset to "false" at the start of each test class by "resetReady".
  private static boolean ready;

  private Context ctx;
  private VTGateBlockingConn conn;

  @BeforeClass
  public static void resetReady() throws Exception {
    ready = false;
  }

  @Before
  public void setUp() throws SQLException, InterruptedException {
    // Test VTGateConn via the synchronous VTGateBlockingConn wrapper.
    conn = new VTGateBlockingConn(client, KEYSPACE);

    waitForVtgateclienttest();

    // ctx is used by all RPCs within one test method. A deadline is set to cap test execution.
    // (RPCs will fail with DEADLINE_EXCEEDED if they keep using "ctx" 5 seconds from now.)
    ctx = Context.getDefault().withDeadlineAfter(Duration.standardSeconds(5))
        .withCallerId(CALLER_ID);
  }

  private static final String ECHO_PREFIX = "echo://";
  private static final String ERROR_PREFIX = "error://";
  private static final String PARTIAL_ERROR_PREFIX = "partialerror://";

  private static final ImmutableMap<String, Class<?>> EXECUTE_ERRORS =
      new ImmutableMap.Builder<String, Class<?>>()
          .put("bad input", SQLSyntaxErrorException.class)
          .put("deadline exceeded", SQLTimeoutException.class)
          .put("integrity error", SQLIntegrityConstraintViolationException.class)
          .put("transient error", SQLTransientException.class)
          .put("unauthenticated", SQLInvalidAuthorizationSpecException.class)
          .put("aborted", SQLRecoverableException.class)
          .put("unknown error", SQLNonTransientException.class)
          .build();

  private static final String QUERY = "test query with unicode: \u6211\u80fd\u541e\u4e0b\u73bb\u7483\u800c\u4e0d\u50b7\u8eab\u9ad4";
  private static final String KEYSPACE = "test_keyspace";

  private static final List<String> SHARDS = Arrays.asList("-80", "80-");
  private static final String SHARDS_ECHO = "[-80 80-]";

  private static final List<byte[]> KEYSPACE_IDS =
      Arrays.asList(new byte[]{1, 2, 3, 4}, new byte[]{5, 6, 7, 8});
  private static final String KEYSPACE_IDS_ECHO = "[[1 2 3 4] [5 6 7 8]]";

  private static final List<KeyRange> KEY_RANGES =
      Arrays.asList(KeyRange.newBuilder().setStart(ByteString.copyFrom(new byte[]{1, 2, 3, 4}))
          .setEnd(ByteString.copyFrom(new byte[]{5, 6, 7, 8})).build());
  private static final String KEY_RANGES_ECHO =
      "[start:\"\\001\\002\\003\\004\" end:\"\\005\\006\\007\\010\" ]";

  private static final ImmutableMap<byte[], Object> ENTITY_KEYSPACE_IDS =
      new ImmutableMap.Builder<byte[], Object>()
          .put(new byte[]{1, 2, 3}, 123)
          .put(new byte[]{4, 5, 6}, 2.5)
          .put(new byte[]{7, 8, 9}, new byte[]{1, 2, 3})
          .build();
  private static final String ENTITY_KEYSPACE_IDS_ECHO =
      "[type:INT64 value:\"123\" keyspace_id:\"\\001\\002\\003\"  type:FLOAT64 value:\"2.5\" keyspace_id:\"\\004\\005\\006\"  type:VARBINARY value:\"\\001\\002\\003\" keyspace_id:\"\\007\\010\\t\" ]";

  private static final TabletType TABLET_TYPE = TabletType.REPLICA;
  private static final String TABLET_TYPE_ECHO = TABLET_TYPE.toString();
  private static final Query.ExecuteOptions.IncludedFields ALL_FIELDS = Query.ExecuteOptions.IncludedFields.ALL;
  private static final String OPTIONS_ALL_FIELDS_ECHO =
      "included_fields:" + ALL_FIELDS.toString() + " ";

  private static final ImmutableMap<String, Object> BIND_VARS = new ImmutableMap.Builder<String, Object>()
      .put("int", 123)
      .put("float", 2.5)
      .put("bytes", new byte[]{1, 2, 3})
      .build();
  private static final String BIND_VARS_ECHO =
      "map[bytes:type:VARBINARY value:\"\\001\\002\\003\"  float:type:FLOAT64 value:\"2.5\"  int:type:INT64 value:\"123\" ]";

  private static final String SESSION_ECHO = "in_transaction:true ";

  private static final String NONTX_V3_SESSION_ECHO = "autocommit:true target_string:\"test_keyspace@replica\" options:<included_fields:ALL > ";

  private static final String V3_SESSION_ECHO = "in_transaction:true target_string:\"test_keyspace@replica\" options:<included_fields:ALL > ";

  private static final CallerID CALLER_ID = CallerID.newBuilder().setPrincipal("test_principal")
      .setComponent("test_component").setSubcomponent("test_subcomponent").build();
  private static final String CALLER_ID_ECHO =
      "principal:\"test_principal\" component:\"test_component\" subcomponent:\"test_subcomponent\" ";

  private static Map<String, String> getEcho(Cursor cursor) throws Exception {
    Map<String, String> values = new HashMap<String, String>();
    Map<String, Object> rawValues = new HashMap<String, Object>();

    // Echo values are stored as columns in the first row of the result.
    List<Field> fields = cursor.getFields();
    Row row = cursor.next();
    Assert.assertNotNull(row);
    int columnIndex = 1;
    for (Field field : fields) {
      byte[] bytes = row.getBytes(columnIndex);
      if (bytes != null) {
        values.put(field.getName(), new String(row.getBytes(columnIndex), StandardCharsets.UTF_8));
      }
      rawValues.put(field.getName(), row.getObject(columnIndex));
      ++columnIndex;
    }
    Assert.assertNull(cursor.next()); // There should only be one row.
    cursor.close();

    // Check NULL vs. empty string.
    Assert.assertTrue(rawValues.containsKey("null"));
    Assert.assertNull(rawValues.get("null"));
    Assert.assertTrue(values.containsKey("emptyString"));
    Assert.assertEquals("", values.get("emptyString"));

    return values;
  }

  /**
   * waitForVtgateclienttest blocks until the "vtgateclienttest" binary is reachable via RPC.
   *
   * We will constantly execute the "GetSrvKeyspace" RPC and return when the binary responded
   * successfully.
   */
  private void waitForVtgateclienttest() throws SQLException, InterruptedException {
    if (ready) {
      return;
    }

    DateTime start = DateTime.now();
    DateTime deadline = start.plusSeconds(60);

    boolean waited = false;
    while (DateTime.now().isBefore(deadline)) {
      try {
        ctx = Context.getDefault().withDeadlineAfter(Duration.standardSeconds(30));
        conn.getSrvKeyspace(ctx, "small");
        // RPC succeeded. Stop testing.
        break;
      } catch (SQLTransientException e) {
        Throwable rootCause = Throwables.getRootCause(e);
        if (!rootCause.getMessage().contains("Connection refused")) {
          // Non-retryable exception. Fail for good.
          throw e;
        }

        System.out
            .format("Waiting until vtgateclienttest is ready and responds (got exception: %s)\n",
                rootCause);
        Thread.sleep(100 /* milliseconds */);
        waited = true;
      }
    }

    if (waited) {
      double waitTimeSeconds = (DateTime.now().getMillis() - start.getMillis()) / 1000.0;
      System.out.format("Had to wait %.1f second(s) until vtgateclienttest was ready.\n",
          waitTimeSeconds);
    }
    ready = true;
  }

  @Test
  public void testEchoExecute() throws Exception {
    Map<String, String> echo;

    echo = getEcho(conn.execute(ctx, ECHO_PREFIX + QUERY, BIND_VARS, TABLET_TYPE, ALL_FIELDS));
    Assert.assertEquals(CALLER_ID_ECHO, echo.get("callerId"));
    Assert.assertEquals(ECHO_PREFIX + QUERY, echo.get("query"));
    Assert.assertEquals(BIND_VARS_ECHO, echo.get("bindVars"));
    Assert.assertEquals(NONTX_V3_SESSION_ECHO, echo.get("session"));

    echo = getEcho(
        conn.executeShards(ctx, ECHO_PREFIX + QUERY, KEYSPACE, SHARDS, BIND_VARS, TABLET_TYPE,
            ALL_FIELDS));
    Assert.assertEquals(CALLER_ID_ECHO, echo.get("callerId"));
    Assert.assertEquals(ECHO_PREFIX + QUERY, echo.get("query"));
    Assert.assertEquals(KEYSPACE, echo.get("keyspace"));
    Assert.assertEquals(SHARDS_ECHO, echo.get("shards"));
    Assert.assertEquals(BIND_VARS_ECHO, echo.get("bindVars"));
    Assert.assertEquals(TABLET_TYPE_ECHO, echo.get("tabletType"));
    Assert.assertEquals(OPTIONS_ALL_FIELDS_ECHO, echo.get("options"));

    echo = getEcho(conn.executeKeyspaceIds(ctx, ECHO_PREFIX + QUERY, KEYSPACE, KEYSPACE_IDS,
        BIND_VARS, TABLET_TYPE, ALL_FIELDS));
    Assert.assertEquals(CALLER_ID_ECHO, echo.get("callerId"));
    Assert.assertEquals(ECHO_PREFIX + QUERY, echo.get("query"));
    Assert.assertEquals(KEYSPACE, echo.get("keyspace"));
    Assert.assertEquals(KEYSPACE_IDS_ECHO, echo.get("keyspaceIds"));
    Assert.assertEquals(BIND_VARS_ECHO, echo.get("bindVars"));
    Assert.assertEquals(TABLET_TYPE_ECHO, echo.get("tabletType"));
    Assert.assertEquals(OPTIONS_ALL_FIELDS_ECHO, echo.get("options"));

    echo = getEcho(conn.executeKeyRanges(ctx, ECHO_PREFIX + QUERY, KEYSPACE, KEY_RANGES, BIND_VARS,
        TABLET_TYPE, ALL_FIELDS));
    Assert.assertEquals(CALLER_ID_ECHO, echo.get("callerId"));
    Assert.assertEquals(ECHO_PREFIX + QUERY, echo.get("query"));
    Assert.assertEquals(KEYSPACE, echo.get("keyspace"));
    Assert.assertEquals(KEY_RANGES_ECHO, echo.get("keyRanges"));
    Assert.assertEquals(BIND_VARS_ECHO, echo.get("bindVars"));
    Assert.assertEquals(TABLET_TYPE_ECHO, echo.get("tabletType"));
    Assert.assertEquals(OPTIONS_ALL_FIELDS_ECHO, echo.get("options"));

    echo = getEcho(conn.executeEntityIds(ctx, ECHO_PREFIX + QUERY, KEYSPACE, "column1",
        ENTITY_KEYSPACE_IDS, BIND_VARS, TABLET_TYPE, ALL_FIELDS));
    Assert.assertEquals(CALLER_ID_ECHO, echo.get("callerId"));
    Assert.assertEquals(ECHO_PREFIX + QUERY, echo.get("query"));
    Assert.assertEquals(KEYSPACE, echo.get("keyspace"));
    Assert.assertEquals("column1", echo.get("entityColumnName"));
    Assert.assertEquals(ENTITY_KEYSPACE_IDS_ECHO, echo.get("entityIds"));
    Assert.assertEquals(BIND_VARS_ECHO, echo.get("bindVars"));
    Assert.assertEquals(TABLET_TYPE_ECHO, echo.get("tabletType"));
    Assert.assertEquals(OPTIONS_ALL_FIELDS_ECHO, echo.get("options"));

    echo = getEcho(conn.executeBatchShards(ctx,
        Arrays.asList(Proto.bindShardQuery(KEYSPACE, SHARDS, ECHO_PREFIX + QUERY, BIND_VARS)),
        TABLET_TYPE, true, ALL_FIELDS).get(0));
    Assert.assertEquals(CALLER_ID_ECHO, echo.get("callerId"));
    Assert.assertEquals(ECHO_PREFIX + QUERY, echo.get("query"));
    Assert.assertEquals(KEYSPACE, echo.get("keyspace"));
    Assert.assertEquals(SHARDS_ECHO, echo.get("shards"));
    Assert.assertEquals(BIND_VARS_ECHO, echo.get("bindVars"));
    Assert.assertEquals(TABLET_TYPE_ECHO, echo.get("tabletType"));
    Assert.assertEquals("true", echo.get("asTransaction"));
    Assert.assertEquals(OPTIONS_ALL_FIELDS_ECHO, echo.get("options"));

    echo = getEcho(conn.executeBatchKeyspaceIds(ctx,
        Arrays.asList(
            Proto.bindKeyspaceIdQuery(KEYSPACE, KEYSPACE_IDS, ECHO_PREFIX + QUERY, BIND_VARS)),
        TABLET_TYPE, true, ALL_FIELDS).get(0));
    Assert.assertEquals(CALLER_ID_ECHO, echo.get("callerId"));
    Assert.assertEquals(ECHO_PREFIX + QUERY, echo.get("query"));
    Assert.assertEquals(KEYSPACE, echo.get("keyspace"));
    Assert.assertEquals(KEYSPACE_IDS_ECHO, echo.get("keyspaceIds"));
    Assert.assertEquals(BIND_VARS_ECHO, echo.get("bindVars"));
    Assert.assertEquals(TABLET_TYPE_ECHO, echo.get("tabletType"));
    Assert.assertEquals("true", echo.get("asTransaction"));
    Assert.assertEquals(OPTIONS_ALL_FIELDS_ECHO, echo.get("options"));
  }

  @Test
  public void testEchoStreamExecute() throws Exception {
    Map<String, String> echo;

    echo = getEcho(
        conn.streamExecute(ctx, ECHO_PREFIX + QUERY, BIND_VARS, TABLET_TYPE, ALL_FIELDS));
    Assert.assertEquals(CALLER_ID_ECHO, echo.get("callerId"));
    Assert.assertEquals(ECHO_PREFIX + QUERY, echo.get("query"));
    Assert.assertEquals(BIND_VARS_ECHO, echo.get("bindVars"));
    Assert.assertEquals(NONTX_V3_SESSION_ECHO, echo.get("session"));

    echo = getEcho(conn.streamExecuteShards(ctx, ECHO_PREFIX + QUERY, KEYSPACE, SHARDS, BIND_VARS,
        TABLET_TYPE, ALL_FIELDS));
    Assert.assertEquals(CALLER_ID_ECHO, echo.get("callerId"));
    Assert.assertEquals(ECHO_PREFIX + QUERY, echo.get("query"));
    Assert.assertEquals(KEYSPACE, echo.get("keyspace"));
    Assert.assertEquals(SHARDS_ECHO, echo.get("shards"));
    Assert.assertEquals(BIND_VARS_ECHO, echo.get("bindVars"));
    Assert.assertEquals(TABLET_TYPE_ECHO, echo.get("tabletType"));
    Assert.assertEquals(OPTIONS_ALL_FIELDS_ECHO, echo.get("options"));

    echo = getEcho(conn.streamExecuteKeyspaceIds(ctx, ECHO_PREFIX + QUERY, KEYSPACE, KEYSPACE_IDS,
        BIND_VARS, TABLET_TYPE, ALL_FIELDS));
    Assert.assertEquals(CALLER_ID_ECHO, echo.get("callerId"));
    Assert.assertEquals(ECHO_PREFIX + QUERY, echo.get("query"));
    Assert.assertEquals(KEYSPACE, echo.get("keyspace"));
    Assert.assertEquals(KEYSPACE_IDS_ECHO, echo.get("keyspaceIds"));
    Assert.assertEquals(BIND_VARS_ECHO, echo.get("bindVars"));
    Assert.assertEquals(TABLET_TYPE_ECHO, echo.get("tabletType"));
    Assert.assertEquals(OPTIONS_ALL_FIELDS_ECHO, echo.get("options"));

    echo = getEcho(conn.streamExecuteKeyRanges(ctx, ECHO_PREFIX + QUERY, KEYSPACE, KEY_RANGES,
        BIND_VARS, TABLET_TYPE, ALL_FIELDS));
    Assert.assertEquals(CALLER_ID_ECHO, echo.get("callerId"));
    Assert.assertEquals(ECHO_PREFIX + QUERY, echo.get("query"));
    Assert.assertEquals(KEYSPACE, echo.get("keyspace"));
    Assert.assertEquals(KEY_RANGES_ECHO, echo.get("keyRanges"));
    Assert.assertEquals(BIND_VARS_ECHO, echo.get("bindVars"));
    Assert.assertEquals(TABLET_TYPE_ECHO, echo.get("tabletType"));
    Assert.assertEquals(OPTIONS_ALL_FIELDS_ECHO, echo.get("options"));
  }

  @Test
  public void testEchoTransactionExecute() throws Exception {
    Map<String, String> echo;

    VTGateBlockingTx tx = conn.begin(ctx);

    echo = getEcho(tx.execute(ctx, ECHO_PREFIX + QUERY, BIND_VARS, TABLET_TYPE, ALL_FIELDS));
    Assert.assertEquals(CALLER_ID_ECHO, echo.get("callerId"));
    Assert.assertEquals(ECHO_PREFIX + QUERY, echo.get("query"));
    Assert.assertEquals(BIND_VARS_ECHO, echo.get("bindVars"));
    Assert.assertEquals(V3_SESSION_ECHO, echo.get("session"));

    // V3 returns additional session artifacts that V2
    // doesn't care about. So, start with a new session
    // before testing V2 functionality.
    tx.rollback(ctx);
    tx = conn.begin(ctx);

    echo = getEcho(
        tx.executeShards(ctx, ECHO_PREFIX + QUERY, KEYSPACE, SHARDS, BIND_VARS, TABLET_TYPE,
            ALL_FIELDS));
    Assert.assertEquals(CALLER_ID_ECHO, echo.get("callerId"));
    Assert.assertEquals(ECHO_PREFIX + QUERY, echo.get("query"));
    Assert.assertEquals(KEYSPACE, echo.get("keyspace"));
    Assert.assertEquals(SHARDS_ECHO, echo.get("shards"));
    Assert.assertEquals(BIND_VARS_ECHO, echo.get("bindVars"));
    Assert.assertEquals(TABLET_TYPE_ECHO, echo.get("tabletType"));
    Assert.assertEquals(SESSION_ECHO, echo.get("session"));
    Assert.assertEquals("false", echo.get("notInTransaction"));
    Assert.assertEquals(OPTIONS_ALL_FIELDS_ECHO, echo.get("options"));

    echo = getEcho(tx.executeKeyspaceIds(ctx, ECHO_PREFIX + QUERY, KEYSPACE, KEYSPACE_IDS,
        BIND_VARS, TABLET_TYPE, ALL_FIELDS));
    Assert.assertEquals(CALLER_ID_ECHO, echo.get("callerId"));
    Assert.assertEquals(ECHO_PREFIX + QUERY, echo.get("query"));
    Assert.assertEquals(KEYSPACE, echo.get("keyspace"));
    Assert.assertEquals(KEYSPACE_IDS_ECHO, echo.get("keyspaceIds"));
    Assert.assertEquals(BIND_VARS_ECHO, echo.get("bindVars"));
    Assert.assertEquals(TABLET_TYPE_ECHO, echo.get("tabletType"));
    Assert.assertEquals(SESSION_ECHO, echo.get("session"));
    Assert.assertEquals("false", echo.get("notInTransaction"));
    Assert.assertEquals(OPTIONS_ALL_FIELDS_ECHO, echo.get("options"));

    echo = getEcho(tx.executeKeyRanges(ctx, ECHO_PREFIX + QUERY, KEYSPACE, KEY_RANGES, BIND_VARS,
        TABLET_TYPE, ALL_FIELDS));
    Assert.assertEquals(CALLER_ID_ECHO, echo.get("callerId"));
    Assert.assertEquals(ECHO_PREFIX + QUERY, echo.get("query"));
    Assert.assertEquals(KEYSPACE, echo.get("keyspace"));
    Assert.assertEquals(KEY_RANGES_ECHO, echo.get("keyRanges"));
    Assert.assertEquals(BIND_VARS_ECHO, echo.get("bindVars"));
    Assert.assertEquals(TABLET_TYPE_ECHO, echo.get("tabletType"));
    Assert.assertEquals(SESSION_ECHO, echo.get("session"));
    Assert.assertEquals("false", echo.get("notInTransaction"));
    Assert.assertEquals(OPTIONS_ALL_FIELDS_ECHO, echo.get("options"));

    echo = getEcho(tx.executeEntityIds(ctx, ECHO_PREFIX + QUERY, KEYSPACE, "column1",
        ENTITY_KEYSPACE_IDS, BIND_VARS, TABLET_TYPE, ALL_FIELDS));
    Assert.assertEquals(CALLER_ID_ECHO, echo.get("callerId"));
    Assert.assertEquals(ECHO_PREFIX + QUERY, echo.get("query"));
    Assert.assertEquals(KEYSPACE, echo.get("keyspace"));
    Assert.assertEquals("column1", echo.get("entityColumnName"));
    Assert.assertEquals(ENTITY_KEYSPACE_IDS_ECHO, echo.get("entityIds"));
    Assert.assertEquals(BIND_VARS_ECHO, echo.get("bindVars"));
    Assert.assertEquals(TABLET_TYPE_ECHO, echo.get("tabletType"));
    Assert.assertEquals(SESSION_ECHO, echo.get("session"));
    Assert.assertEquals("false", echo.get("notInTransaction"));
    Assert.assertEquals(OPTIONS_ALL_FIELDS_ECHO, echo.get("options"));

    tx.rollback(ctx);
    tx = conn.begin(ctx);

    echo = getEcho(tx.executeBatchShards(ctx,
        Arrays.asList(Proto.bindShardQuery(KEYSPACE, SHARDS, ECHO_PREFIX + QUERY, BIND_VARS)),
        TABLET_TYPE, ALL_FIELDS).get(0));
    Assert.assertEquals(CALLER_ID_ECHO, echo.get("callerId"));
    Assert.assertEquals(ECHO_PREFIX + QUERY, echo.get("query"));
    Assert.assertEquals(KEYSPACE, echo.get("keyspace"));
    Assert.assertEquals(SHARDS_ECHO, echo.get("shards"));
    Assert.assertEquals(BIND_VARS_ECHO, echo.get("bindVars"));
    Assert.assertEquals(TABLET_TYPE_ECHO, echo.get("tabletType"));
    Assert.assertEquals(SESSION_ECHO, echo.get("session"));
    Assert.assertEquals("false", echo.get("asTransaction"));
    Assert.assertEquals(OPTIONS_ALL_FIELDS_ECHO, echo.get("options"));

    echo = getEcho(tx.executeBatchKeyspaceIds(ctx,
        Arrays.asList(
            Proto.bindKeyspaceIdQuery(KEYSPACE, KEYSPACE_IDS, ECHO_PREFIX + QUERY, BIND_VARS)),
        TABLET_TYPE, ALL_FIELDS).get(0));
    Assert.assertEquals(CALLER_ID_ECHO, echo.get("callerId"));
    Assert.assertEquals(ECHO_PREFIX + QUERY, echo.get("query"));
    Assert.assertEquals(KEYSPACE, echo.get("keyspace"));
    Assert.assertEquals(KEYSPACE_IDS_ECHO, echo.get("keyspaceIds"));
    Assert.assertEquals(BIND_VARS_ECHO, echo.get("bindVars"));
    Assert.assertEquals(TABLET_TYPE_ECHO, echo.get("tabletType"));
    Assert.assertEquals(SESSION_ECHO, echo.get("session"));
    Assert.assertEquals("false", echo.get("asTransaction"));
    Assert.assertEquals(OPTIONS_ALL_FIELDS_ECHO, echo.get("options"));

    tx.commit(ctx);
  }

  @Test
  public void testEchoSplitQuery() throws Exception {
    SplitQueryResponse.Part expected = SplitQueryResponse.Part.newBuilder()
        .setQuery(Proto.bindQuery(
            ECHO_PREFIX + QUERY + ":[split_column1 split_column2]:123:1000:FULL_SCAN",
            BIND_VARS))
        .setKeyRangePart(SplitQueryResponse.KeyRangePart.newBuilder().setKeyspace(KEYSPACE).build())
        .build();
    SplitQueryResponse.Part actual =
        conn.splitQuery(
            ctx,
            KEYSPACE,
            ECHO_PREFIX + QUERY,
            BIND_VARS,
            ImmutableList.of("split_column1", "split_column2"),
            123,
            1000,
            Algorithm.FULL_SCAN)
            .get(0);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testGetSrvKeyspace() throws Exception {
    SrvKeyspace expected = SrvKeyspace.newBuilder()
        .addPartitions(KeyspacePartition.newBuilder().setServedType(TabletType.REPLICA)
            .addShardReferences(ShardReference.newBuilder().setName("shard0").setKeyRange(KeyRange
                .newBuilder().setStart(ByteString.copyFrom(new byte[]{0x40, 0, 0, 0, 0, 0, 0, 0}))
                .setEnd(ByteString.copyFrom(new byte[]{(byte) 0x80, 0, 0, 0, 0, 0, 0, 0})).build())
                .build())
            .build())
        .setShardingColumnName("sharding_column_name")
        .setShardingColumnType(KeyspaceIdType.UINT64).addServedFrom(SrvKeyspace.ServedFrom
            .newBuilder().setTabletType(TabletType.MASTER).setKeyspace("other_keyspace").build())
        .build();
    SrvKeyspace actual = conn.getSrvKeyspace(ctx, "big");
    Assert.assertEquals(expected, actual);
  }

  abstract static class Executable {

    abstract void execute(String query) throws Exception;
  }

  void checkExecuteErrors(Executable exe, boolean partial) {
    for (String error : EXECUTE_ERRORS.keySet()) {
      Class<?> cls = EXECUTE_ERRORS.get(error);

      try {
        String query = ERROR_PREFIX + error;
        exe.execute(query);
        Assert.fail("no exception thrown for " + query);
      } catch (Exception e) {
        Assert.assertEquals(cls, e.getClass());
      }

      if (partial) {
        try {
          String query = PARTIAL_ERROR_PREFIX + error;
          exe.execute(query);
          Assert.fail("no exception thrown for " + query);
        } catch (Exception e) {
          Assert.assertEquals(cls, e.getClass());
        }
      }
    }
  }

  void checkExecuteErrors(Executable exe) {
    checkExecuteErrors(exe, true);
  }

  void checkStreamExecuteErrors(Executable exe) {
    // Streaming calls don't have partial errors.
    checkExecuteErrors(exe, false);
  }

  abstract static class TransactionExecutable {

    abstract void execute(VTGateBlockingTx tx, String query) throws Exception;
  }

  void checkTransactionExecuteErrors(TransactionExecutable exe) throws Exception {
    for (String error : EXECUTE_ERRORS.keySet()) {
      Class<?> cls = EXECUTE_ERRORS.get(error);

      try {
        VTGateBlockingTx tx = conn.begin(ctx);
        String query = ERROR_PREFIX + error;
        exe.execute(tx, query);
        Assert.fail("no exception thrown for " + query);
      } catch (Exception e) {
        Assert.assertEquals(cls, e.getClass());

        if (error.equals("integrity error")) {
          // The mock test server sends back errno:1062 sqlstate:23000 for this case.
          // Make sure these values get properly extracted by the client.
          SQLException sqlException = (SQLException) e;
          Assert.assertEquals(1062, sqlException.getErrorCode());
          Assert.assertEquals("23000", sqlException.getSQLState());
        }
      }

      // Don't close the transaction on partial error.
      VTGateBlockingTx tx = conn.begin(ctx);
      try {
        String query = PARTIAL_ERROR_PREFIX + error;
        exe.execute(tx, query);
        Assert.fail("no exception thrown for " + query);
      } catch (Exception e) {
        Assert.assertEquals(cls, e.getClass());

        if (error.equals("integrity error")) {
          // The mock test server sends back errno:1062 sqlstate:23000 for this case.
          // Make sure these values get properly extracted by the client.
          SQLException sqlException = (SQLException) e;
          Assert.assertEquals(1062, sqlException.getErrorCode());
          Assert.assertEquals("23000", sqlException.getSQLState());
        }
      }
      // The transaction should still be usable now.
      tx.rollback(ctx);

      // Close the transaction on partial error.
      tx = conn.begin(ctx);
      try {
        String query = PARTIAL_ERROR_PREFIX + error + "/close transaction";
        exe.execute(tx, query);
        Assert.fail("no exception thrown for " + query);
      } catch (Exception e) {
        Assert.assertEquals(cls, e.getClass());
      }
      // The transaction should be unusable now.
      try {
        tx.rollback(ctx);
        Assert.fail("no exception thrown for rollback() after closed transaction");
      } catch (Exception e) {
        Assert.assertEquals(SQLDataException.class, e.getClass());
        Assert.assertEquals(true, e.getMessage().contains("not in transaction"));
      }
    }
  }

  @Test
  public void testExecuteErrors() throws Exception {
    checkExecuteErrors(new Executable() {
      @Override
      void execute(String query) throws Exception {
        conn.execute(ctx, query, BIND_VARS, TABLET_TYPE, ALL_FIELDS);
      }
    });
    checkExecuteErrors(new Executable() {
      @Override
      void execute(String query) throws Exception {
        conn.executeShards(ctx, query, KEYSPACE, SHARDS, BIND_VARS, TABLET_TYPE, ALL_FIELDS);
      }
    });
    checkExecuteErrors(new Executable() {
      @Override
      void execute(String query) throws Exception {
        conn.executeKeyspaceIds(ctx, query, KEYSPACE, KEYSPACE_IDS, BIND_VARS, TABLET_TYPE,
            ALL_FIELDS);
      }
    });
    checkExecuteErrors(new Executable() {
      @Override
      void execute(String query) throws Exception {
        conn.executeKeyRanges(ctx, query, KEYSPACE, KEY_RANGES, BIND_VARS, TABLET_TYPE, ALL_FIELDS);
      }
    });
    checkExecuteErrors(new Executable() {
      @Override
      void execute(String query) throws Exception {
        conn.executeEntityIds(ctx, query, KEYSPACE, "column1", ENTITY_KEYSPACE_IDS, BIND_VARS,
            TABLET_TYPE, ALL_FIELDS);
      }
    });
    checkExecuteErrors(new Executable() {
      @Override
      void execute(String query) throws Exception {
        conn.executeBatchShards(ctx,
            Arrays.asList(Proto.bindShardQuery(KEYSPACE, SHARDS, query, BIND_VARS)), TABLET_TYPE,
            true, ALL_FIELDS);
      }
    });
    checkExecuteErrors(new Executable() {
      @Override
      void execute(String query) throws Exception {
        conn.executeBatchKeyspaceIds(ctx,
            Arrays.asList(Proto.bindKeyspaceIdQuery(KEYSPACE, KEYSPACE_IDS, query, BIND_VARS)),
            TABLET_TYPE, true, ALL_FIELDS);
      }
    });
  }

  @Test
  public void testStreamExecuteErrors() throws Exception {
    checkStreamExecuteErrors(new Executable() {
      @Override
      void execute(String query) throws Exception {
        conn.streamExecute(ctx, query, BIND_VARS, TABLET_TYPE, ALL_FIELDS).next();
      }
    });
    checkStreamExecuteErrors(new Executable() {
      @Override
      void execute(String query) throws Exception {
        conn.streamExecuteShards(ctx, query, KEYSPACE, SHARDS, BIND_VARS, TABLET_TYPE, ALL_FIELDS)
            .next();
      }
    });
    checkStreamExecuteErrors(new Executable() {
      @Override
      void execute(String query) throws Exception {
        conn.streamExecuteKeyspaceIds(ctx, query, KEYSPACE, KEYSPACE_IDS, BIND_VARS, TABLET_TYPE,
            ALL_FIELDS)
            .next();
      }
    });
    checkStreamExecuteErrors(new Executable() {
      @Override
      void execute(String query) throws Exception {
        conn.streamExecuteKeyRanges(ctx, query, KEYSPACE, KEY_RANGES, BIND_VARS, TABLET_TYPE,
            ALL_FIELDS)
            .next();
      }
    });
  }

  @Test
  public void testTransactionExecuteErrors() throws Exception {
    checkTransactionExecuteErrors(new TransactionExecutable() {
      @Override
      void execute(VTGateBlockingTx tx, String query) throws Exception {
        tx.execute(ctx, query, BIND_VARS, TABLET_TYPE, ALL_FIELDS);
      }
    });
    checkTransactionExecuteErrors(new TransactionExecutable() {
      @Override
      void execute(VTGateBlockingTx tx, String query) throws Exception {
        tx.executeShards(ctx, query, KEYSPACE, SHARDS, BIND_VARS, TABLET_TYPE, ALL_FIELDS);
      }
    });
    checkTransactionExecuteErrors(new TransactionExecutable() {
      @Override
      void execute(VTGateBlockingTx tx, String query) throws Exception {
        tx.executeKeyspaceIds(ctx, query, KEYSPACE, KEYSPACE_IDS, BIND_VARS, TABLET_TYPE,
            ALL_FIELDS);
      }
    });
    checkTransactionExecuteErrors(new TransactionExecutable() {
      @Override
      void execute(VTGateBlockingTx tx, String query) throws Exception {
        tx.executeKeyRanges(ctx, query, KEYSPACE, KEY_RANGES, BIND_VARS, TABLET_TYPE, ALL_FIELDS);
      }
    });
    checkTransactionExecuteErrors(new TransactionExecutable() {
      @Override
      void execute(VTGateBlockingTx tx, String query) throws Exception {
        tx.executeEntityIds(ctx, query, KEYSPACE, "column1", ENTITY_KEYSPACE_IDS, BIND_VARS,
            TABLET_TYPE, ALL_FIELDS);
      }
    });
    checkTransactionExecuteErrors(new TransactionExecutable() {
      @Override
      void execute(VTGateBlockingTx tx, String query) throws Exception {
        tx.executeBatchShards(ctx,
            Arrays.asList(Proto.bindShardQuery(KEYSPACE, SHARDS, query, BIND_VARS)), TABLET_TYPE,
            ALL_FIELDS);
      }
    });
    checkTransactionExecuteErrors(new TransactionExecutable() {
      @Override
      void execute(VTGateBlockingTx tx, String query) throws Exception {
        tx.executeBatchKeyspaceIds(ctx,
            Arrays.asList(Proto.bindKeyspaceIdQuery(KEYSPACE, KEYSPACE_IDS, query, BIND_VARS)),
            TABLET_TYPE, ALL_FIELDS);
      }
    });
  }

  @Test
  public void testVStream() throws Exception {
    VGtid vgtid = VGtid.newBuilder()
        .addShardGtids(ShardGtid.newBuilder()
            .setGtid("gtid")
            .setShard(ECHO_PREFIX + System.currentTimeMillis())
            .setKeyspace("keyspace: " + System.currentTimeMillis())
            .build())
        .build();

    VStreamRequest vstreamRequest = VStreamRequest.newBuilder()
        .setCallerId(CALLER_ID)
        .setVgtid(vgtid)
        .setTabletType(TABLET_TYPE)
        .build();

    StreamIterator<VStreamResponse> vstream = client.getVStream(ctx, vstreamRequest);
    VStreamResponse actual = vstream.next();
    Assert.assertFalse(vstream.hasNext());

    VStreamResponse expected = VStreamResponse.newBuilder()
        .addEvents(VEvent.newBuilder()
            .setType(VEventType.forNumber(1))
            .setTimestamp(1234)
            .setGtid("echo-gtid-1")
            .setDdl("echo-ddl-1")
            .setVgtid(vgtid)
            .setRowEvent(RowEvent.newBuilder()
                .setTableName("echo-table-1")
                .build())
            .build())
        .addEvents(VEvent.newBuilder()
            .setType(VEventType.forNumber(2))
            .setTimestamp(4321)
            .setGtid("echo-gtid-2")
            .setDdl("echo-ddl-2")
            .setVgtid(vgtid)
            .setFieldEvent(FieldEvent.newBuilder()
                .setTableName("echo-table-2")
                .build())
            .build())
        .build();

    Assert.assertEquals(expected, actual);
  }

}
