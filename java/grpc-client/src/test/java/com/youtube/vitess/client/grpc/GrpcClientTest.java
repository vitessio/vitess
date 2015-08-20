package com.youtube.vitess.client.grpc;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;

import com.youtube.vitess.client.Context;
import com.youtube.vitess.client.Proto;
import com.youtube.vitess.client.RpcClient;
import com.youtube.vitess.client.VTGateConn;
import com.youtube.vitess.client.VTGateTx;
import com.youtube.vitess.proto.Query.QueryResult;
import com.youtube.vitess.proto.Topodata.KeyRange;
import com.youtube.vitess.proto.Topodata.KeyspaceIdType;
import com.youtube.vitess.proto.Topodata.ShardReference;
import com.youtube.vitess.proto.Topodata.SrvKeyspace;
import com.youtube.vitess.proto.Topodata.SrvKeyspace.KeyspacePartition;
import com.youtube.vitess.proto.Topodata.TabletType;
import com.youtube.vitess.proto.Vtgate.SplitQueryResponse;
import com.youtube.vitess.proto.Vtrpc.CallerID;

import org.joda.time.Duration;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This tests GrpcClient with a mock vtgate server (go/cmd/vtgateclienttest).
 */
public class GrpcClientTest {
  private static Process vtgateclienttest;
  private static int grpc_port;
  private static RpcClient client;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    String vtRoot = System.getenv("VTROOT");
    if (vtRoot == null) {
      throw new RuntimeException("cannot find env variable VTROOT; make sure to source dev.env");
    }

    ServerSocket socket = new ServerSocket(0);
    grpc_port = socket.getLocalPort();
    socket.close();

    vtgateclienttest =
        new ProcessBuilder(
            Arrays.asList(vtRoot + "/bin/vtgateclienttest", "-logtostderr", "-grpc_port",
                Integer.toString(grpc_port), "-service_map", "grpc-vtgateservice")).start();

    client = new GrpcClientFactory().create(
        Context.getDefault().withDeadlineAfter(Duration.millis(5000)),
        new InetSocketAddress("localhost", grpc_port));
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    if (client != null) {
      client.close();
    }
    if (vtgateclienttest != null) {
      vtgateclienttest.destroy();
    }
  }

  private Context ctx;
  private VTGateConn conn;

  @Before
  public void setUp() {
    ctx = Context.getDefault().withDeadlineAfter(Duration.millis(5000)).withCallerId(CALLER_ID);
    conn = new VTGateConn(client);
  }

  private static final String ECHO_PREFIX = "echo://";

  private static final String QUERY = "test query";
  private static final String KEYSPACE = "test_keyspace";

  private static final List<String> SHARDS = Arrays.asList("-80", "80-");
  private static final String SHARDS_ECHO = "[-80 80-]";

  private static final List<byte[]> KEYSPACE_IDS =
      Arrays.asList(new byte[] {1, 2, 3, 4}, new byte[] {5, 6, 7, 8});
  private static final String KEYSPACE_IDS_ECHO = "[01020304 05060708]";

  private static final List<KeyRange> KEY_RANGES = Arrays.asList(
      KeyRange.newBuilder()
          .setStart(ByteString.copyFrom(new byte[] {1, 2, 3, 4}))
          .setEnd(ByteString.copyFrom(new byte[] {5, 6, 7, 8}))
          .build());
  private static final String KEY_RANGES_ECHO = "[{Start: 01020304, End: 05060708}]";

  private static final Map<byte[], Object> ENTITY_KEYSPACE_IDS =
      new ImmutableMap.Builder<byte[], Object>()
          .put(new byte[] {1, 2, 3}, 123)
          .put(new byte[] {4, 5, 6}, 2.0)
          .put(new byte[] {7, 8, 9}, new byte[] {1, 2, 3})
          .build();
  private static final String ENTITY_KEYSPACE_IDS_ECHO =
      "[{123 010203} {2 040506} {[1 2 3] 070809}]";

  private static final TabletType TABLET_TYPE = TabletType.REPLICA;
  private static final String TABLET_TYPE_ECHO = TABLET_TYPE.toString();

  private static final Map<String, Object> BIND_VARS =
      new ImmutableMap.Builder<String, Object>()
          .put("int", 123)
          .put("float", 2.0)
          .put("bytes", new byte[] {1, 2, 3})
          .build();
  private static final String BIND_VARS_ECHO = "map[bytes:[1 2 3] float:2 int:123]";

  private static final String SESSION_ECHO = "InTransaction: true, ShardSession: []";

  private static final CallerID CALLER_ID =
      CallerID.newBuilder()
          .setPrincipal("test_principal")
          .setComponent("test_component")
          .setSubcomponent("test_subcomponent")
          .build();
  private static final String CALLER_ID_ECHO =
      "principal:\"test_principal\" component:\"test_component\" subcomponent:\"test_subcomponent\" ";

  private static Map<String, String> getEcho(QueryResult result) {
    Map<String, String> fields = new HashMap<String, String>();
    for (int i = 0; i < result.getFieldsCount(); i++) {
      fields.put(result.getFields(i).getName(), result.getRows(0).getValues(i).toStringUtf8());
    }
    return fields;
  }

  @Test
  public void testEchoExecute() throws Exception {
    Map<String, String> echo;

    echo = getEcho(conn.execute(ctx, ECHO_PREFIX + QUERY, BIND_VARS, TABLET_TYPE));
    Assert.assertEquals(CALLER_ID_ECHO, echo.get("callerId"));
    Assert.assertEquals(ECHO_PREFIX + QUERY, echo.get("query"));
    Assert.assertEquals(BIND_VARS_ECHO, echo.get("bindVars"));
    Assert.assertEquals(TABLET_TYPE_ECHO, echo.get("tabletType"));

    echo = getEcho(
        conn.executeShards(ctx, ECHO_PREFIX + QUERY, KEYSPACE, SHARDS, BIND_VARS, TABLET_TYPE));
    Assert.assertEquals(CALLER_ID_ECHO, echo.get("callerId"));
    Assert.assertEquals(ECHO_PREFIX + QUERY, echo.get("query"));
    Assert.assertEquals(KEYSPACE, echo.get("keyspace"));
    Assert.assertEquals(SHARDS_ECHO, echo.get("shards"));
    Assert.assertEquals(BIND_VARS_ECHO, echo.get("bindVars"));
    Assert.assertEquals(TABLET_TYPE_ECHO, echo.get("tabletType"));

    echo = getEcho(conn.executeKeyspaceIds(
        ctx, ECHO_PREFIX + QUERY, KEYSPACE, KEYSPACE_IDS, BIND_VARS, TABLET_TYPE));
    Assert.assertEquals(CALLER_ID_ECHO, echo.get("callerId"));
    Assert.assertEquals(ECHO_PREFIX + QUERY, echo.get("query"));
    Assert.assertEquals(KEYSPACE, echo.get("keyspace"));
    Assert.assertEquals(KEYSPACE_IDS_ECHO, echo.get("keyspaceIds"));
    Assert.assertEquals(BIND_VARS_ECHO, echo.get("bindVars"));
    Assert.assertEquals(TABLET_TYPE_ECHO, echo.get("tabletType"));

    echo = getEcho(conn.executeKeyRanges(
        ctx, ECHO_PREFIX + QUERY, KEYSPACE, KEY_RANGES, BIND_VARS, TABLET_TYPE));
    Assert.assertEquals(CALLER_ID_ECHO, echo.get("callerId"));
    Assert.assertEquals(ECHO_PREFIX + QUERY, echo.get("query"));
    Assert.assertEquals(KEYSPACE, echo.get("keyspace"));
    Assert.assertEquals(KEY_RANGES_ECHO, echo.get("keyRanges"));
    Assert.assertEquals(BIND_VARS_ECHO, echo.get("bindVars"));
    Assert.assertEquals(TABLET_TYPE_ECHO, echo.get("tabletType"));

    echo = getEcho(conn.executeEntityIds(ctx, ECHO_PREFIX + QUERY, KEYSPACE, "column1",
        ENTITY_KEYSPACE_IDS, BIND_VARS, TABLET_TYPE));
    Assert.assertEquals(CALLER_ID_ECHO, echo.get("callerId"));
    Assert.assertEquals(ECHO_PREFIX + QUERY, echo.get("query"));
    Assert.assertEquals(KEYSPACE, echo.get("keyspace"));
    Assert.assertEquals("column1", echo.get("entityColumnName"));
    Assert.assertEquals(ENTITY_KEYSPACE_IDS_ECHO, echo.get("entityIds"));
    Assert.assertEquals(BIND_VARS_ECHO, echo.get("bindVars"));
    Assert.assertEquals(TABLET_TYPE_ECHO, echo.get("tabletType"));

    echo = getEcho(conn.executeBatchShards(ctx, Arrays.asList(Proto.bindShardQuery(KEYSPACE, SHARDS,
                                                    ECHO_PREFIX + QUERY, BIND_VARS)),
                           TABLET_TYPE, true).get(0));
    Assert.assertEquals(CALLER_ID_ECHO, echo.get("callerId"));
    Assert.assertEquals(ECHO_PREFIX + QUERY, echo.get("query"));
    Assert.assertEquals(KEYSPACE, echo.get("keyspace"));
    Assert.assertEquals(SHARDS_ECHO, echo.get("shards"));
    Assert.assertEquals(BIND_VARS_ECHO, echo.get("bindVars"));
    Assert.assertEquals(TABLET_TYPE_ECHO, echo.get("tabletType"));

    echo = getEcho(
        conn.executeBatchKeyspaceIds(ctx, Arrays.asList(Proto.bindKeyspaceIdQuery(KEYSPACE,
                                              KEYSPACE_IDS, ECHO_PREFIX + QUERY, BIND_VARS)),
                TABLET_TYPE, true).get(0));
    Assert.assertEquals(CALLER_ID_ECHO, echo.get("callerId"));
    Assert.assertEquals(ECHO_PREFIX + QUERY, echo.get("query"));
    Assert.assertEquals(KEYSPACE, echo.get("keyspace"));
    Assert.assertEquals(KEYSPACE_IDS_ECHO, echo.get("keyspaceIds"));
    Assert.assertEquals(BIND_VARS_ECHO, echo.get("bindVars"));
    Assert.assertEquals(TABLET_TYPE_ECHO, echo.get("tabletType"));
  }

  @Test
  public void testEchoStreamExecute() throws Exception {
    Map<String, String> echo;

    echo = getEcho(conn.streamExecute(ctx, ECHO_PREFIX + QUERY, BIND_VARS, TABLET_TYPE).next());
    Assert.assertEquals(CALLER_ID_ECHO, echo.get("callerId"));
    Assert.assertEquals(ECHO_PREFIX + QUERY, echo.get("query"));
    Assert.assertEquals(BIND_VARS_ECHO, echo.get("bindVars"));
    Assert.assertEquals(TABLET_TYPE_ECHO, echo.get("tabletType"));

    echo = getEcho(
        conn.streamExecuteShards(ctx, ECHO_PREFIX + QUERY, KEYSPACE, SHARDS, BIND_VARS, TABLET_TYPE)
            .next());
    Assert.assertEquals(CALLER_ID_ECHO, echo.get("callerId"));
    Assert.assertEquals(ECHO_PREFIX + QUERY, echo.get("query"));
    Assert.assertEquals(KEYSPACE, echo.get("keyspace"));
    Assert.assertEquals(SHARDS_ECHO, echo.get("shards"));
    Assert.assertEquals(BIND_VARS_ECHO, echo.get("bindVars"));
    Assert.assertEquals(TABLET_TYPE_ECHO, echo.get("tabletType"));

    echo = getEcho(conn.streamExecuteKeyspaceIds(ctx, ECHO_PREFIX + QUERY, KEYSPACE, KEYSPACE_IDS,
                           BIND_VARS, TABLET_TYPE).next());
    Assert.assertEquals(CALLER_ID_ECHO, echo.get("callerId"));
    Assert.assertEquals(ECHO_PREFIX + QUERY, echo.get("query"));
    Assert.assertEquals(KEYSPACE, echo.get("keyspace"));
    Assert.assertEquals(KEYSPACE_IDS_ECHO, echo.get("keyspaceIds"));
    Assert.assertEquals(BIND_VARS_ECHO, echo.get("bindVars"));
    Assert.assertEquals(TABLET_TYPE_ECHO, echo.get("tabletType"));

    echo = getEcho(conn.streamExecuteKeyRanges(ctx, ECHO_PREFIX + QUERY, KEYSPACE, KEY_RANGES,
                           BIND_VARS, TABLET_TYPE).next());
    Assert.assertEquals(CALLER_ID_ECHO, echo.get("callerId"));
    Assert.assertEquals(ECHO_PREFIX + QUERY, echo.get("query"));
    Assert.assertEquals(KEYSPACE, echo.get("keyspace"));
    Assert.assertEquals(KEY_RANGES_ECHO, echo.get("keyRanges"));
    Assert.assertEquals(BIND_VARS_ECHO, echo.get("bindVars"));
    Assert.assertEquals(TABLET_TYPE_ECHO, echo.get("tabletType"));
  }

  @Test
  public void testEchoTransactionExecute() throws Exception {
    Map<String, String> echo;

    VTGateTx tx = conn.begin(ctx);

    echo = getEcho(tx.execute(ctx, ECHO_PREFIX + QUERY, BIND_VARS, TABLET_TYPE));
    Assert.assertEquals(CALLER_ID_ECHO, echo.get("callerId"));
    Assert.assertEquals(ECHO_PREFIX + QUERY, echo.get("query"));
    Assert.assertEquals(BIND_VARS_ECHO, echo.get("bindVars"));
    Assert.assertEquals(TABLET_TYPE_ECHO, echo.get("tabletType"));
    Assert.assertEquals(SESSION_ECHO, echo.get("session"));

    echo = getEcho(
        tx.executeShards(ctx, ECHO_PREFIX + QUERY, KEYSPACE, SHARDS, BIND_VARS, TABLET_TYPE));
    Assert.assertEquals(CALLER_ID_ECHO, echo.get("callerId"));
    Assert.assertEquals(ECHO_PREFIX + QUERY, echo.get("query"));
    Assert.assertEquals(KEYSPACE, echo.get("keyspace"));
    Assert.assertEquals(SHARDS_ECHO, echo.get("shards"));
    Assert.assertEquals(BIND_VARS_ECHO, echo.get("bindVars"));
    Assert.assertEquals(TABLET_TYPE_ECHO, echo.get("tabletType"));
    Assert.assertEquals(SESSION_ECHO, echo.get("session"));

    echo = getEcho(tx.executeKeyspaceIds(
        ctx, ECHO_PREFIX + QUERY, KEYSPACE, KEYSPACE_IDS, BIND_VARS, TABLET_TYPE));
    Assert.assertEquals(CALLER_ID_ECHO, echo.get("callerId"));
    Assert.assertEquals(ECHO_PREFIX + QUERY, echo.get("query"));
    Assert.assertEquals(KEYSPACE, echo.get("keyspace"));
    Assert.assertEquals(KEYSPACE_IDS_ECHO, echo.get("keyspaceIds"));
    Assert.assertEquals(BIND_VARS_ECHO, echo.get("bindVars"));
    Assert.assertEquals(TABLET_TYPE_ECHO, echo.get("tabletType"));
    Assert.assertEquals(SESSION_ECHO, echo.get("session"));

    echo = getEcho(tx.executeKeyRanges(
        ctx, ECHO_PREFIX + QUERY, KEYSPACE, KEY_RANGES, BIND_VARS, TABLET_TYPE));
    Assert.assertEquals(CALLER_ID_ECHO, echo.get("callerId"));
    Assert.assertEquals(ECHO_PREFIX + QUERY, echo.get("query"));
    Assert.assertEquals(KEYSPACE, echo.get("keyspace"));
    Assert.assertEquals(KEY_RANGES_ECHO, echo.get("keyRanges"));
    Assert.assertEquals(BIND_VARS_ECHO, echo.get("bindVars"));
    Assert.assertEquals(TABLET_TYPE_ECHO, echo.get("tabletType"));
    Assert.assertEquals(SESSION_ECHO, echo.get("session"));

    echo = getEcho(tx.executeEntityIds(ctx, ECHO_PREFIX + QUERY, KEYSPACE, "column1",
        ENTITY_KEYSPACE_IDS, BIND_VARS, TABLET_TYPE));
    Assert.assertEquals(CALLER_ID_ECHO, echo.get("callerId"));
    Assert.assertEquals(ECHO_PREFIX + QUERY, echo.get("query"));
    Assert.assertEquals(KEYSPACE, echo.get("keyspace"));
    Assert.assertEquals("column1", echo.get("entityColumnName"));
    Assert.assertEquals(ENTITY_KEYSPACE_IDS_ECHO, echo.get("entityIds"));
    Assert.assertEquals(BIND_VARS_ECHO, echo.get("bindVars"));
    Assert.assertEquals(TABLET_TYPE_ECHO, echo.get("tabletType"));
    Assert.assertEquals(SESSION_ECHO, echo.get("session"));

    tx.rollback(ctx);
    tx = conn.begin(ctx);

    echo = getEcho(tx.executeBatchShards(ctx, Arrays.asList(Proto.bindShardQuery(KEYSPACE, SHARDS,
                                                  ECHO_PREFIX + QUERY, BIND_VARS)),
                         TABLET_TYPE, true).get(0));
    Assert.assertEquals(CALLER_ID_ECHO, echo.get("callerId"));
    Assert.assertEquals(ECHO_PREFIX + QUERY, echo.get("query"));
    Assert.assertEquals(KEYSPACE, echo.get("keyspace"));
    Assert.assertEquals(SHARDS_ECHO, echo.get("shards"));
    Assert.assertEquals(BIND_VARS_ECHO, echo.get("bindVars"));
    Assert.assertEquals(TABLET_TYPE_ECHO, echo.get("tabletType"));
    Assert.assertEquals(SESSION_ECHO, echo.get("session"));

    echo =
        getEcho(tx.executeBatchKeyspaceIds(ctx, Arrays.asList(Proto.bindKeyspaceIdQuery(KEYSPACE,
                                                    KEYSPACE_IDS, ECHO_PREFIX + QUERY, BIND_VARS)),
                      TABLET_TYPE, true).get(0));
    Assert.assertEquals(CALLER_ID_ECHO, echo.get("callerId"));
    Assert.assertEquals(ECHO_PREFIX + QUERY, echo.get("query"));
    Assert.assertEquals(KEYSPACE, echo.get("keyspace"));
    Assert.assertEquals(KEYSPACE_IDS_ECHO, echo.get("keyspaceIds"));
    Assert.assertEquals(BIND_VARS_ECHO, echo.get("bindVars"));
    Assert.assertEquals(TABLET_TYPE_ECHO, echo.get("tabletType"));
    Assert.assertEquals(SESSION_ECHO, echo.get("session"));

    tx.commit(ctx);
  }

  @Test
  public void testEchoSplitQuery() throws Exception {
    SplitQueryResponse.Part expected =
        SplitQueryResponse.Part.newBuilder()
            .setQuery(Proto.bindQuery(ECHO_PREFIX + QUERY + ":split_column:123", BIND_VARS))
            .setKeyRangePart(
                SplitQueryResponse.KeyRangePart.newBuilder().setKeyspace(KEYSPACE).build())
            .build();
    SplitQueryResponse.Part actual =
        conn.splitQuery(ctx, KEYSPACE, ECHO_PREFIX + QUERY, BIND_VARS, "split_column", 123).get(0);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testGetSrvKeyspace() throws Exception {
    SrvKeyspace expected =
        SrvKeyspace.newBuilder()
            .addPartitions(
                KeyspacePartition.newBuilder()
                    .setServedType(TabletType.REPLICA)
                    .addShardReferences(
                        ShardReference.newBuilder()
                            .setName("shard0")
                            .setKeyRange(
                                KeyRange.newBuilder()
                                    .setStart(
                                        ByteString.copyFrom(new byte[] {0x40, 0, 0, 0, 0, 0, 0, 0}))
                                    .setEnd(ByteString.copyFrom(
                                        new byte[] {(byte) 0x80, 0, 0, 0, 0, 0, 0, 0}))
                                    .build())
                            .build())
                    .build())
            .setShardingColumnName("sharding_column_name")
            .setShardingColumnType(KeyspaceIdType.UINT64)
            .addServedFrom(
                SrvKeyspace.ServedFrom.newBuilder()
                    .setTabletType(TabletType.MASTER)
                    .setKeyspace("other_keyspace")
                    .build())
            .setSplitShardCount(128)
            .build();
    SrvKeyspace actual = conn.getSrvKeyspace(ctx, "big");
    Assert.assertEquals(expected, actual);
  }
}
