package com.youtube.vitess.client;

import com.youtube.vitess.proto.Vtgate.BeginRequest;
import com.youtube.vitess.proto.Vtgate.BeginResponse;
import com.youtube.vitess.proto.Vtgate.ExecuteBatchKeyspaceIdsRequest;
import com.youtube.vitess.proto.Vtgate.ExecuteBatchKeyspaceIdsResponse;
import com.youtube.vitess.proto.Vtgate.ExecuteBatchShardsRequest;
import com.youtube.vitess.proto.Vtgate.ExecuteBatchShardsResponse;
import com.youtube.vitess.proto.Vtgate.ExecuteKeyRangesRequest;
import com.youtube.vitess.proto.Vtgate.ExecuteKeyRangesResponse;
import com.youtube.vitess.proto.Vtgate.ExecuteKeyspaceIdsRequest;
import com.youtube.vitess.proto.Vtgate.ExecuteKeyspaceIdsResponse;
import com.youtube.vitess.proto.Vtgate.ExecuteShardsRequest;
import com.youtube.vitess.proto.Vtgate.ExecuteShardsResponse;
import com.youtube.vitess.proto.Vtgate.GetSrvKeyspaceRequest;
import com.youtube.vitess.proto.Vtgate.GetSrvKeyspaceResponse;
import com.youtube.vitess.proto.Vtgate.StreamExecuteKeyRangesRequest;
import com.youtube.vitess.proto.Vtgate.StreamExecuteKeyRangesResponse;
import com.youtube.vitess.proto.Vtgate.StreamExecuteKeyspaceIdsRequest;
import com.youtube.vitess.proto.Vtgate.StreamExecuteKeyspaceIdsResponse;
import com.youtube.vitess.proto.Vtgate.StreamExecuteShardsRequest;
import com.youtube.vitess.proto.Vtgate.StreamExecuteShardsResponse;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

/**
 * VTGateConn manages a VTGate connection.
 * TODO(shengzhe): define VitessException for app-level error
 * <p>Usage:
 *
 * <code>
 *   RpcClient client = RpcClientFactory.create(
 *     InetAddresses.forUriString("${VTGATE_ADDR}", Duration.millis(500)));
 *   VTGateConn conn = VTGateConn.WithRpcClient(client);
 *   Context ctx = Context.withDeadline(DateTime.now().plusMillis(500));
 *
 *   try {
 *
 *       CallerID callerId = CallerID.newBuilder().setPrincipal("username").build();
 *       BindVariable bindVars = BindVariable.newBuilder()
 *           .setType(Type.TYPE_INT)
 *           .setValueInt(12345)
 *           .build();
 *       BoundQuery.Builder queryBuilder = BoundQuery.newBuilder()
 *           .setSql(ByteString.copyFrom("INSERT INTO test_table VALUES(1, 2, 3)", Charsets.UTF_8));
 *       queryBuilder.getMutableBindVariables().put("keyspaceid_01", bindVars);
 *       BoundQuery query = queryBuilder.build();
 *       ExecuteKeyspaceIdsRequest.newBuilder()
 *           .setCallerId(callerId)
 *           .setQuery(query)
 *           .setKeyspace("my_keyspace")
 *           .setKeyspaceIds(0, ByteString.copyFrom("keyspaceid_01", Charsets.UTF_8))
 *           .setTabletType(TabletType.MASTER)
 *           .build();
 *
 *       ExecuteKeyspaceIdsResponse response = conn.ExecuteKeyspaceIds(ctx, request);
 *       if (response.hasError()) {
 *         // handle error.
 *       }
 *       QueryResult result = response.getResult();
 *       for (Row row : result.getRowsList()) {
 *         // process each row.
 *       }
 *  } catch (VitessRpcException e) {
 *     // ...
 *   }
 * </code>
 * */
public class VTGateConn implements Closeable {

  private RpcClient client;

  private VTGateConn(RpcClient client) { this.client = client; }

  public static VTGateConn WithRpcClient(RpcClient client) {
    return new VTGateConn(client);
  }

  public ExecuteShardsResponse ExecuteShard(Context ctx, ExecuteShardsRequest request)
      throws VitessRpcException {
    return this.client.executeShard(ctx, request);
  }

  public ExecuteKeyspaceIdsResponse ExecuteKeyspaceIds(Context ctx, ExecuteKeyspaceIdsRequest request)
      throws VitessRpcException {
    return this.client.executeKeyspaceIds(ctx, request);
  }

  public ExecuteKeyRangesResponse executeKeyRanges(Context ctx, ExecuteKeyRangesRequest request)
      throws VitessRpcException {
    return this.client.executeKeyRanges(ctx, request);
  }

  public ExecuteBatchKeyspaceIdsResponse executeBatchKeyspaceIds(
      Context ctx, ExecuteBatchKeyspaceIdsRequest request) throws VitessRpcException {
    return this.client.executeBatchKeyspaceIds(ctx, request);
  }

  public ExecuteBatchShardsResponse executeBatchShards(Context ctx, ExecuteBatchShardsRequest request)
      throws VitessRpcException {
    return this.client.executeBatchShards(ctx, request);
  }

  public Iterator<StreamExecuteShardsResponse> streamExecuteShard(
      Context ctx, StreamExecuteShardsRequest request) throws VitessRpcException {
    return this.client.streamExecuteShard(ctx, request);
  }

  public Iterator<StreamExecuteKeyspaceIdsResponse> streamExecuteKeyspaceIds(
      Context ctx, StreamExecuteKeyspaceIdsRequest request) throws VitessRpcException {
    return this.client.streamExecuteKeyspaceIds(ctx, request);
  }

  public Iterator<StreamExecuteKeyRangesResponse> streamExecuteKeyRanges(
      Context ctx, StreamExecuteKeyRangesRequest request) throws VitessRpcException {
    return this.client.streamExecuteKeyRanges(ctx, request);
  }

  public VTGateTx begin(Context ctx, BeginRequest request) throws VitessRpcException {
    BeginResponse response = this.client.begin(ctx, request);
    if (response.hasError()) {
      throw new VitessRpcException(response.getError().getMessage());
    }
    return VTGateTx.withRpcClientAndSession(this.client, response.getSession());
  }

  public GetSrvKeyspaceResponse getSrvKeyspace(Context ctx, GetSrvKeyspaceRequest request)
      throws VitessRpcException {
    return this.client.getSrvKeyspace(ctx, request);
  }

  @Override
  public void close() throws IOException {
    this.client.close();
  }
}
