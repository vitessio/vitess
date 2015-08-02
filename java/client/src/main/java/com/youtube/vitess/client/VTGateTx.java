package com.youtube.vitess.client;

import com.youtube.vitess.proto.Query.QueryResult;
import com.youtube.vitess.proto.Vtgate.CommitRequest;
import com.youtube.vitess.proto.Vtgate.CommitResponse;
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
import com.youtube.vitess.proto.Vtgate.RollbackRequest;
import com.youtube.vitess.proto.Vtgate.RollbackResponse;
import com.youtube.vitess.proto.Vtgate.Session;

import java.util.List;

/**
 * VTGateTx manages a pending transaction.
 */
public class VTGateTx {

  private RpcClient client;
  private Session session;

  private VTGateTx(RpcClient client, Session session) {
    this.client = client;
    this.session = session;
  }

  public static VTGateTx withRpcClientAndSession(RpcClient client, Session session) {
    return new VTGateTx(client, session);
  }

  public QueryResult executeShard(Context ctx, ExecuteShardsRequest request)
      throws VitessRpcException, VitessNotInTransactionException {
    if (this.session == null) {
      throw new VitessNotInTransactionException("executeShard: not in transaction");
    }
    ExecuteShardsResponse response = this.client.executeShard(ctx, request);
    this.session = response.getSession();
    return response.getResult();
  }

  public QueryResult executeKeyspaceIds(Context ctx, ExecuteKeyspaceIdsRequest request)
      throws VitessRpcException, VitessNotInTransactionException {
    if (this.session == null) {
      throw new VitessNotInTransactionException("executeKeyspaceIds: not in transaction");
    }
    ExecuteKeyspaceIdsResponse response = this.client.executeKeyspaceIds(ctx, request);
    this.session = response.getSession();
    return response.getResult();
  }

  public QueryResult executeKeyRanges(Context ctx, ExecuteKeyRangesRequest request)
      throws VitessRpcException, VitessNotInTransactionException {
    if (this.session == null) {
      throw new VitessNotInTransactionException("executeKeyRanges: not in transaction");
    }
    ExecuteKeyRangesResponse response = this.client.executeKeyRanges(ctx, request);
    this.session = response.getSession();
    return response.getResult();
  }

  public List<QueryResult> executeBatchKeyspaceIds(
      Context ctx, ExecuteBatchKeyspaceIdsRequest request)
      throws VitessRpcException, VitessNotInTransactionException {
    if (this.session == null) {
      throw new VitessNotInTransactionException("executeBatchKeyspaceIds: not in transaction");
    }
    ExecuteBatchKeyspaceIdsResponse response = this.client.executeBatchKeyspaceIds(ctx, request);
    this.session = response.getSession();
    return response.getResultsList();
  }

  public List<QueryResult> executeBatchShards(Context ctx, ExecuteBatchShardsRequest request)
      throws VitessRpcException, VitessNotInTransactionException {
    if (this.session == null) {
      throw new VitessNotInTransactionException("executeBatchShards: not in transaction");
    }
    ExecuteBatchShardsResponse response = this.client.executeBatchShards(ctx, request);
    this.session = response.getSession();
    return response.getResultsList();
  }


  public CommitResponse commit(Context ctx, CommitRequest request)
      throws VitessRpcException, VitessNotInTransactionException {
    if (this.session == null) {
      throw new VitessNotInTransactionException("commit: not in transaction");
    }
    CommitResponse response = this.client.commit(
        ctx, CommitRequest.newBuilder().setSession(this.session).build());
    this.session = null;
    return response;
  }

  public RollbackResponse rollback(Context ctx, RollbackRequest request)
      throws VitessRpcException, VitessNotInTransactionException {
    if (this.session == null) {
      throw new VitessNotInTransactionException("rollback: not in transaction");
    }
    RollbackResponse response = this.client.rollback(
        ctx, RollbackRequest.newBuilder().setSession(this.session).build());
    this.session = null;
    return response;
  }
}
