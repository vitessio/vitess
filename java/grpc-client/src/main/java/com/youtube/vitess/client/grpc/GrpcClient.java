package com.youtube.vitess.client.grpc;

import com.youtube.vitess.client.Context;
import com.youtube.vitess.client.Proto;
import com.youtube.vitess.client.RpcClient;
import com.youtube.vitess.client.StreamIterator;
import com.youtube.vitess.client.VitessException;
import com.youtube.vitess.client.VitessRpcException;
import com.youtube.vitess.proto.Query.QueryResult;
import com.youtube.vitess.proto.Vtgate.BeginRequest;
import com.youtube.vitess.proto.Vtgate.BeginResponse;
import com.youtube.vitess.proto.Vtgate.CommitRequest;
import com.youtube.vitess.proto.Vtgate.CommitResponse;
import com.youtube.vitess.proto.Vtgate.ExecuteBatchKeyspaceIdsRequest;
import com.youtube.vitess.proto.Vtgate.ExecuteBatchKeyspaceIdsResponse;
import com.youtube.vitess.proto.Vtgate.ExecuteBatchShardsRequest;
import com.youtube.vitess.proto.Vtgate.ExecuteBatchShardsResponse;
import com.youtube.vitess.proto.Vtgate.ExecuteEntityIdsRequest;
import com.youtube.vitess.proto.Vtgate.ExecuteEntityIdsResponse;
import com.youtube.vitess.proto.Vtgate.ExecuteKeyRangesRequest;
import com.youtube.vitess.proto.Vtgate.ExecuteKeyRangesResponse;
import com.youtube.vitess.proto.Vtgate.ExecuteKeyspaceIdsRequest;
import com.youtube.vitess.proto.Vtgate.ExecuteKeyspaceIdsResponse;
import com.youtube.vitess.proto.Vtgate.ExecuteRequest;
import com.youtube.vitess.proto.Vtgate.ExecuteResponse;
import com.youtube.vitess.proto.Vtgate.ExecuteShardsRequest;
import com.youtube.vitess.proto.Vtgate.ExecuteShardsResponse;
import com.youtube.vitess.proto.Vtgate.GetSrvKeyspaceRequest;
import com.youtube.vitess.proto.Vtgate.GetSrvKeyspaceResponse;
import com.youtube.vitess.proto.Vtgate.RollbackRequest;
import com.youtube.vitess.proto.Vtgate.RollbackResponse;
import com.youtube.vitess.proto.Vtgate.SplitQueryRequest;
import com.youtube.vitess.proto.Vtgate.SplitQueryResponse;
import com.youtube.vitess.proto.Vtgate.StreamExecuteKeyRangesRequest;
import com.youtube.vitess.proto.Vtgate.StreamExecuteKeyRangesResponse;
import com.youtube.vitess.proto.Vtgate.StreamExecuteKeyspaceIdsRequest;
import com.youtube.vitess.proto.Vtgate.StreamExecuteKeyspaceIdsResponse;
import com.youtube.vitess.proto.Vtgate.StreamExecuteRequest;
import com.youtube.vitess.proto.Vtgate.StreamExecuteResponse;
import com.youtube.vitess.proto.Vtgate.StreamExecuteShardsRequest;
import com.youtube.vitess.proto.Vtgate.StreamExecuteShardsResponse;
import com.youtube.vitess.proto.grpc.VitessGrpc;
import com.youtube.vitess.proto.grpc.VitessGrpc.VitessBlockingStub;
import com.youtube.vitess.proto.grpc.VitessGrpc.VitessStub;

import io.grpc.ChannelImpl;

import java.io.IOException;

/**
 * GrpcClient is a gRPC-based implementation of Vitess Rpcclient.
 */
public class GrpcClient implements RpcClient {
  private ChannelImpl channel;
  private VitessStub asyncStub;
  private VitessBlockingStub blockingStub;

  public GrpcClient(ChannelImpl channel) {
    this.channel = channel;
    asyncStub = VitessGrpc.newStub(channel);
    blockingStub = VitessGrpc.newBlockingStub(channel);
  }

  @Override
  public void close() throws IOException {
    channel.shutdown();
  }

  @Override
  public ExecuteResponse execute(Context ctx, ExecuteRequest request)
      throws VitessException, VitessRpcException {
    try (GrpcContext gctx = new GrpcContext(ctx)) {
      return blockingStub.execute(request);
    } catch (Exception e) {
      checkGrpcError(e);
      throw new VitessRpcException("grpc error", e);
    }
  }

  @Override
  public ExecuteShardsResponse executeShards(Context ctx, ExecuteShardsRequest request)
      throws VitessException, VitessRpcException {
    try (GrpcContext gctx = new GrpcContext(ctx)) {
      return blockingStub.executeShards(request);
    } catch (Exception e) {
      checkGrpcError(e);
      throw new VitessRpcException("grpc error", e);
    }
  }

  @Override
  public ExecuteKeyspaceIdsResponse executeKeyspaceIds(
      Context ctx, ExecuteKeyspaceIdsRequest request) throws VitessException, VitessRpcException {
    try (GrpcContext gctx = new GrpcContext(ctx)) {
      return blockingStub.executeKeyspaceIds(request);
    } catch (Exception e) {
      checkGrpcError(e);
      throw new VitessRpcException("grpc error", e);
    }
  }

  @Override
  public ExecuteKeyRangesResponse executeKeyRanges(Context ctx, ExecuteKeyRangesRequest request)
      throws VitessException, VitessRpcException {
    try (GrpcContext gctx = new GrpcContext(ctx)) {
      return blockingStub.executeKeyRanges(request);
    } catch (Exception e) {
      checkGrpcError(e);
      throw new VitessRpcException("grpc error", e);
    }
  }

  @Override
  public ExecuteEntityIdsResponse executeEntityIds(Context ctx, ExecuteEntityIdsRequest request)
      throws VitessException, VitessRpcException {
    try (GrpcContext gctx = new GrpcContext(ctx)) {
      return blockingStub.executeEntityIds(request);
    } catch (Exception e) {
      checkGrpcError(e);
      throw new VitessRpcException("grpc error", e);
    }
  }

  @Override
  public ExecuteBatchShardsResponse executeBatchShards(
      Context ctx, ExecuteBatchShardsRequest request) throws VitessException, VitessRpcException {
    try (GrpcContext gctx = new GrpcContext(ctx)) {
      return blockingStub.executeBatchShards(request);
    } catch (Exception e) {
      checkGrpcError(e);
      throw new VitessRpcException("grpc error", e);
    }
  }

  @Override
  public ExecuteBatchKeyspaceIdsResponse executeBatchKeyspaceIds(
      Context ctx, ExecuteBatchKeyspaceIdsRequest request)
      throws VitessException, VitessRpcException {
    try (GrpcContext gctx = new GrpcContext(ctx)) {
      return blockingStub.executeBatchKeyspaceIds(request);
    } catch (Exception e) {
      checkGrpcError(e);
      throw new VitessRpcException("grpc error", e);
    }
  }

  @Override
  public StreamIterator<QueryResult> streamExecute(Context ctx, StreamExecuteRequest request)
      throws VitessRpcException {
    try (GrpcContext gctx = new GrpcContext(ctx)) {
      GrpcStreamAdapter<StreamExecuteResponse, QueryResult> adapter =
          new GrpcStreamAdapter<StreamExecuteResponse, QueryResult>() {
            @Override
            QueryResult getResult(StreamExecuteResponse response) throws VitessException {
              Proto.checkError(response.getError());
              return response.getResult();
            }
          };
      asyncStub.streamExecute(request, adapter);
      return adapter;
    } catch (Exception e) {
      throw new VitessRpcException("grpc error", e);
    }
  }

  @Override
  public StreamIterator<QueryResult> streamExecuteShards(
      Context ctx, StreamExecuteShardsRequest request) throws VitessRpcException {
    try (GrpcContext gctx = new GrpcContext(ctx)) {
      GrpcStreamAdapter<StreamExecuteShardsResponse, QueryResult> adapter =
          new GrpcStreamAdapter<StreamExecuteShardsResponse, QueryResult>() {
            @Override
            QueryResult getResult(StreamExecuteShardsResponse response) throws VitessException {
              Proto.checkError(response.getError());
              return response.getResult();
            }
          };
      asyncStub.streamExecuteShards(request, adapter);
      return adapter;
    } catch (Exception e) {
      throw new VitessRpcException("grpc error", e);
    }
  }

  @Override
  public StreamIterator<QueryResult> streamExecuteKeyspaceIds(
      Context ctx, StreamExecuteKeyspaceIdsRequest request) throws VitessRpcException {
    try (GrpcContext gctx = new GrpcContext(ctx)) {
      GrpcStreamAdapter<StreamExecuteKeyspaceIdsResponse, QueryResult> adapter =
          new GrpcStreamAdapter<StreamExecuteKeyspaceIdsResponse, QueryResult>() {
            @Override
            QueryResult getResult(StreamExecuteKeyspaceIdsResponse response)
                throws VitessException {
              Proto.checkError(response.getError());
              return response.getResult();
            }
          };
      asyncStub.streamExecuteKeyspaceIds(request, adapter);
      return adapter;
    } catch (Exception e) {
      throw new VitessRpcException("grpc error", e);
    }
  }

  @Override
  public StreamIterator<QueryResult> streamExecuteKeyRanges(
      Context ctx, StreamExecuteKeyRangesRequest request) throws VitessRpcException {
    try (GrpcContext gctx = new GrpcContext(ctx)) {
      GrpcStreamAdapter<StreamExecuteKeyRangesResponse, QueryResult> adapter =
          new GrpcStreamAdapter<StreamExecuteKeyRangesResponse, QueryResult>() {
            @Override
            QueryResult getResult(StreamExecuteKeyRangesResponse response) throws VitessException {
              Proto.checkError(response.getError());
              return response.getResult();
            }
          };
      asyncStub.streamExecuteKeyRanges(request, adapter);
      return adapter;
    } catch (Exception e) {
      throw new VitessRpcException("grpc error", e);
    }
  }

  @Override
  public BeginResponse begin(Context ctx, BeginRequest request)
      throws VitessException, VitessRpcException {
    try (GrpcContext gctx = new GrpcContext(ctx)) {
      return blockingStub.begin(request);
    } catch (Exception e) {
      checkGrpcError(e);
      throw new VitessRpcException("grpc error", e);
    }
  }

  @Override
  public CommitResponse commit(Context ctx, CommitRequest request)
      throws VitessException, VitessRpcException {
    try (GrpcContext gctx = new GrpcContext(ctx)) {
      return blockingStub.commit(request);
    } catch (Exception e) {
      checkGrpcError(e);
      throw new VitessRpcException("grpc error", e);
    }
  }

  @Override
  public RollbackResponse rollback(Context ctx, RollbackRequest request)
      throws VitessException, VitessRpcException {
    try (GrpcContext gctx = new GrpcContext(ctx)) {
      return blockingStub.rollback(request);
    } catch (Exception e) {
      checkGrpcError(e);
      throw new VitessRpcException("grpc error", e);
    }
  }

  @Override
  public SplitQueryResponse splitQuery(Context ctx, SplitQueryRequest request)
      throws VitessException, VitessRpcException {
    try (GrpcContext gctx = new GrpcContext(ctx)) {
      return blockingStub.splitQuery(request);
    } catch (Exception e) {
      checkGrpcError(e);
      throw new VitessRpcException("grpc error", e);
    }
  }

  @Override
  public GetSrvKeyspaceResponse getSrvKeyspace(Context ctx, GetSrvKeyspaceRequest request)
      throws VitessException, VitessRpcException {
    try (GrpcContext gctx = new GrpcContext(ctx)) {
      return blockingStub.getSrvKeyspace(request);
    } catch (Exception e) {
      checkGrpcError(e);
      throw new VitessRpcException("grpc error", e);
    }
  }

  /**
   * checkGrpcError converts an exception from the gRPC framework into
   * a VitessException if it represents an app-level Vitess error.
   * @param e
   * @throws VitessException
   */
  private void checkGrpcError(Exception e) throws VitessException {
    // TODO(enisoc): Implement checkGrpcError.
  }
}
