package com.youtube.vitess.client.grpc;

import com.youtube.vitess.client.Context;
import com.youtube.vitess.client.RpcClient;
import com.youtube.vitess.client.StreamIterator;
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
import io.grpc.StatusRuntimeException;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.SQLInvalidAuthorizationSpecException;
import java.sql.SQLNonTransientException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLTimeoutException;
import java.sql.SQLTransientException;

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
  public ExecuteResponse execute(Context ctx, ExecuteRequest request) throws SQLException {
    try (GrpcContext gctx = new GrpcContext(ctx)) {
      return blockingStub.execute(request);
    } catch (Exception e) {
      throw convertGrpcError(e);
    }
  }

  @Override
  public ExecuteShardsResponse executeShards(Context ctx, ExecuteShardsRequest request)
      throws SQLException {
    try (GrpcContext gctx = new GrpcContext(ctx)) {
      return blockingStub.executeShards(request);
    } catch (Exception e) {
      throw convertGrpcError(e);
    }
  }

  @Override
  public ExecuteKeyspaceIdsResponse executeKeyspaceIds(
      Context ctx, ExecuteKeyspaceIdsRequest request) throws SQLException {
    try (GrpcContext gctx = new GrpcContext(ctx)) {
      return blockingStub.executeKeyspaceIds(request);
    } catch (Exception e) {
      throw convertGrpcError(e);
    }
  }

  @Override
  public ExecuteKeyRangesResponse executeKeyRanges(Context ctx, ExecuteKeyRangesRequest request)
      throws SQLException {
    try (GrpcContext gctx = new GrpcContext(ctx)) {
      return blockingStub.executeKeyRanges(request);
    } catch (Exception e) {
      throw convertGrpcError(e);
    }
  }

  @Override
  public ExecuteEntityIdsResponse executeEntityIds(Context ctx, ExecuteEntityIdsRequest request)
      throws SQLException {
    try (GrpcContext gctx = new GrpcContext(ctx)) {
      return blockingStub.executeEntityIds(request);
    } catch (Exception e) {
      throw convertGrpcError(e);
    }
  }

  @Override
  public ExecuteBatchShardsResponse executeBatchShards(
      Context ctx, ExecuteBatchShardsRequest request) throws SQLException {
    try (GrpcContext gctx = new GrpcContext(ctx)) {
      return blockingStub.executeBatchShards(request);
    } catch (Exception e) {
      throw convertGrpcError(e);
    }
  }

  @Override
  public ExecuteBatchKeyspaceIdsResponse executeBatchKeyspaceIds(
      Context ctx, ExecuteBatchKeyspaceIdsRequest request) throws SQLException {
    try (GrpcContext gctx = new GrpcContext(ctx)) {
      return blockingStub.executeBatchKeyspaceIds(request);
    } catch (Exception e) {
      throw convertGrpcError(e);
    }
  }

  @Override
  public StreamIterator<QueryResult> streamExecute(Context ctx, StreamExecuteRequest request)
      throws SQLException {
    try (GrpcContext gctx = new GrpcContext(ctx)) {
      GrpcStreamAdapter<StreamExecuteResponse, QueryResult> adapter =
          new GrpcStreamAdapter<StreamExecuteResponse, QueryResult>() {
            @Override
            QueryResult getResult(StreamExecuteResponse response) throws SQLException {
              return response.getResult();
            }
          };
      asyncStub.streamExecute(request, adapter);
      return adapter;
    } catch (Exception e) {
      throw convertGrpcError(e);
    }
  }

  @Override
  public StreamIterator<QueryResult> streamExecuteShards(
      Context ctx, StreamExecuteShardsRequest request) throws SQLException {
    try (GrpcContext gctx = new GrpcContext(ctx)) {
      GrpcStreamAdapter<StreamExecuteShardsResponse, QueryResult> adapter =
          new GrpcStreamAdapter<StreamExecuteShardsResponse, QueryResult>() {
            @Override
            QueryResult getResult(StreamExecuteShardsResponse response) throws SQLException {
              return response.getResult();
            }
          };
      asyncStub.streamExecuteShards(request, adapter);
      return adapter;
    } catch (Exception e) {
      throw convertGrpcError(e);
    }
  }

  @Override
  public StreamIterator<QueryResult> streamExecuteKeyspaceIds(
      Context ctx, StreamExecuteKeyspaceIdsRequest request) throws SQLException {
    try (GrpcContext gctx = new GrpcContext(ctx)) {
      GrpcStreamAdapter<StreamExecuteKeyspaceIdsResponse, QueryResult> adapter =
          new GrpcStreamAdapter<StreamExecuteKeyspaceIdsResponse, QueryResult>() {
            @Override
            QueryResult getResult(StreamExecuteKeyspaceIdsResponse response) throws SQLException {
              return response.getResult();
            }
          };
      asyncStub.streamExecuteKeyspaceIds(request, adapter);
      return adapter;
    } catch (Exception e) {
      throw convertGrpcError(e);
    }
  }

  @Override
  public StreamIterator<QueryResult> streamExecuteKeyRanges(
      Context ctx, StreamExecuteKeyRangesRequest request) throws SQLException {
    try (GrpcContext gctx = new GrpcContext(ctx)) {
      GrpcStreamAdapter<StreamExecuteKeyRangesResponse, QueryResult> adapter =
          new GrpcStreamAdapter<StreamExecuteKeyRangesResponse, QueryResult>() {
            @Override
            QueryResult getResult(StreamExecuteKeyRangesResponse response) throws SQLException {
              return response.getResult();
            }
          };
      asyncStub.streamExecuteKeyRanges(request, adapter);
      return adapter;
    } catch (Exception e) {
      throw convertGrpcError(e);
    }
  }

  @Override
  public BeginResponse begin(Context ctx, BeginRequest request) throws SQLException {
    try (GrpcContext gctx = new GrpcContext(ctx)) {
      return blockingStub.begin(request);
    } catch (Exception e) {
      throw convertGrpcError(e);
    }
  }

  @Override
  public CommitResponse commit(Context ctx, CommitRequest request) throws SQLException {
    try (GrpcContext gctx = new GrpcContext(ctx)) {
      return blockingStub.commit(request);
    } catch (Exception e) {
      throw convertGrpcError(e);
    }
  }

  @Override
  public RollbackResponse rollback(Context ctx, RollbackRequest request) throws SQLException {
    try (GrpcContext gctx = new GrpcContext(ctx)) {
      return blockingStub.rollback(request);
    } catch (Exception e) {
      throw convertGrpcError(e);
    }
  }

  @Override
  public SplitQueryResponse splitQuery(Context ctx, SplitQueryRequest request) throws SQLException {
    try (GrpcContext gctx = new GrpcContext(ctx)) {
      return blockingStub.splitQuery(request);
    } catch (Exception e) {
      throw convertGrpcError(e);
    }
  }

  @Override
  public GetSrvKeyspaceResponse getSrvKeyspace(Context ctx, GetSrvKeyspaceRequest request)
      throws SQLException {
    try (GrpcContext gctx = new GrpcContext(ctx)) {
      return blockingStub.getSrvKeyspace(request);
    } catch (Exception e) {
      throw convertGrpcError(e);
    }
  }

  /**
   * Converts an exception from the gRPC framework into the appropriate {@link SQLException}.
   */
  static SQLException convertGrpcError(Throwable e) {
    if (e instanceof StatusRuntimeException) {
      StatusRuntimeException sre = (StatusRuntimeException) e;
      switch (sre.getStatus().getCode()) {
        case INVALID_ARGUMENT:
          return new SQLSyntaxErrorException(sre.toString(), sre);
        case DEADLINE_EXCEEDED:
          return new SQLTimeoutException(sre.toString(), sre);
        case ALREADY_EXISTS:
          return new SQLIntegrityConstraintViolationException(sre.toString(), sre);
        case UNAUTHENTICATED:
          return new SQLInvalidAuthorizationSpecException(sre.toString(), sre);
        case UNAVAILABLE:
          return new SQLTransientException(sre.toString(), sre);
        default:
          return new SQLNonTransientException("gRPC StatusRuntimeException: " + e.toString(), e);
      }
    }
    return new SQLNonTransientException("gRPC error: " + e.toString(), e);
  }
}
