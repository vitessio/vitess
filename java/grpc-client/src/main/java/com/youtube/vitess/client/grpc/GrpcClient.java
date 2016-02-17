package com.youtube.vitess.client.grpc;

import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

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
import com.youtube.vitess.proto.grpc.VitessGrpc.VitessFutureStub;
import com.youtube.vitess.proto.grpc.VitessGrpc.VitessStub;

import io.grpc.ManagedChannel;
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
  private ManagedChannel channel;
  private VitessStub asyncStub;
  private VitessFutureStub futureStub;

  public GrpcClient(ManagedChannel channel) {
    this.channel = channel;
    asyncStub = VitessGrpc.newStub(channel);
    futureStub = VitessGrpc.newFutureStub(channel);
  }

  @Override
  public void close() throws IOException {
    channel.shutdown();
  }

  @Override
  public ListenableFuture<ExecuteResponse> execute(Context ctx, ExecuteRequest request)
      throws SQLException {
    try (GrpcContext gctx = new GrpcContext(ctx)) {
      return Futures.catchingAsync(
          futureStub.execute(request), Exception.class, new ExceptionConverter<ExecuteResponse>());
    } catch (Exception e) {
      throw convertGrpcError(e);
    }
  }

  @Override
  public ListenableFuture<ExecuteShardsResponse> executeShards(
      Context ctx, ExecuteShardsRequest request) throws SQLException {
    try (GrpcContext gctx = new GrpcContext(ctx)) {
      return Futures.catchingAsync(
          futureStub.executeShards(request),
          Exception.class,
          new ExceptionConverter<ExecuteShardsResponse>());
    } catch (Exception e) {
      throw convertGrpcError(e);
    }
  }

  @Override
  public ListenableFuture<ExecuteKeyspaceIdsResponse> executeKeyspaceIds(
      Context ctx, ExecuteKeyspaceIdsRequest request) throws SQLException {
    try (GrpcContext gctx = new GrpcContext(ctx)) {
      return Futures.catchingAsync(
          futureStub.executeKeyspaceIds(request),
          Exception.class,
          new ExceptionConverter<ExecuteKeyspaceIdsResponse>());
    } catch (Exception e) {
      throw convertGrpcError(e);
    }
  }

  @Override
  public ListenableFuture<ExecuteKeyRangesResponse> executeKeyRanges(
      Context ctx, ExecuteKeyRangesRequest request) throws SQLException {
    try (GrpcContext gctx = new GrpcContext(ctx)) {
      return Futures.catchingAsync(
          futureStub.executeKeyRanges(request),
          Exception.class,
          new ExceptionConverter<ExecuteKeyRangesResponse>());
    } catch (Exception e) {
      throw convertGrpcError(e);
    }
  }

  @Override
  public ListenableFuture<ExecuteEntityIdsResponse> executeEntityIds(
      Context ctx, ExecuteEntityIdsRequest request) throws SQLException {
    try (GrpcContext gctx = new GrpcContext(ctx)) {
      return Futures.catchingAsync(
          futureStub.executeEntityIds(request),
          Exception.class,
          new ExceptionConverter<ExecuteEntityIdsResponse>());
    } catch (Exception e) {
      throw convertGrpcError(e);
    }
  }

  @Override
  public ListenableFuture<ExecuteBatchShardsResponse> executeBatchShards(
      Context ctx, ExecuteBatchShardsRequest request) throws SQLException {
    try (GrpcContext gctx = new GrpcContext(ctx)) {
      return Futures.catchingAsync(
          futureStub.executeBatchShards(request),
          Exception.class,
          new ExceptionConverter<ExecuteBatchShardsResponse>());
    } catch (Exception e) {
      throw convertGrpcError(e);
    }
  }

  @Override
  public ListenableFuture<ExecuteBatchKeyspaceIdsResponse> executeBatchKeyspaceIds(
      Context ctx, ExecuteBatchKeyspaceIdsRequest request) throws SQLException {
    try (GrpcContext gctx = new GrpcContext(ctx)) {
      return Futures.catchingAsync(
          futureStub.executeBatchKeyspaceIds(request),
          Exception.class,
          new ExceptionConverter<ExecuteBatchKeyspaceIdsResponse>());
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
  public ListenableFuture<BeginResponse> begin(Context ctx, BeginRequest request)
      throws SQLException {
    try (GrpcContext gctx = new GrpcContext(ctx)) {
      return Futures.catchingAsync(
          futureStub.begin(request), Exception.class, new ExceptionConverter<BeginResponse>());
    } catch (Exception e) {
      throw convertGrpcError(e);
    }
  }

  @Override
  public ListenableFuture<CommitResponse> commit(Context ctx, CommitRequest request)
      throws SQLException {
    try (GrpcContext gctx = new GrpcContext(ctx)) {
      return Futures.catchingAsync(
          futureStub.commit(request), Exception.class, new ExceptionConverter<CommitResponse>());
    } catch (Exception e) {
      throw convertGrpcError(e);
    }
  }

  @Override
  public ListenableFuture<RollbackResponse> rollback(Context ctx, RollbackRequest request)
      throws SQLException {
    try (GrpcContext gctx = new GrpcContext(ctx)) {
      return Futures.catchingAsync(
          futureStub.rollback(request),
          Exception.class,
          new ExceptionConverter<RollbackResponse>());
    } catch (Exception e) {
      throw convertGrpcError(e);
    }
  }

  @Override
  public ListenableFuture<SplitQueryResponse> splitQuery(Context ctx, SplitQueryRequest request)
      throws SQLException {
    try (GrpcContext gctx = new GrpcContext(ctx)) {
      return Futures.catchingAsync(
          futureStub.splitQuery(request),
          Exception.class,
          new ExceptionConverter<SplitQueryResponse>());
    } catch (Exception e) {
      throw convertGrpcError(e);
    }
  }

  @Override
  public ListenableFuture<GetSrvKeyspaceResponse> getSrvKeyspace(
      Context ctx, GetSrvKeyspaceRequest request) throws SQLException {
    try (GrpcContext gctx = new GrpcContext(ctx)) {
      return Futures.catchingAsync(
          futureStub.getSrvKeyspace(request),
          Exception.class,
          new ExceptionConverter<GetSrvKeyspaceResponse>());
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
        default: // Covers e.g. UNKNOWN.
          String advice = "";
          if (e.getCause() instanceof java.nio.channels.ClosedChannelException) {
            advice =
                "Failed to connect to vtgate. Make sure that vtgate is running and you are using the correct address. Details: ";
          }
          return new SQLNonTransientException(
              "gRPC StatusRuntimeException: " + advice + e.toString(), e);
      }
    }
    return new SQLNonTransientException("gRPC error: " + e.toString(), e);
  }

  static class ExceptionConverter<V> implements AsyncFunction<Exception, V> {
    @Override
    public ListenableFuture<V> apply(Exception e) throws Exception {
      throw convertGrpcError(e);
    }
  }
}
