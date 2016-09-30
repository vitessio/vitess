package com.youtube.vitess.client.grpc;

import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.youtube.vitess.client.Context;
import com.youtube.vitess.client.Proto;
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
import java.sql.SQLRecoverableException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLTimeoutException;
import java.sql.SQLTransientException;
import java.util.concurrent.TimeUnit;
import org.joda.time.Duration;

/**
 * GrpcClient is a gRPC-based implementation of Vitess Rpcclient.
 */
public class GrpcClient implements RpcClient {
  private final ManagedChannel channel;
  private final VitessStub asyncStub;
  private final VitessFutureStub futureStub;

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
    return Futures.catchingAsync(getFutureStub(ctx).execute(request), Exception.class,
        new ExceptionConverter<ExecuteResponse>());
  }

  @Override
  public ListenableFuture<ExecuteShardsResponse> executeShards(Context ctx,
      ExecuteShardsRequest request) throws SQLException {
    return Futures.catchingAsync(getFutureStub(ctx).executeShards(request), Exception.class,
        new ExceptionConverter<ExecuteShardsResponse>());
  }

  @Override
  public ListenableFuture<ExecuteKeyspaceIdsResponse> executeKeyspaceIds(Context ctx,
      ExecuteKeyspaceIdsRequest request) throws SQLException {
    return Futures.catchingAsync(getFutureStub(ctx).executeKeyspaceIds(request), Exception.class,
        new ExceptionConverter<ExecuteKeyspaceIdsResponse>());
  }

  @Override
  public ListenableFuture<ExecuteKeyRangesResponse> executeKeyRanges(Context ctx,
      ExecuteKeyRangesRequest request) throws SQLException {
    return Futures.catchingAsync(getFutureStub(ctx).executeKeyRanges(request), Exception.class,
        new ExceptionConverter<ExecuteKeyRangesResponse>());
  }

  @Override
  public ListenableFuture<ExecuteEntityIdsResponse> executeEntityIds(Context ctx,
      ExecuteEntityIdsRequest request) throws SQLException {
    return Futures.catchingAsync(getFutureStub(ctx).executeEntityIds(request), Exception.class,
        new ExceptionConverter<ExecuteEntityIdsResponse>());
  }

  @Override
  public ListenableFuture<ExecuteBatchShardsResponse> executeBatchShards(Context ctx,
      ExecuteBatchShardsRequest request) throws SQLException {
    return Futures.catchingAsync(getFutureStub(ctx).executeBatchShards(request), Exception.class,
        new ExceptionConverter<ExecuteBatchShardsResponse>());
  }

  @Override
  public ListenableFuture<ExecuteBatchKeyspaceIdsResponse> executeBatchKeyspaceIds(Context ctx,
      ExecuteBatchKeyspaceIdsRequest request) throws SQLException {
    return Futures.catchingAsync(getFutureStub(ctx).executeBatchKeyspaceIds(request),
        Exception.class, new ExceptionConverter<ExecuteBatchKeyspaceIdsResponse>());
  }

  @Override
  public StreamIterator<QueryResult> streamExecute(Context ctx, StreamExecuteRequest request)
      throws SQLException {
    GrpcStreamAdapter<StreamExecuteResponse, QueryResult> adapter =
        new GrpcStreamAdapter<StreamExecuteResponse, QueryResult>() {
          @Override
          QueryResult getResult(StreamExecuteResponse response) throws SQLException {
            return response.getResult();
          }
        };
    getAsyncStub(ctx).streamExecute(request, adapter);
    return adapter;
  }

  @Override
  public StreamIterator<QueryResult> streamExecuteShards(Context ctx,
      StreamExecuteShardsRequest request) throws SQLException {
    GrpcStreamAdapter<StreamExecuteShardsResponse, QueryResult> adapter =
        new GrpcStreamAdapter<StreamExecuteShardsResponse, QueryResult>() {
          @Override
          QueryResult getResult(StreamExecuteShardsResponse response) throws SQLException {
            return response.getResult();
          }
        };
    getAsyncStub(ctx).streamExecuteShards(request, adapter);
    return adapter;
  }

  @Override
  public StreamIterator<QueryResult> streamExecuteKeyspaceIds(Context ctx,
      StreamExecuteKeyspaceIdsRequest request) throws SQLException {
    GrpcStreamAdapter<StreamExecuteKeyspaceIdsResponse, QueryResult> adapter =
        new GrpcStreamAdapter<StreamExecuteKeyspaceIdsResponse, QueryResult>() {
          @Override
          QueryResult getResult(StreamExecuteKeyspaceIdsResponse response) throws SQLException {
            return response.getResult();
          }
        };
    getAsyncStub(ctx).streamExecuteKeyspaceIds(request, adapter);
    return adapter;
  }

  @Override
  public StreamIterator<QueryResult> streamExecuteKeyRanges(Context ctx,
      StreamExecuteKeyRangesRequest request) throws SQLException {
    GrpcStreamAdapter<StreamExecuteKeyRangesResponse, QueryResult> adapter =
        new GrpcStreamAdapter<StreamExecuteKeyRangesResponse, QueryResult>() {
          @Override
          QueryResult getResult(StreamExecuteKeyRangesResponse response) throws SQLException {
            return response.getResult();
          }
        };
    getAsyncStub(ctx).streamExecuteKeyRanges(request, adapter);
    return adapter;
  }

  @Override
  public ListenableFuture<BeginResponse> begin(Context ctx, BeginRequest request)
      throws SQLException {
    return Futures.catchingAsync(getFutureStub(ctx).begin(request), Exception.class,
        new ExceptionConverter<BeginResponse>());
  }

  @Override
  public ListenableFuture<CommitResponse> commit(Context ctx, CommitRequest request)
      throws SQLException {
    return Futures.catchingAsync(getFutureStub(ctx).commit(request), Exception.class,
        new ExceptionConverter<CommitResponse>());
  }

  @Override
  public ListenableFuture<RollbackResponse> rollback(Context ctx, RollbackRequest request)
      throws SQLException {
    return Futures.catchingAsync(getFutureStub(ctx).rollback(request), Exception.class,
        new ExceptionConverter<RollbackResponse>());
  }

  @Override
  public ListenableFuture<SplitQueryResponse> splitQuery(Context ctx, SplitQueryRequest request)
      throws SQLException {
    return Futures.catchingAsync(getFutureStub(ctx).splitQuery(request), Exception.class,
        new ExceptionConverter<SplitQueryResponse>());
  }

  @Override
  public ListenableFuture<GetSrvKeyspaceResponse> getSrvKeyspace(Context ctx,
      GetSrvKeyspaceRequest request) throws SQLException {
    return Futures.catchingAsync(getFutureStub(ctx).getSrvKeyspace(request), Exception.class,
        new ExceptionConverter<GetSrvKeyspaceResponse>());
  }

  /**
   * Converts an exception from the gRPC framework into the appropriate {@link SQLException}.
   */
  static SQLException convertGrpcError(Throwable e) {
    if (e instanceof StatusRuntimeException) {
      StatusRuntimeException sre = (StatusRuntimeException) e;

      int errno = Proto.getErrno(sre.getMessage());
      String sqlState = Proto.getSQLState(sre.getMessage());

      switch (sre.getStatus().getCode()) {
        case INVALID_ARGUMENT:
          return new SQLSyntaxErrorException(sre.toString(), sqlState, errno, sre);
        case DEADLINE_EXCEEDED:
          return new SQLTimeoutException(sre.toString(), sqlState, errno, sre);
        case ALREADY_EXISTS:
          return new SQLIntegrityConstraintViolationException(sre.toString(), sqlState, errno, sre);
        case UNAUTHENTICATED:
          return new SQLInvalidAuthorizationSpecException(sre.toString(), sqlState, errno, sre);
        case UNAVAILABLE:
          return new SQLTransientException(sre.toString(), sqlState, errno, sre);
        case ABORTED:
          return new SQLRecoverableException(sre.toString(), sqlState, errno, sre);
        default: // Covers e.g. UNKNOWN.
          String advice = "";
          if (e.getCause() instanceof java.nio.channels.ClosedChannelException) {
            advice =
                "Failed to connect to vtgate. Make sure that vtgate is running and you are using the correct address. Details: ";
          }
          return new SQLNonTransientException(
              "gRPC StatusRuntimeException: " + advice + e.toString(), sqlState, errno, e);
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

  private VitessStub getAsyncStub(Context ctx) {
    Duration timeout = ctx.getTimeout();
    if (timeout == null) {
      return asyncStub;
    }
    return asyncStub.withDeadlineAfter(timeout.getMillis(), TimeUnit.MILLISECONDS);
  }

  private VitessFutureStub getFutureStub(Context ctx) {
    Duration timeout = ctx.getTimeout();
    if (timeout == null) {
      return futureStub;
    }
    return futureStub.withDeadlineAfter(timeout.getMillis(), TimeUnit.MILLISECONDS);
  }
}
