package io.vitess.client.grpc;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.util.concurrent.ListenableFuture;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.vitess.proto.Vtgate;
import io.vitess.proto.grpc.VitessGrpc;

public class RetryingInterceptorTest {

  @Test
  public void testNoopConfigPassesThrough() throws ExecutionException, InterruptedException {
    ForceRetryNTimesInterceptor forceRetryNTimesInterceptor = new ForceRetryNTimesInterceptor(3);

    ManagedChannel channel = InProcessChannelBuilder.forName("foo").intercept(new RetryingInterceptor(RetryingInterceptorConfig.noOpConfig()), forceRetryNTimesInterceptor).build();
    VitessGrpc.VitessFutureStub stub = VitessGrpc.newFutureStub(channel);
    ListenableFuture<Vtgate.ExecuteResponse> resp = stub.execute(Vtgate.ExecuteRequest.getDefaultInstance());
    try {
      resp.get();
      Assert.fail("Should have failed after 1 attempt");
    } catch (Exception e) {
      Assert.assertEquals(1, forceRetryNTimesInterceptor.getNumRetryableFailures());
    }
  }

  @Test
  public void testRetryAfterBackoff() throws ExecutionException, InterruptedException {
    ForceRetryNTimesInterceptor forceRetryNTimesInterceptor = new ForceRetryNTimesInterceptor(3);
    RetryingInterceptorConfig retryingInterceptorConfig = RetryingInterceptorConfig.exponentialConfig(5, 60, 2);
    ManagedChannel channel = InProcessChannelBuilder.forName("foo").intercept(forceRetryNTimesInterceptor, new RetryingInterceptor(retryingInterceptorConfig)).build();
    VitessGrpc.VitessFutureStub stub = VitessGrpc.newFutureStub(channel);
    ListenableFuture<Vtgate.ExecuteResponse> resp = stub.execute(Vtgate.ExecuteRequest.getDefaultInstance());
    try {
      resp.get();
      Assert.fail("Should have failed after 3 attempt");
    } catch (Exception e) {
      e.printStackTrace();
      Assert.assertEquals(3, forceRetryNTimesInterceptor.getNumRetryableFailures());
    }
  }

  @Test
  public void testRetryDeadlineExceeded() throws ExecutionException, InterruptedException {
    ForceRetryNTimesInterceptor forceRetryNTimesInterceptor = new ForceRetryNTimesInterceptor(3);
    RetryingInterceptorConfig retryingInterceptorConfig = RetryingInterceptorConfig.exponentialConfig(5, 60, 2);
    ManagedChannel channel = InProcessChannelBuilder.forName("foo").intercept(forceRetryNTimesInterceptor, new RetryingInterceptor(retryingInterceptorConfig)).build();
    VitessGrpc.VitessFutureStub stub = VitessGrpc.newFutureStub(channel).withDeadlineAfter(1, TimeUnit.MILLISECONDS);
    ListenableFuture<Vtgate.ExecuteResponse> resp = stub.execute(Vtgate.ExecuteRequest.getDefaultInstance());
    try {
      resp.get();
      Assert.fail("Should have failed");
    } catch (Exception e) {
      Assert.assertEquals(1, forceRetryNTimesInterceptor.getNumRetryableFailures());
    }
  }

  public class ForceRetryNTimesInterceptor implements ClientInterceptor {

    private final int timesToForceRetry;

    private int numRetryableFailures = 0;

    ForceRetryNTimesInterceptor(int timesToForceRetry) {
      this.timesToForceRetry = timesToForceRetry;
    }

    int getNumRetryableFailures() {
      return numRetryableFailures;
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {

      System.out.println("Num failures: " + numRetryableFailures);
      if (numRetryableFailures < timesToForceRetry) {
        numRetryableFailures++;
        return new FailingCall<ReqT, RespT>(Status.UNAVAILABLE);
      }

      return new FailingCall<>(Status.ABORTED);
    }
  }

  private class FailingCall<ReqT, RespT> extends ClientCall<ReqT, RespT> {

    private final Status statusToFailWith;

    public FailingCall(Status statusToFailWith) {
      this.statusToFailWith = statusToFailWith;
    }

    @Override
    public void start(Listener<RespT> responseListener, Metadata headers) {
      responseListener.onClose(statusToFailWith, null);
    }

    @Override
    public void request(int numMessages) {
      // no op, will fail immediately on start
    }

    @Override
    public void cancel(@Nullable String message, @Nullable Throwable cause) {
      // no op, will fail immediately on start
    }

    @Override
    public void halfClose() {
      // no op, will fail immediately on start
    }

    @Override
    public void sendMessage(ReqT message) {
      // no op, will fail immediately on start
    }
  }
}
