package io.vitess.client.grpc;

import static com.google.common.base.Preconditions.checkState;
import static io.grpc.internal.GrpcUtil.TIMER_SERVICE;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.Context;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.internal.SharedResourceHolder;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

/**
 * RetryingInterceptor is used for retrying certain classes of failed requests in the underlying
 * gRPC connection. At this time it handles {@link MethodDescriptor.MethodType.UNARY} requests with
 * status {@link Status.Code.UNAVAILABLE}, which is according to the spec meant to be a transient
 * error. This class can be configured with {@link RetryingInterceptorConfig} to determine what
 * level of exponential backoff to apply to the handled types of failing requests.
 *
 * When enabled, this interceptor will retry valid requests with an exponentially increasing backoff
 * time up to the maximum time defined by the {@link io.grpc.Deadline} in the call's {@link
 * CallOptions}.
 */
public class RetryingInterceptor implements ClientInterceptor {

  private final RetryingInterceptorConfig config;

  RetryingInterceptor(RetryingInterceptorConfig config) {
    this.config = config;
  }

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
      MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {

    // Only UNARY calls can be retried, streaming call errors should be handled by user.
    if (method.getType() != MethodDescriptor.MethodType.UNARY || config.isDisabled()) {
      return next.newCall(method, callOptions);
    }

    return new RetryingCall<ReqT, RespT>(method, callOptions, next, Context.current(), config);
  }

  private class RetryingCall<ReqT, RespT> extends ClientCall<ReqT, RespT> {

    private final MethodDescriptor<ReqT, RespT> method;
    private final CallOptions callOptions;
    private final Channel channel;
    private final Context context;
    private final ScheduledExecutorService scheduledExecutor;
    private Listener<RespT> responseListener;
    private Metadata requestHeaders;
    private ReqT requestMessage;
    private boolean compressionEnabled;
    private final Queue<AttemptListener> attemptListeners =
        new ConcurrentLinkedQueue<AttemptListener>();
    private volatile AttemptListener latestResponse;
    private volatile ScheduledFuture<?> retryTask;

    private volatile long nextBackoffMillis = 5;
    private final long maxBackoffMillis;
    private final double backoffMultiplier;

    RetryingCall(MethodDescriptor<ReqT, RespT> method,
        CallOptions callOptions, Channel channel, Context context,
        RetryingInterceptorConfig config) {
      this.method = method;
      this.callOptions = callOptions;
      this.channel = channel;
      this.context = context;
      this.nextBackoffMillis = config.getInitialBackoffMillis();
      this.maxBackoffMillis = config.getMaxBackoffMillis();
      this.backoffMultiplier = config.getBackoffMultiplier();
      this.scheduledExecutor = SharedResourceHolder.get(TIMER_SERVICE);
    }

    @Override
    public void start(Listener<RespT> listener, Metadata headers) {
      checkState(attemptListeners.isEmpty());
      checkState(responseListener == null);
      checkState(requestHeaders == null);
      responseListener = listener;
      requestHeaders = headers;
      ClientCall<ReqT, RespT> firstCall = channel.newCall(method, callOptions);
      AttemptListener attemptListener = new AttemptListener(firstCall);
      attemptListeners.add(attemptListener);
      firstCall.start(attemptListener, headers);
    }

    @Override
    public void request(int numMessages) {
      lastCall().request(numMessages);
    }

    @Override
    public void cancel(@Nullable String message, @Nullable Throwable cause) {
      for (AttemptListener attempt : attemptListeners) {
        attempt.call.cancel(message, cause);
      }
      if (retryTask != null) {
        retryTask.cancel(true);
      }
    }

    @Override
    public void halfClose() {
      lastCall().halfClose();
    }

    @Override
    public void sendMessage(ReqT message) {
      checkState(requestMessage == null);
      requestMessage = message;
      lastCall().sendMessage(message);
    }

    @Override
    public boolean isReady() {
      return lastCall().isReady();
    }

    @Override
    public void setMessageCompression(boolean enabled) {
      compressionEnabled = enabled;
      lastCall().setMessageCompression(enabled);
    }

    private long computeSleepTime() {
      long currentBackoff = nextBackoffMillis;
      nextBackoffMillis = Math.min((long) (currentBackoff * backoffMultiplier), maxBackoffMillis);
      return currentBackoff;
    }

    private void maybeRetry(AttemptListener attempt) {
      Status status = attempt.responseStatus;

      if (status.isOk() || status.getCode() != Status.Code.UNAVAILABLE) {
        useResponse(attempt);
        return;
      }

      long nextBackoffMillis = computeSleepTime();
      long deadlineMillis = Long.MIN_VALUE;
      if (callOptions.getDeadline() != null) {
        deadlineMillis = callOptions.getDeadline().timeRemaining(TimeUnit.MILLISECONDS);
      }

      if (deadlineMillis > Long.MIN_VALUE && deadlineMillis < nextBackoffMillis) {
        AttemptListener latest = latestResponse;
        if (latest != null) {
          useResponse(latest);
        } else {
          useResponse(attempt);
        }
        return;
      }

      latestResponse = attempt;
      retryTask = scheduledExecutor.schedule(context.wrap(new Runnable() {
        @Override
        public void run() {
          ClientCall<ReqT, RespT> nextCall = channel.newCall(method, callOptions);
          AttemptListener nextAttemptListener = new AttemptListener(nextCall);
          attemptListeners.add(nextAttemptListener);
          nextCall.start(nextAttemptListener, requestHeaders);
          nextCall.setMessageCompression(compressionEnabled);
          nextCall.sendMessage(requestMessage);
          nextCall.request(1);
          nextCall.halfClose();
        }
      }), nextBackoffMillis, TimeUnit.MILLISECONDS);
    }

    private void useResponse(AttemptListener attempt) {
      responseListener.onHeaders(attempt.responseHeaders);
      if (attempt.responseMessage != null) {
        responseListener.onMessage(attempt.responseMessage);
      }
      responseListener.onClose(attempt.responseStatus, attempt.responseTrailers);
    }

    private ClientCall<ReqT, RespT> lastCall() {
      checkState(!attemptListeners.isEmpty());
      return attemptListeners.peek().call;
    }

    private class AttemptListener extends ClientCall.Listener<RespT> {

      final ClientCall<ReqT, RespT> call;
      Metadata responseHeaders;
      RespT responseMessage;
      Status responseStatus;
      Metadata responseTrailers;

      AttemptListener(ClientCall<ReqT, RespT> call) {
        this.call = call;
      }

      @Override
      public void onHeaders(Metadata headers) {
        responseHeaders = headers;
      }

      @Override
      public void onMessage(RespT message) {
        responseMessage = message;
      }

      @Override
      public void onClose(Status status, Metadata trailers) {
        responseStatus = status;
        responseTrailers = trailers;
        maybeRetry(this);
      }

      @Override
      public void onReady() {
        // Pass-through to original listener.
        responseListener.onReady();
      }
    }
  }

}
