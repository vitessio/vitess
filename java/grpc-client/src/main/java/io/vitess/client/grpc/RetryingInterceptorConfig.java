package io.vitess.client.grpc;

public class RetryingInterceptorConfig {

  private static final long DISABLED = -1;

  private final long initialBackoffMillis;
  private final long maxBackoffMillis;
  private final double backoffMultiplier;

  private RetryingInterceptorConfig(long initialBackoffMillis, long maxBackoffMillis, double backoffMultiplier) {
    this.initialBackoffMillis = initialBackoffMillis;
    this.maxBackoffMillis = maxBackoffMillis;
    this.backoffMultiplier = backoffMultiplier;
  }

  public static RetryingInterceptorConfig noopConfig() {
    return new RetryingInterceptorConfig(DISABLED, DISABLED, 0);
  }

  public static RetryingInterceptorConfig exponentialConfig(long initialBackoffMillis, long maxBackoffMillis, double backoffMultiplier) {
    return new RetryingInterceptorConfig(initialBackoffMillis, maxBackoffMillis, backoffMultiplier);
  }

  boolean isDisabled() {
    return initialBackoffMillis == DISABLED || maxBackoffMillis == DISABLED;
  }

  long getInitialBackoffMillis() {
    return initialBackoffMillis;
  }

  long getMaxBackoffMillis() {
    return maxBackoffMillis;
  }

  double getBackoffMultiplier() {
    return backoffMultiplier;
  }
}
