package io.vitess.client.grpc;

/**
 * This class defines what level of exponential backoff to apply in the {@link RetryingInterceptor}.
 * It can be disabled with the {@link #noOpConfig()}.
 */
public class RetryingInterceptorConfig {

  private static final long DISABLED = -1;

  private final long initialBackoffMillis;
  private final long maxBackoffMillis;
  private final double backoffMultiplier;

  private RetryingInterceptorConfig(long initialBackoffMillis, long maxBackoffMillis,
      double backoffMultiplier) {
    this.initialBackoffMillis = initialBackoffMillis;
    this.maxBackoffMillis = maxBackoffMillis;
    this.backoffMultiplier = backoffMultiplier;
  }

  /**
   * Returns a no-op config which will not do any retries.
   */
  public static RetryingInterceptorConfig noOpConfig() {
    return new RetryingInterceptorConfig(DISABLED, DISABLED, 0);
  }

  /**
   * Returns an exponential config with the given values to determine how aggressive of a backoff is
   * needed
   *
   * @param initialBackoffMillis how long in millis to backoff off for the first attempt
   * @param maxBackoffMillis the maximum value the backoff time can grow to, at which point all
   *     future retries will backoff at this amount
   * @param backoffMultiplier how quickly the backoff time should grow
   */
  public static RetryingInterceptorConfig exponentialConfig(long initialBackoffMillis,
      long maxBackoffMillis, double backoffMultiplier) {
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
