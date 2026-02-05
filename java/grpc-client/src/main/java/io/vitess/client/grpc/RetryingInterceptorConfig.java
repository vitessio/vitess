/*
 * Copyright 2019 The Vitess Authors.

 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

 *     http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
