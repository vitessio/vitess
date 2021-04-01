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

import io.grpc.Attributes;
import io.grpc.CallCredentials;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;

import java.util.Objects;
import java.util.concurrent.Executor;

/**
 * {@link CallCredentials} that applies plain username and password.
 */
public class StaticAuthCredentials extends CallCredentials {

  private static final Metadata.Key<String> USERNAME =
      Metadata.Key.of("username", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> PASSWORD =
      Metadata.Key.of("password", Metadata.ASCII_STRING_MARSHALLER);

  private final String username;
  private final String password;

  public StaticAuthCredentials(String username, String password) {
    this.username = Objects.requireNonNull(username);
    this.password = Objects.requireNonNull(password);
  }

  @Override
  public void applyRequestMetadata(RequestInfo requestInfo,
                                   Executor executor, MetadataApplier applier) {
    executor.execute(() -> {
      try {
        Metadata headers = new Metadata();
        headers.put(USERNAME, username);
        headers.put(PASSWORD, password);
        applier.apply(headers);
      } catch (Throwable exc) {
        applier.fail(Status.UNAUTHENTICATED.withCause(exc));
      }
    });
  }

  @Override
  public void thisUsesUnstableApi() {
  }
}
