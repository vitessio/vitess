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
public class StaticAuthCredentials implements CallCredentials {

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
  public void applyRequestMetadata(MethodDescriptor<?, ?> method, Attributes attrs,
      Executor executor, MetadataApplier applier) {
    executor.execute(() -> {
      try {
        Metadata headers = new Metadata();
        headers.put(USERNAME, username);
        headers.put(PASSWORD, password);
        applier.apply(headers);
      } catch (Throwable e) {
        applier.fail(Status.UNAUTHENTICATED.withCause(e));
      }
    });
  }

  @Override
  public void thisUsesUnstableApi() {
  }
}
