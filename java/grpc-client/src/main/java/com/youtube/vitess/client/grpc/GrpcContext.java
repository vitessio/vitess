package com.youtube.vitess.client.grpc;

import com.youtube.vitess.client.Context;

import java.util.concurrent.TimeUnit;

/**
 * GrpcContext converts from the Vitess wrapper Context
 * to the form of Context used in gRPC.
 *
 * <p>It implements AutoCloseable, so it should typically be used like:
 * <p><code>try (GrpcContext gctx = new GrpcContext(ctx)) { ... }</code>
 */
class GrpcContext implements AutoCloseable {
  private io.grpc.Context ctx;

  public GrpcContext(Context ctx) {
    if (ctx != null && ctx.getDeadline() != null) {
      this.ctx = io.grpc.Context.current().withDeadlineAfter(
          ctx.getDeadline().getMillis(), TimeUnit.MILLISECONDS);
      this.ctx.attach();
    }
  }

  @Override
  public void close() throws Exception {
    if (ctx != null) {
      ctx.detach();
    }
  }
}
