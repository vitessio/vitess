package com.github.youtube.vitess.jdbc.vtocc;

import com.google.inject.AbstractModule;
import com.google.inject.BindingAnnotation;
import com.google.inject.Provides;
import com.google.inject.ScopeAnnotation;
import com.google.protobuf.BlockingRpcChannel;
import com.google.protobuf.RpcController;

import com.github.youtube.vitess.jdbc.vtocc.QueryService.SqlQuery.BlockingInterface;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * All required binding for instantiating Vtocc connections.
 */
public class VtoccModule extends AbstractModule {

  @Override
  protected void configure() {
    // we inject scope as a singleton and we use it as a scope
    VtoccConnectionCreationScope vtoccConnectionCreationScope = new VtoccConnectionCreationScope();
    bind(VtoccConnectionCreationScope.class).toInstance(vtoccConnectionCreationScope);
    bindScope(VtoccConnectionCreationScoped.class, vtoccConnectionCreationScope);

    bind(BlockingRpcChannel.class).toProvider(
        SimpleScope.<BlockingRpcChannel>seededKeyProvider())
        .in(VtoccConnectionCreationScoped.class);

    bind(String.class).annotatedWith(VtoccKeyspaceShard.class).toProvider(
        SimpleScope.<String>seededKeyProvider())
        .in(VtoccConnectionCreationScoped.class);
    // make sure that we would not have two transactions created as the same time
    bind(VtoccTransactionHandler.class).in(VtoccConnectionCreationScoped.class);
  }

  @Provides
  @VtoccConnectionCreationScoped
  BlockingInterface getSqlQueryStub(BlockingRpcChannel channel) {
    return QueryService.SqlQuery.newBlockingStub(channel);
  }

  @Provides
  RpcController getRpcControllerProvider() {
    return new BasicRpcController();
  }

  @Provides
  @VtoccConnectionCreationScoped
  acolyte.Connection getAcolyteConnection(VtoccStatementHandler vtoccStatementHandler) {
    return acolyte.Driver.connection(vtoccStatementHandler);
  }

  /**
   * Annotates {@link String} with Vtocc key space.
   */
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.METHOD, ElementType.FIELD, ElementType.PARAMETER})
  @BindingAnnotation
  public @interface VtoccKeyspaceShard {

  }

  /**
   * Active during connection creation to Vtocc, Implemented by {@link SimpleScope}.
   */
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.METHOD, ElementType.FIELD, ElementType.PARAMETER})
  @ScopeAnnotation
  public @interface VtoccConnectionCreationScoped {

  }

  /**
   * Empty class just to satisfy Guice injection rules.
   */
  public static class VtoccConnectionCreationScope extends SimpleScope {

  }
}
