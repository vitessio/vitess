package com.github.youtube.vitess.jdbc;

import com.google.inject.AbstractModule;
import com.google.inject.BindingAnnotation;
import com.google.inject.Provides;
import com.google.inject.ScopeAnnotation;
import com.google.inject.Scopes;

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

    // Provide vtoccServerSpec only within this scope
    bind(String.class).annotatedWith(VtoccServerSpec.class).toProvider(
        SimpleScope.<String>seededKeyProvider())
        .in(VtoccConnectionCreationScoped.class);
    bind(String.class).annotatedWith(VtoccKeyspace.class).toProvider(
        SimpleScope.<String>seededKeyProvider())
        .in(VtoccConnectionCreationScoped.class);
    // make sure that we would not have two transactions created as the same time
    bind(VtoccTransactionHandler.class).in(VtoccConnectionCreationScoped.class);
  }

  /**
   * Empty class just to satisfy Guice injection rules.
   */
  public static class VtoccConnectionCreationScope extends SimpleScope {
  }

  @Provides
  @VtoccConnectionCreationScoped
  QueryService.SqlQuery getSqlQueryStub(@VtoccServerSpec String vtoccServerSpec) {
    // TODO(timofeyb): implement actual transport to vtocc using bson
    return null;
  }

  @Provides
  @VtoccConnectionCreationScoped
  acolyte.Connection getAcolyteConnection(VtoccStatementHandler vtoccStatementHandler) {
    return acolyte.Driver.connection(vtoccStatementHandler);
  }

  /**
   * Annotates {@link String} with Vtocc server specification.
   */
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.METHOD, ElementType.FIELD, ElementType.PARAMETER})
  @BindingAnnotation
  public @interface VtoccServerSpec {}

  /**
   * Annotates {@link String} with Vtocc key space.
   */
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.METHOD, ElementType.FIELD, ElementType.PARAMETER})
  @BindingAnnotation
  public @interface VtoccKeyspace {}

  /**
   * Active during connection creation to Vtocc, notably {@link VtoccServerSpec} lives in this
   * scope. Implemented by {@link SimpleScope}.
   */
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.METHOD, ElementType.FIELD, ElementType.PARAMETER})
  @ScopeAnnotation
  public @interface VtoccConnectionCreationScoped {}
}
