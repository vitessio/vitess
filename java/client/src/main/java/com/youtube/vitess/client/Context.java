package com.youtube.vitess.client;

import org.joda.time.DateTime;

/**
 * Context carries deadlines, cancelation signals and other request-scoped values across API
 * boundaries and between processes.
 *
 * TODO(shengzhe): implement Context.
 */
public class Context {
  public static Context withDeadline(DateTime deadline) { return new Context(); }

  private Context() {}
}
