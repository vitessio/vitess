package com.youtube.vitess.gorpc;

public class Exceptions {
  /**
   * Exception raised due to connection issues or RPC protocol violations
   */
  @SuppressWarnings("serial")
  public static class GoRpcException extends Exception {
    public GoRpcException(String message) {
      super(message);
    }
  }

  /**
   * Exception caused by the caller due to invalid arguments or wrong sequence of invocations
   */
  @SuppressWarnings("serial")
  public static class ApplicationException extends Exception {
    public ApplicationException(String message) {
      super(message);
    }
  }
}
