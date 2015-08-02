package com.youtube.vitess.client;

/**
 * VitessRpcException indicates a rpc error between client and VTGate.
 */
public class VitessRpcException extends Exception {

  public VitessRpcException() {
    super();
  }

  public VitessRpcException(String message) {
    super(message);
  }

  public VitessRpcException(String message, Throwable cause) {
    super(message, cause);
  }

  public VitessRpcException(Throwable cause) {
    super(cause);
  }
}
