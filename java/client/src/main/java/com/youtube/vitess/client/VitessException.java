package com.youtube.vitess.client;

/**
 * VitessException indicates an error returned by VTGate.
 */
public class VitessException extends Exception {

  public VitessException() {
    super();
  }

  public VitessException(String message) {
    super(message);
  }

  public VitessException(String message, Throwable cause) {
    super(message, cause);
  }

  public VitessException(Throwable cause) {
    super(cause);
  }
}
