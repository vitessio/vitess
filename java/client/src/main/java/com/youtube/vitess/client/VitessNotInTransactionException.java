package com.youtube.vitess.client;

/**
 * VitessNotInTransactionException indicates a request is not wrapped in a transaction.
 */
public class VitessNotInTransactionException extends Exception {

  public VitessNotInTransactionException() {
    super();
  }

  public VitessNotInTransactionException(String message) {
    super(message);
  }

  public VitessNotInTransactionException(String message, Throwable cause) {
    super(message, cause);
  }

  public VitessNotInTransactionException(Throwable cause) {
    super(cause);
  }
}
