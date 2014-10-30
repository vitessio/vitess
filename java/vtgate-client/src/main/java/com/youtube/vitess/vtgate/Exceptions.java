package com.youtube.vitess.vtgate;

public class Exceptions {

  /**
   * Exception raised at the tablet or MySQL layer due to issues such as invalid syntax, etc.
   */
  @SuppressWarnings("serial")
  public static class DatabaseException extends Exception {
    public DatabaseException(String message) {
      super(message);
    }
  }

  /**
   * Exception raised by MySQL due to violation of unique key constraint
   */
  @SuppressWarnings("serial")
  public static class IntegrityException extends DatabaseException {
    public IntegrityException(String message) {
      super(message);
    }
  }

  /**
   * Exception caused due to irrecoverable connection failures or other low level exceptions
   */
  @SuppressWarnings("serial")
  public static class ConnectionException extends Exception {
    public ConnectionException(String message) {
      super(message);
    }
  }

  /**
   * Exception raised due to fetching a non-existent field or with the wrong type
   */
  @SuppressWarnings("serial")
  public static class InvalidFieldException extends Exception {
    public InvalidFieldException(String message) {
      super(message);
    }
  }
}
