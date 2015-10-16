package com.youtube.vitess.vtgate;

public class RPCError {
  private long code;
  private String message;

  public RPCError(long code, String message) {
    this.code = code;
    this.message = message;
  }

  public long getCode() {
    return code;
  }

  public String getMessage() {
    return message;
  }

}
