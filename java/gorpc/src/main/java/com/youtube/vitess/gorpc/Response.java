package com.youtube.vitess.gorpc;

import com.google.common.primitives.UnsignedLong;

public class Response {
  private String serviceMethod;
  private UnsignedLong seq;
  private String error;
  private Object reply;

  public String getServiceMethod() {
    return serviceMethod;
  }

  public void setServiceMethod(String serviceMethod) {
    this.serviceMethod = serviceMethod;
  }

  public UnsignedLong getSeq() {
    return seq;
  }

  public void setSeq(UnsignedLong seq) {
    this.seq = seq;
  }

  public String getError() {
    return error;
  }

  public void setError(String error) {
    this.error = error;
  }

  public Object getReply() {
    return reply;
  }

  public void setReply(Object reply) {
    this.reply = reply;
  }
}
