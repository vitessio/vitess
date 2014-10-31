package com.youtube.vitess.gorpc;

import com.google.common.primitives.UnsignedLong;

public class Request {

  private String serviceMethod;
  private UnsignedLong seq;

  public Request(String serviceMethod, UnsignedLong seq) {
    this.serviceMethod = serviceMethod;
    this.seq = seq;
  }

  public String getServiceMethod() {
    return serviceMethod;
  }

  public UnsignedLong getSeq() {
    return seq;
  }
}
