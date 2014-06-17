package com.github.youtube.vitess.jdbc.vtocc;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

/**
 * Basic implementation of an {@link RpcController}. This is for use with implementations of {#link
 * BlockingRpcChannel}.
 */
public class BasicRpcController implements RpcController {

  private String error = "";
  private boolean fail = false;

  @Override
  public void reset() {
    error = "";
    fail = false;

  }

  @Override
  public boolean failed() {
    return fail;
  }

  @Override
  public String errorText() {
    return error;
  }

  @Override
  public void startCancel() {

  }

  @Override
  public void setFailed(String reason) {
    fail = true;
    error = reason;
  }

  @Override
  public boolean isCanceled() {
    return false;
  }

  @Override
  public void notifyOnCancel(RpcCallback<Object> callback) {

  }
}
