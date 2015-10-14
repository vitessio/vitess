package com.youtube.vitess.vtgate;

import java.util.Map;

public class SplitQueryResponse {
  private Map<Query, Long> queries;
  private String error;
  private RPCError err;

  public SplitQueryResponse(Map<Query, Long> queries, String error, RPCError err) {
    this.queries = queries;
    this.error = error;
    this.err = err;
  }

  public Map<Query, Long> getQueries() {
    return queries;
  }

  public String getError() {
    return error;
  }

  public RPCError getErr() {
    return err;
  }

}
