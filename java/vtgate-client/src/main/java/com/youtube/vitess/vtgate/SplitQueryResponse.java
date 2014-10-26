package com.youtube.vitess.vtgate;

import java.util.Map;

public class SplitQueryResponse {
  private Map<Query, Long> queries;
  private String error;

  public SplitQueryResponse(Map<Query, Long> queries, String error) {
    this.queries = queries;
    this.error = error;
  }

  public Map<Query, Long> getQueries() {
    return queries;
  }

  public String getError() {
    return error;
  }

}
