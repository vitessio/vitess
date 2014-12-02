package com.youtube.vitess.vtgate;

public class SplitQueryRequest {
  private String sql;
  private String keyspace;
  private int splitCount;

  public SplitQueryRequest(String sql, String keyspace, int splitCount) {
    this.sql = sql;
    this.keyspace = keyspace;
    this.splitCount = splitCount;
  }

  public String getSql() {
    return sql;
  }

  public String getKeyspace() {
    return keyspace;
  }

  public int getSplitCount() {
    return splitCount;
  }
}
