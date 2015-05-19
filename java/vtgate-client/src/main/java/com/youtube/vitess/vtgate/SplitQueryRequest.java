package com.youtube.vitess.vtgate;

public class SplitQueryRequest {
  private String sql;
  private String keyspace;
  private String splitColumn;
  private int splitCount;

  public SplitQueryRequest(String sql, String keyspace, int splitCount, String splitColumn) {
    this.sql = sql;
    this.keyspace = keyspace;
    this.splitCount = splitCount;
    this.splitColumn = splitColumn;
  }

  public String getSql() {
    return this.sql;
  }

  public String getKeyspace() {
    return this.keyspace;
  }

  public int getSplitCount() {
    return this.splitCount;
  }

  public String getSplitColumn() {
    return this.splitColumn;
  }
}
