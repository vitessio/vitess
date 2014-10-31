package com.youtube.vitess.vtgate;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class BatchQuery {
  private List<String> sqls;
  private List<List<BindVariable>> bindVarsList;
  private String keyspace;
  private String tabletType;
  private List<byte[]> keyspaceIds;
  private Object session;

  private BatchQuery(String keyspace, String tabletType) {
    this.keyspace = keyspace;
    this.tabletType = tabletType;
    this.sqls = new LinkedList<>();
    this.bindVarsList = new LinkedList<>();
  }

  public List<String> getSqls() {
    return sqls;
  }

  public List<List<BindVariable>> getBindVarsList() {
    return bindVarsList;
  }

  public String getTabletType() {
    return tabletType;
  }

  public String getKeyspace() {
    return keyspace;
  }

  public List<byte[]> getKeyspaceIds() {
    return keyspaceIds;
  }

  public Object getSession() {
    return session;
  }

  public void setSession(Object session) {
    this.session = session;
  }

  public static class BatchQueryBuilder {
    private BatchQuery query;

    public BatchQueryBuilder(String keyspace, String tabletType) {
      query = new BatchQuery(keyspace, tabletType);
    }

    public BatchQuery build() {
      if (query.sqls.size() == 0) {
        throw new IllegalStateException("query must have at least one sql");
      }
      if (query.keyspaceIds == null) {
        throw new IllegalStateException("query must have keyspaceIds set");
      }
      return query;
    }

    public BatchQueryBuilder addSqlAndBindVars(String sql, List<BindVariable> bindVars) {
      query.sqls.add(sql);
      query.bindVarsList.add(bindVars);
      return this;
    }

    public BatchQueryBuilder withKeyspaceIds(List<KeyspaceId> keyspaceIds) {
      List<byte[]> kidsBytes = new ArrayList<>();
      for (KeyspaceId kid : keyspaceIds) {
        kidsBytes.add(kid.getBytes());
      }
      query.keyspaceIds = kidsBytes;
      return this;
    }

    public BatchQueryBuilder withAddedKeyspaceId(KeyspaceId keyspaceId) {
      if (query.getKeyspaceIds() == null) {
        query.keyspaceIds = new ArrayList<byte[]>();
      }
      query.getKeyspaceIds().add(keyspaceId.getBytes());
      return this;
    }
  }
}
