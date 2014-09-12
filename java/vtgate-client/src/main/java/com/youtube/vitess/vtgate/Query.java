package com.youtube.vitess.vtgate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Represents a VtGate query request. Use QueryBuilder to construct instances.
 *
 */
public class Query {
	private String sql;
	private String keyspace;
	private Map<String, Object> bindVars;
	private String tabletType;
	private List<String> keyspaceIds;

	private Query(String sql, String keyspace, String tabletType) {
		this.sql = sql;
		this.keyspace = keyspace;
		this.tabletType = tabletType;
	}

	public String getSql() {
		return sql;
	}

	public void setSql(String sql) {
		this.sql = sql;
	}

	public String getKeyspace() {
		return keyspace;
	}

	public void setKeyspace(String keyspace) {
		this.keyspace = keyspace;
	}

	public Map<String, Object> getBindVars() {
		return bindVars;
	}

	public void setBindVars(Map<String, Object> bindVars) {
		this.bindVars = bindVars;
	}

	public String getTabletType() {
		return tabletType;
	}

	public void setTabletType(String tabletType) {
		this.tabletType = tabletType;
	}

	public List<String> getKeyspaceIds() {
		return keyspaceIds;
	}

	public void setKeyspaceIds(List<String> keyspaceIds) {
		this.keyspaceIds = keyspaceIds;
	}

	public static class QueryBuilder {
		private Query query;

		public QueryBuilder(String sql, String keyspace, String tabletType) {
			query = new Query(sql, keyspace, tabletType);
		}

		public Query build() {
			return query;
		}

		public QueryBuilder withBindVars(Map<String, Object> bindVars) {
			query.setBindVars(bindVars);
			return this;
		}

		public QueryBuilder withKeyspaceIds(List<String> keyspaceIds) {
			query.setKeyspaceIds(keyspaceIds);
			return this;
		}

		public QueryBuilder withAddedKeyspaceId(String keyspaceId) {
			if (query.getKeyspaceIds() == null) {
				query.setKeyspaceIds(new ArrayList<String>());
			}
			query.getKeyspaceIds().add(keyspaceId);
			return this;
		}
	}
}
