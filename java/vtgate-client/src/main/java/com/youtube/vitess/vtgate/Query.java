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
	private List<byte[]> keyspaceIds;
	private boolean stream;

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

	public List<byte[]> getKeyspaceIds() {
		return keyspaceIds;
	}

	public void setKeyspaceIds(List<byte[]> keyspaceIds) {
		this.keyspaceIds = keyspaceIds;
	}

	public boolean isStream() {
		return stream;
	}

	public void setStream(boolean stream) {
		this.stream = stream;
	}

	public void populate(Map<String, Object> map) {
		map.put("Sql", sql);
		map.put("Keyspace", keyspace);
		map.put("TabletType", tabletType);
		map.put("BindVariables", bindVars);

		if (keyspaceIds != null) {
			map.put("KeyspaceIds", keyspaceIds);
		}
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

		public QueryBuilder withKeyspaceIds(List<KeyspaceId> keyspaceIds) {
			List<byte[]> kidsBytes = new ArrayList<>();
			for (KeyspaceId kid : keyspaceIds) {
				kidsBytes.add(kid.getBytes());
			}
			query.setKeyspaceIds(kidsBytes);
			return this;
		}

		public QueryBuilder withStream(boolean stream) {
			query.setStream(stream);
			return this;
		}

		public QueryBuilder withAddedKeyspaceId(KeyspaceId keyspaceId) {
			if (query.getKeyspaceIds() == null) {
				query.setKeyspaceIds(new ArrayList<byte[]>());
			}
			query.getKeyspaceIds().add(keyspaceId.getBytes());
			return this;
		}
	}
}
