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
	private List<Map<String, byte[]>> keyRanges;
	private boolean streaming;

	private Query(String sql, String keyspace, String tabletType) {
		this.sql = sql;
		this.keyspace = keyspace;
		this.tabletType = tabletType;
	}

	public String getSql() {
		return sql;
	}

	public String getKeyspace() {
		return keyspace;
	}

	public Map<String, Object> getBindVars() {
		return bindVars;
	}

	public String getTabletType() {
		return tabletType;
	}

	public List<byte[]> getKeyspaceIds() {
		return keyspaceIds;
	}

	public List<Map<String, byte[]>> getKeyRanges() {
		return keyRanges;
	}

	public boolean isStreaming() {
		return streaming;
	}

	public void populate(Map<String, Object> map) {
		map.put("Sql", sql);
		map.put("Keyspace", keyspace);
		map.put("TabletType", tabletType);
		map.put("BindVariables", bindVars);

		if (keyspaceIds != null) {
			map.put("KeyspaceIds", keyspaceIds);
		} else {
			map.put("KeyRanges", keyRanges);
		}
	}

	public static class QueryBuilder {
		private Query query;

		public QueryBuilder(String sql, String keyspace, String tabletType) {
			query = new Query(sql, keyspace, tabletType);
		}

		public Query build() {
			if (query.keyRanges == null && query.keyspaceIds == null) {
				throw new IllegalStateException(
						"query must have either keyspaceIds or keyRanges");
			}
			if (query.keyRanges != null && query.keyspaceIds != null) {
				throw new IllegalStateException(
						"query cannot have both keyspaceIds and keyRanges");
			}
			return query;
		}

		public QueryBuilder withBindVars(Map<String, Object> bindVars) {
			query.bindVars = bindVars;
			return this;
		}

		public QueryBuilder withKeyspaceIds(List<KeyspaceId> keyspaceIds) {
			List<byte[]> kidsBytes = new ArrayList<>();
			for (KeyspaceId kid : keyspaceIds) {
				kidsBytes.add(kid.getBytes());
			}
			query.keyspaceIds = kidsBytes;
			return this;
		}

		public QueryBuilder withKeyRanges(List<KeyRange> keyRanges) {
			List<Map<String, byte[]>> keyRangeMaps = new ArrayList<>();
			for (KeyRange kr : keyRanges) {
				keyRangeMaps.add(kr.toMap());
			}
			query.keyRanges = keyRangeMaps;
			return this;
		}

		public QueryBuilder withStreaming(boolean streaming) {
			query.streaming = streaming;
			return this;
		}

		public QueryBuilder withAddedKeyspaceId(KeyspaceId keyspaceId) {
			if (query.getKeyspaceIds() == null) {
				query.keyspaceIds = new ArrayList<byte[]>();
			}
			query.getKeyspaceIds().add(keyspaceId.getBytes());
			return this;
		}

		public QueryBuilder withAddedKeyRange(KeyRange keyRange) {
			if (query.getKeyRanges() == null) {
				query.keyRanges = new ArrayList<Map<String, byte[]>>();
			}
			query.getKeyRanges().add(keyRange.toMap());
			return this;
		}
	}
}
