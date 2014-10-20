package com.youtube.vitess.vtgate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class BatchQuery {
	private List<String> sqls;
	private List<Map<String, Object>> bindVarsList;
	private String keyspace;
	private String tabletType;
	private List<byte[]> keyspaceIds;

	private BatchQuery(String keyspace, String tabletType) {
		this.keyspace = keyspace;
		this.tabletType = tabletType;
		this.sqls = new LinkedList<>();
		this.bindVarsList = new LinkedList<>();
	}

	public List<String> getSqls() {
		return sqls;
	}

	public List<Map<String, Object>> getBindVarsList() {
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

	public void populate(Map<String, Object> map) {
		List<Map<String, Object>> queries = new LinkedList<>();
		Iterator<String> sqlIter = sqls.iterator();
		Iterator<Map<String, Object>> bvIter = bindVarsList.iterator();
		while (sqlIter.hasNext()) {
			Map<String, Object> query = new HashMap<>();
			query.put("Sql", sqlIter.next());
			query.put("BindVariables", bvIter.next());
			queries.add(query);
		}
		map.put("Queries", queries);
		map.put("Keyspace", keyspace);
		map.put("TabletType", tabletType);
		map.put("KeyspaceIds", keyspaceIds);
	}

	public static class BatchQueryBuilder {
		private BatchQuery query;

		public BatchQueryBuilder(String keyspace, String tabletType) {
			query = new BatchQuery(keyspace, tabletType);
		}

		public BatchQuery build() {
			if (query.sqls.size() == 0) {
				throw new IllegalStateException(
						"query must have at least one sql");
			}
			if (query.keyspaceIds == null) {
				throw new IllegalStateException(
						"query must have keyspaceIds set");
			}
			return query;
		}

		public BatchQueryBuilder withAddedSqlBindVars(String sql,
				Map<String, Object> bindVars) {
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
