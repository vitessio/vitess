package com.youtube.vitess.vtgate;

public class SplitQueryRequest {
	private String sql;
	private String keyspace;
	private int splitsPerShard;

	public SplitQueryRequest(String sql, String keyspace, int splitsPerShard) {
		this.sql = sql;
		this.keyspace = keyspace;
		this.splitsPerShard = splitsPerShard;
	}

	public String getSql() {
		return sql;
	}

	public String getKeyspace() {
		return keyspace;
	}

	public int getSplitsPerShard() {
		return splitsPerShard;
	}
}
