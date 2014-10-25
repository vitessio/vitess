package com.youtube.vitess.vtgate;

public class QueryResponse {
	private QueryResult result;
	private Object session;
	private String error;

	public QueryResponse(QueryResult result, Object session, String error) {
		this.result = result;
		this.session = session;
		this.error = error;
	}

	public QueryResult getResult() {
		return result;
	}

	public Object getSession() {
		return session;
	}

	public String getError() {
		return error;
	}
}
