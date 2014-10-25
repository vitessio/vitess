package com.youtube.vitess.vtgate;

import java.util.List;

public class BatchQueryResponse {
	private List<QueryResult> results;
	private Object session;
	private String error;

	public BatchQueryResponse(List<QueryResult> results, Object session,
			String error) {
		this.results = results;
		this.session = session;
		this.error = error;
	}

	public List<QueryResult> getResults() {
		return results;
	}

	public Object getSession() {
		return session;
	}

	public String getError() {
		return error;
	}
}
