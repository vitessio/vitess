package com.youtube.vitess.vtgate.rpcclient;

import java.util.List;

import com.youtube.vitess.vtgate.BatchQuery;
import com.youtube.vitess.vtgate.BatchQueryResponse;
import com.youtube.vitess.vtgate.Exceptions.ConnectionException;
import com.youtube.vitess.vtgate.Field;
import com.youtube.vitess.vtgate.Query;
import com.youtube.vitess.vtgate.QueryResponse;
import com.youtube.vitess.vtgate.QueryResult;
import com.youtube.vitess.vtgate.SplitQueryRequest;
import com.youtube.vitess.vtgate.SplitQueryResponse;

public interface RpcClient {

	public Object begin() throws ConnectionException;

	public void commit(Object session) throws ConnectionException;

	public void rollback(Object session) throws ConnectionException;

	public QueryResponse execute(Query query) throws ConnectionException;

	public QueryResult streamNext(List<Field> fields)
			throws ConnectionException;

	public BatchQueryResponse batchExecute(BatchQuery query)
			throws ConnectionException;

	public SplitQueryResponse splitQuery(SplitQueryRequest request)
			throws ConnectionException;

	public void close() throws ConnectionException;
}