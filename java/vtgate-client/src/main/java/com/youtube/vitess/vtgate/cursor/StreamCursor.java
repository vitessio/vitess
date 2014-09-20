package com.youtube.vitess.vtgate.cursor;

import java.util.Iterator;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.youtube.vitess.vtgate.Exceptions.ConnectionException;
import com.youtube.vitess.vtgate.QueryResult;
import com.youtube.vitess.vtgate.Row;
import com.youtube.vitess.vtgate.rpcclient.RpcClient;

/**
 * StreamCursor is for iterating through stream query results. When the current
 * buffer is completed, StreamCursor does an RPC fetch for the next set until
 * the stream ends or an error occurs.
 *
 */
public class StreamCursor implements Cursor {
	static final Logger logger = LogManager.getLogger(StreamCursor.class
			.getName());

	private QueryResult queryResult;
	private Iterator<Row> iterator;
	private RpcClient client;
	private boolean streamEnded;

	public StreamCursor(QueryResult currentResult, RpcClient client) {
		this.queryResult = currentResult;
		this.iterator = currentResult.getRows().iterator();
		this.client = client;
	}

	@Override
	public boolean hasNext() {
		updateIterator();
		return iterator.hasNext();
	}

	@Override
	public Row next() {
		updateIterator();
		return this.iterator.next();
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException("cannot remove from results");
	}

	@Override
	public Iterator<Row> iterator() {
		return this;
	}

	public long getRowsAffected() {
		throw new UnsupportedOperationException(
				"not supported for streaming cursors");
	}

	public long getLastRowId() {
		throw new UnsupportedOperationException(
				"not supported for streaming cursors");
	}

	/**
	 * Update the iterator by fetching more rows if current buffer is done
	 */
	private void updateIterator() {
		// Either current buffer is not empty or we have reached end of stream,
		// nothing to do here.
		if (this.iterator.hasNext() || streamEnded) {
			return;
		}

		Map<String, Object> reply;
		try {
			reply = client.streamNext();
		} catch (ConnectionException e) {
			logger.error("connection exception while streaming", e);
			throw new RuntimeException(e);
		}

		// null reply indicates EndOfStream mark stream as ended and return
		if (reply == null) {
			streamEnded = true;
			return;
		}

		Map<String, Object> result = (Map<String, Object>) reply.get("Result");
		// Fields are only returned in the StreamExecute calls. Subsequent
		// fetches do not return fields, so propagate it from the current
		// queryResult
		queryResult = QueryResult.parse(result, queryResult.getFields());
		iterator = queryResult.getRows().iterator();
	}
}
