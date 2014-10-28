package com.youtube.vitess.vtgate.cursor;

import java.util.Iterator;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.youtube.vitess.vtgate.Exceptions.ConnectionException;
import com.youtube.vitess.vtgate.QueryResult;
import com.youtube.vitess.vtgate.Row;
import com.youtube.vitess.vtgate.rpcclient.RpcClient;

/**
 * StreamCursor is for iterating through stream query results. When the current buffer is completed,
 * StreamCursor does an RPC fetch for the next set until the stream ends or an error occurs.
 *
 */
public class StreamCursor implements Cursor {
  static final Logger logger = LogManager.getLogger(StreamCursor.class.getName());

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
    fetchMoreIfNeeded();
    return iterator.hasNext();
  }

  @Override
  public Row next() {
    fetchMoreIfNeeded();
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
    throw new UnsupportedOperationException("not supported for streaming cursors");
  }

  public long getLastRowId() {
    throw new UnsupportedOperationException("not supported for streaming cursors");
  }

  /**
   * Update the iterator by fetching more rows if current buffer is done
   */
  private void fetchMoreIfNeeded() {
    // Either current buffer is not empty or we have reached end of stream,
    // nothing to do here.
    if (this.iterator.hasNext() || streamEnded) {
      return;
    }

    QueryResult qr;
    try {
      qr = client.streamNext(queryResult.getFields());
    } catch (ConnectionException e) {
      logger.error("connection exception while streaming", e);
      throw new RuntimeException(e);
    }

    // null reply indicates EndOfStream, mark stream as ended and return
    if (qr == null) {
      streamEnded = true;
      return;
    }

    // For scatter streaming queries, VtGate sends fields data from each
    // shard. Since fields has already been fetched, just ignore these and
    // fetch the next batch.
    if (qr.getRows().size() == 0) {
      fetchMoreIfNeeded();
    } else {
      // We got more rows, update the current buffer and iterator
      queryResult = qr;
      iterator = queryResult.getRows().iterator();
    }
  }
}
