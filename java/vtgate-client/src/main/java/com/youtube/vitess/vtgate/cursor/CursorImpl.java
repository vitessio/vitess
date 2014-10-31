package com.youtube.vitess.vtgate.cursor;

import com.youtube.vitess.vtgate.QueryResult;
import com.youtube.vitess.vtgate.Row;

import java.util.Iterator;

public class CursorImpl implements Cursor {
  private QueryResult result;
  private Iterator<Row> iter;

  public CursorImpl(QueryResult result) {
    this.result = result;
    this.iter = result.getRows().iterator();
  }

  public Row next() {
    return this.iter.next();
  }

  public boolean hasNext() {
    return this.iter.hasNext();
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
    return result.getRowsAffected();
  }

  public long getLastRowId() {
    return result.getLastRowId();
  }

}
