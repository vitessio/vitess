package com.youtube.vitess.client.cursor;

import com.youtube.vitess.proto.Query;
import com.youtube.vitess.proto.Query.Field;
import com.youtube.vitess.proto.Query.QueryResult;

import java.sql.SQLDataException;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;

/**
 * A {@link Cursor} that serves records from a single {@link QueryResult} object.
 */
public class SimpleCursor extends Cursor {
  private QueryResult queryResult;
  private Iterator<Query.Row> rowIterator;

  public SimpleCursor(QueryResult queryResult) {
    this.queryResult = queryResult;
    rowIterator = queryResult.getRowsList().iterator();
  }

  @Override
  public long getRowsAffected() throws SQLException {
    return queryResult.getRowsAffected();
  }

  @Override
  public long getInsertId() throws SQLException {
    return queryResult.getInsertId();
  }

  @Override
  public List<Field> getFields() throws SQLException {
    return queryResult.getFieldsList();
  }

  @Override
  public void close() throws Exception {
    rowIterator = null;
  }

  @Override
  public Row next() throws SQLException {
    if (rowIterator == null) {
      throw new SQLDataException("next() called on closed Cursor");
    }

    if (rowIterator.hasNext()) {
      return new Row(queryResult.getFieldsList(), rowIterator.next(), getFieldMap());
    }
    return null;
  }
}
