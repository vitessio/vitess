package com.youtube.vitess.client.cursor;

import com.youtube.vitess.proto.Query.Field;
import com.youtube.vitess.proto.Query.QueryResult;
import com.youtube.vitess.proto.Query.Row;

import java.sql.SQLDataException;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;

/**
 * A {@link Cursor} that serves records from a single {@link QueryResult} object.
 */
public class SimpleCursor extends Cursor {
  private QueryResult queryResult;
  private Iterator<Row> rowIterator;
  private Row row;

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
    row = null;
    rowIterator = null;
  }

  @Override
  public boolean next() throws SQLException {
    if (rowIterator == null) {
      throw new SQLDataException("next() called on closed Cursor");
    }

    if (rowIterator.hasNext()) {
      row = rowIterator.next();
      return true;
    }

    row = null;
    return false;
  }

  @Override
  protected Row getCurrentRow() throws SQLException {
    if (row == null) {
      throw new SQLDataException("no current row");
    }
    return row;
  }
}
