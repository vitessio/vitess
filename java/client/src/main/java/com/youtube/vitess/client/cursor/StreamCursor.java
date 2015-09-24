package com.youtube.vitess.client.cursor;

import com.youtube.vitess.client.StreamIterator;
import com.youtube.vitess.proto.Query.Field;
import com.youtube.vitess.proto.Query.QueryResult;
import com.youtube.vitess.proto.Query.Row;

import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Iterator;
import java.util.List;

/**
 * A {@link Cursor} that serves records from the sequence of {@link QueryResult} objects
 * represented by a {@link StreamIterator}.
 */
public class StreamCursor extends Cursor {
  private StreamIterator<QueryResult> streamIterator;
  private Iterator<Row> rowIterator;
  private Row row;

  private List<Field> fields;

  public StreamCursor(StreamIterator<QueryResult> streamIterator) {
    this.streamIterator = streamIterator;
  }

  @Override
  public long getRowsAffected() throws SQLException {
    throw new SQLFeatureNotSupportedException("getRowsAffected() is not supported on StreamCursor");
  }

  @Override
  public long getInsertId() throws SQLException {
    throw new SQLFeatureNotSupportedException("getInsertId() is not supported on StreamCursor");
  }

  @Override
  public List<Field> getFields() throws SQLException {
    if (fields == null) {
      throw new SQLDataException("can't get fields until first streaming result is received");
    }
    return fields;
  }

  @Override
  public void close() throws Exception {
    streamIterator.close();
    streamIterator = null;
  }

  @Override
  public boolean next() throws SQLException {
    if (streamIterator == null) {
      throw new SQLDataException("next() called on closed Cursor");
    }

    // Get the next Row from the current QueryResult.
    if (rowIterator != null && rowIterator.hasNext()) {
      row = rowIterator.next();
      return true;
    }

    // Get the next QueryResult. Loop in case we get a QueryResult with no Rows (e.g. only Fields).
    while (streamIterator.hasNext()) {
      QueryResult queryResult = streamIterator.next();
      if (fields == null) {
        // The first QueryResult should have the fields.
        fields = queryResult.getFieldsList();
      }
      rowIterator = queryResult.getRowsList().iterator();

      // Get the first Row from the new QueryResult.
      if (rowIterator.hasNext()) {
        row = rowIterator.next();
        return true;
      }
    }

    // No more Rows and no more QueryResults.
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
