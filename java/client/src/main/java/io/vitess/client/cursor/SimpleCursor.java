package io.vitess.client.cursor;

import io.vitess.proto.Query;
import io.vitess.proto.Query.Field;
import io.vitess.proto.Query.QueryResult;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A {@link Cursor} that serves records from a single {@link QueryResult} object.
 */
@NotThreadSafe
public class SimpleCursor extends Cursor {
  private final QueryResult queryResult;
  private final Iterator<Query.Row> rowIterator;

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
    // SimpleCursor doesn't need to do anything.
  }

  @Override
  public Row next() throws SQLException {
    if (rowIterator.hasNext()) {
      return new Row(getFieldMap(), rowIterator.next());
    }
    return null;
  }
}
