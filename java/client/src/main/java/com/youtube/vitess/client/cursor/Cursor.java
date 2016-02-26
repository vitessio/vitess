package com.youtube.vitess.client.cursor;

import com.youtube.vitess.proto.Query.Field;
import com.youtube.vitess.proto.Query.QueryResult;

import java.sql.SQLDataException;
import java.sql.SQLException;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Provides access to the result rows of a query.
 *
 * <p>{@code Cursor} wraps an underlying Vitess {@link QueryResult} object, converting column
 * values from the raw result values to Java types. In the case of streaming queries, the
 * {@link StreamCursor} implementation will also fetch more {@code QueryResult} objects as
 * necessary.
 *
 * <p>Similar to {@link java.sql.ResultSet}, a {@code Cursor} is initially positioned before the
 * first row, and the first call to {@link #next()} moves to the first row. The getter methods
 * return the value of the specified column within the current row. The {@link #close()} method
 * should be called to free resources when done, regardless of whether all the rows were processed.
 *
 * <p>Each individual {@code Cursor} is not thread-safe; it must be protected if used concurrently.
 * However, two cursors from the same {@link com.youtube.vitess.client.VTGateConn VTGateConn} can be
 * accessed concurrently without additional synchronization.
 */
@NotThreadSafe
public abstract class Cursor implements AutoCloseable {
  private FieldMap fieldMap;

  public abstract long getRowsAffected() throws SQLException;

  public abstract long getInsertId() throws SQLException;

  public abstract Row next() throws SQLException;

  public abstract List<Field> getFields() throws SQLException;

  public int findColumn(String columnLabel) throws SQLException {
    Integer columnIndex = getFieldMap().getIndex(columnLabel);
    if (columnIndex == null) {
      throw new SQLDataException("column not found:" + columnLabel);
    }
    return columnIndex;
  }

  protected FieldMap getFieldMap() throws SQLException {
    if (fieldMap == null) {
      fieldMap = new FieldMap(getFields());
    }
    return fieldMap;
  }
}
