package com.youtube.vitess.client.cursor;

import com.google.common.collect.ImmutableMap;
import com.youtube.vitess.proto.Query.Field;
import com.youtube.vitess.proto.Query.QueryResult;

import java.sql.SQLDataException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

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
 * <p>Where possible, the methods use the same signature and exceptions as
 * {@link java.sql.ResultSet}, but implementing the full {@code ResultSet} interface is not a goal
 * of this class.
 *
 * <p>Each individual {@code Cursor} is not thread-safe; it must be protected if used concurrently.
 * However, two cursors from the same {@link com.youtube.vitess.client.VTGateConn VTGateConn} can be
 * accessed concurrently without additional synchronization.
 */
public abstract class Cursor implements AutoCloseable {
  private Map<String, Integer> fieldMap;

  public abstract long getRowsAffected() throws SQLException;

  public abstract long getInsertId() throws SQLException;

  public abstract Row next() throws SQLException;

  public abstract List<Field> getFields() throws SQLException;

  public int findColumn(String columnLabel) throws SQLException {
    if (!getFieldMap().containsKey(columnLabel)) {
      throw new SQLDataException("column not found:" + columnLabel);
    }
    return getFieldMap().get(columnLabel);
  }

  protected Map<String, Integer> getFieldMap() throws SQLException {
    if (fieldMap == null) {
      List<Field> fields = getFields();
      ImmutableMap.Builder<String, Integer> builder = new ImmutableMap.Builder<>();
      for (int i = 0; i < fields.size(); ++i) {
        builder.put(fields.get(i).getName(), i);
      }
      fieldMap = builder.build();
    }
    return fieldMap;
  }
}
