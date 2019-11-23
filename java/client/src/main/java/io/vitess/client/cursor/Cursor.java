/*
 * Copyright 2019 The Vitess Authors.

 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

 *     http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.vitess.client.cursor;

import static com.google.common.base.Preconditions.checkNotNull;

import io.vitess.proto.Query.Field;
import io.vitess.proto.Query.QueryResult;

import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.List;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Provides access to the result rows of a query.
 *
 * <p>{@code Cursor} wraps an underlying Vitess {@link QueryResult} object, converting column
 * values from the raw result values to Java types. In the case of streaming queries, the {@link
 * StreamCursor} implementation will also fetch more {@code QueryResult} objects as necessary.
 *
 * <p>Similar to {@link java.sql.ResultSet}, a {@code Cursor} is initially positioned before the
 * first row, and the first call to {@link #next()} moves to the first row. The getter methods
 * return the value of the specified column within the current row. The {@link #close()} method
 * should be called to free resources when done, regardless of whether all the rows were processed.
 *
 * <p>Each individual {@code Cursor} is not thread-safe; it must be protected if used concurrently.
 * However, two cursors from the same {@link io.vitess.client.VTGateConn VTGateConn} can be accessed
 * concurrently without additional synchronization.
 */
@NotThreadSafe
public abstract class Cursor implements AutoCloseable {

  /**
   * A pre-built {@link FieldMap}, shared by each {@link Row}.
   *
   * <p>Although {@link Cursor} is not supposed to be used by multiple threads,
   * the asynchronous API makes it unavoidable that a {@code Cursor} may be created in one thread
   * and then sent to another. We therefore declare {@code fieldMap} as {@code volatile} to
   * guarantee the value set by the constructor is seen by all threads.
   */
  private volatile FieldMap fieldMap;

  /**
   * Returns the number of rows affected.
   *
   * @throws SQLException if the server returns an error.
   * @throws SQLFeatureNotSupportedException if the cursor type doesn't support this.
   */
  public abstract long getRowsAffected() throws SQLException;

  /**
   * Returns the ID of the last insert.
   *
   * @throws SQLException if the server returns an error.
   * @throws SQLFeatureNotSupportedException if the cursor type doesn't support this.
   */
  public abstract long getInsertId() throws SQLException;

  /**
   * Returns the next {@link Row}, or {@code null} if there are no more rows.
   *
   * @throws SQLException if the server returns an error.
   */
  @Nullable
  public abstract Row next() throws SQLException;

  /**
   * Returns the list of fields.
   *
   * @throws SQLException if the server returns an error.
   */
  public abstract List<Field> getFields() throws SQLException;

  /**
   * Returns the column index for a given column name.
   *
   * @throws SQLDataException if the column is not found.
   */
  public final int findColumn(String columnLabel) throws SQLException {
    Integer columnIndex = getFieldMap().getIndex(checkNotNull(columnLabel));
    if (columnIndex == null) {
      throw new SQLDataException("column not found:" + columnLabel);
    }
    return columnIndex;
  }

  protected final FieldMap getFieldMap() throws SQLException {
    if (fieldMap == null) {
      fieldMap = new FieldMap(getFields());
    }
    return fieldMap;
  }
}
