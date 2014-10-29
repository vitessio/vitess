package com.youtube.vitess.vtgate;

import com.youtube.vitess.vtgate.cursor.Cursor;

import java.util.List;

/**
 * Represents a VtGate query result set. For selects, rows are better accessed through the iterator
 * {@link Cursor}.
 */
public class QueryResult {
  private List<Row> rows;
  private List<Field> fields;
  private long rowsAffected;
  private long lastRowId;

  public QueryResult(List<Row> rows, List<Field> fields, long rowsAffected, long lastRowId) {
    this.rows = rows;
    this.fields = fields;
    this.rowsAffected = rowsAffected;
    this.lastRowId = lastRowId;
  }

  public List<Field> getFields() {
    return fields;
  }

  public List<Row> getRows() {
    return rows;
  }

  public long getRowsAffected() {
    return rowsAffected;
  }

  public long getLastRowId() {
    return lastRowId;
  }
}
