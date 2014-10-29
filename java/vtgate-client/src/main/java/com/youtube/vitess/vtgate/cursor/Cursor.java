package com.youtube.vitess.vtgate.cursor;

import com.youtube.vitess.vtgate.Row;

import java.util.Iterator;

public interface Cursor extends Iterator<Row>, Iterable<Row> {
  public long getRowsAffected();

  public long getLastRowId();
}
