package com.youtube.vitess.vtgate.cursor;

import java.util.Iterator;

import com.youtube.vitess.vtgate.Row;

public interface Cursor extends Iterator<Row>, Iterable<Row> {
	public long getRowsAffected();

	public long getLastRowId();
}
