package com.youtube.vitess.vtgate;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.google.common.primitives.Ints;
import com.youtube.vitess.vtgate.Row.Cell;

/**
 * Represents a VtGate query result set. For selects, rows are better accessed
 * through the iterator {@link Cursor} class.
 */
public class QueryResult {

	public static final String ROWS_AFFECTED = "RowsAffected";
	public static final String INSERT_ID = "InsertId";
	public static final String FIELDS = "Fields";
	public static final String ROWS = "Rows";
	public static final String NAME = "Name";
	public static final String TYPE = "Type";

	private List<Row> rows;
	private long rowsAffected;
	private long lastRowId;

	public static QueryResult parse(Map<String, Object> result) {
		QueryResult qr = new QueryResult();
		qr.rowsAffected = (Long) result.get(ROWS_AFFECTED);
		qr.lastRowId = (Long) result.get(INSERT_ID);
		qr.rows = new LinkedList<>();
		List<Object> fields = (List<Object>) result.get(FIELDS);
		List<Object> rows = (List<Object>) result.get(ROWS);
		for (Object row : rows) {
			LinkedList<Cell> cells = new LinkedList<>();
			List<Object> cols = (List<Object>) row;
			Iterator<Object> fieldsIter = fields.iterator();
			for (Object col : cols) {
				String cell = new String((byte[]) col);
				Map<String, Object> field = (Map<String, Object>) fieldsIter
						.next();
				String fieldName = new String(
						(byte[]) field.get(NAME));
				int mysqlType = Ints.checkedCast((Long) field
						.get(TYPE));
				FieldType ft = FieldType.get(mysqlType);
				cells.add(new Cell(fieldName, ft.convert(cell), ft.javaType));
			}
			qr.rows.add(new Row(cells));
		}
		return qr;
	}

	public List<Row> getRows() {
		return rows;
	}

	public void setRows(List<Row> rows) {
		this.rows = rows;
	}

	public long getRowsAffected() {
		return rowsAffected;
	}

	public void setRowsAffected(long rowsAffected) {
		this.rowsAffected = rowsAffected;
	}

	public long getLastRowId() {
		return lastRowId;
	}

	public void setLastRowId(long lastRowId) {
		this.lastRowId = lastRowId;
	}
}
