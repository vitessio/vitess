package com.youtube.vitess.vtgate;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.google.common.primitives.Ints;
import com.youtube.vitess.vtgate.Row.Cell;
import com.youtube.vitess.vtgate.cursor.Cursor;

/**
 * Represents a VtGate query result set. For selects, rows are better accessed
 * through the iterator {@link Cursor}.
 */
public class QueryResult {

	public static final String ROWS_AFFECTED = "RowsAffected";
	public static final String INSERT_ID = "InsertId";
	public static final String FIELDS = "Fields";
	public static final String ROWS = "Rows";
	public static final String NAME = "Name";
	public static final String TYPE = "Type";

	private List<Row> rows;
	private List<Field> fields;
	private long rowsAffected;
	private long lastRowId;

	private QueryResult() {

	}

	public static QueryResult parse(Map<String, Object> result) {
		return parse(result, null);
	}

	public static QueryResult parse(Map<String, Object> result,
			List<Field> fields) {
		QueryResult qr = new QueryResult();
		qr.rowsAffected = (Long) result.get(ROWS_AFFECTED);
		qr.lastRowId = (Long) result.get(INSERT_ID);
		if (fields != null) {
			qr.fields = fields;
		} else {
			qr.populateFields(result);
		}
		qr.populateRows(result);
		return qr;
	}

	void populateFields(Map<String, Object> result) {
		List<Field> fieldList = new LinkedList<>();
		List<Object> fields = (List<Object>) result.get(FIELDS);
		for (Object field : fields) {
			Map<String, Object> fieldAsMap = (Map<String, Object>) field;
			String fieldName = new String((byte[]) fieldAsMap.get(NAME));
			int mysqlType = Ints.checkedCast((Long) fieldAsMap.get(TYPE));
			FieldType fieldType = FieldType.get(mysqlType);
			fieldList.add(new Field(fieldName, fieldType));
		}
		this.fields = fieldList;
	}

	void populateRows(Map<String, Object> result) {
		List<Row> rowList = new LinkedList<>();
		List<Object> rows = (List<Object>) result.get(ROWS);
		for (Object row : rows) {
			LinkedList<Cell> cells = new LinkedList<>();
			List<Object> cols = (List<Object>) row;
			Iterator<Field> fieldsIter = this.fields.iterator();
			for (Object col : cols) {
				String cell = new String((byte[]) col);
				Field field = fieldsIter.next();
				FieldType ft = field.getType();
				cells.add(new Cell(field.getName(), ft.convert(cell),
						ft.javaType));
			}
			rowList.add(new Row(cells));
		}
		this.rows = rowList;
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
