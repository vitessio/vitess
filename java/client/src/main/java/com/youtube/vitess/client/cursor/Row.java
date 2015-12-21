package com.youtube.vitess.client.cursor;

import com.google.common.primitives.UnsignedLong;
import com.google.protobuf.ByteString;

import com.youtube.vitess.proto.Query;
import com.youtube.vitess.proto.Query.Field;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.ISODateTimeFormat;

import java.math.BigDecimal;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Row {
  private Map<String, Integer> fieldMap;
  private List<Field> fields;
  private List<ByteString> values;
  private Query.Row rawRow;

  public Row(List<Field> fields, Query.Row rawRow, Map<String, Integer> fieldMap) {
    this.fields = fields;
    this.rawRow = rawRow;
    this.fieldMap = fieldMap;
    this.values = extractValues(rawRow.getLengthsList(), rawRow.getValues());
  }

  public List<Field> getFields() {
    return fields;
  }

  public Query.Row getRowProto() {
    return rawRow;
  }

  public Map<String, Integer> getFieldMap() {
    return fieldMap;
  }

  public int findColumn(String columnLabel) throws SQLException {
    if (!fieldMap.containsKey(columnLabel)) {
      throw new SQLDataException("column not found:" + columnLabel);
    }
    return fieldMap.get(columnLabel);
  }

  public Object getObject(String columnLabel) throws SQLException {
    return getObject(findColumn(columnLabel));
  }

  public Object getObject(int columnIndex) throws SQLException {
    if (columnIndex >= values.size()) {
      throw new SQLDataException("invalid columnIndex: " + columnIndex);
    }
    return convertFieldValue(fields.get(columnIndex), values.get(columnIndex));
  }

  public int getInt(String columnLabel) throws SQLException {
    return getInt(findColumn(columnLabel));
  }

  public int getInt(int columnIndex) throws SQLException {
    return (Integer) getAndCheckType(columnIndex, Integer.class);
  }

  public UnsignedLong getULong(String columnLabel) throws SQLException {
    return getULong(findColumn(columnLabel));
  }

  public UnsignedLong getULong(int columnIndex) throws SQLException {
    return (UnsignedLong) getAndCheckType(columnIndex, UnsignedLong.class);
  }

  public String getString(String columnLabel) throws SQLException {
    return getString(findColumn(columnLabel));
  }

  public String getString(int columnIndex) throws SQLException {
    return (String) getAndCheckType(columnIndex, String.class);
  }

  public long getLong(String columnLabel) throws SQLException {
    return getLong(findColumn(columnLabel));
  }

  public long getLong(int columnIndex) throws SQLException {
    return (Long) getAndCheckType(columnIndex, Long.class);
  }

  public double getDouble(String columnLabel) throws SQLException {
    return getDouble(findColumn(columnLabel));
  }

  public double getDouble(int columnIndex) throws SQLException {
    return (Double) getAndCheckType(columnIndex, Double.class);
  }

  public float getFloat(String columnLabel) throws SQLException {
    return getFloat(findColumn(columnLabel));
  }

  public float getFloat(int columnIndex) throws SQLException {
    return (Float) getAndCheckType(columnIndex, Float.class);
  }

  public DateTime getDateTime(String columnLabel) throws SQLException {
    return getDateTime(findColumn(columnLabel));
  }

  public DateTime getDateTime(int columnIndex) throws SQLException {
    return (DateTime) getAndCheckType(columnIndex, DateTime.class);
  }

  public byte[] getBytes(String columnLabel) throws SQLException {
    return getBytes(findColumn(columnLabel));
  }

  public byte[] getBytes(int columnIndex) throws SQLException {
    return (byte[]) getAndCheckType(columnIndex, byte[].class);
  }

  public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
    return getBigDecimal(findColumn(columnLabel));
  }

  public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
    return (BigDecimal) getAndCheckType(columnIndex, BigDecimal.class);
  }

  public short getShort(String columnLabel) throws SQLException {
    return getShort(findColumn(columnLabel));
  }

  public short getShort(int columnIndex) throws SQLException {
    return (Short) getAndCheckType(columnIndex, Short.class);
  }

  private Object getAndCheckType(int columnIndex, Class<?> cls) throws SQLException {
    Object o = getObject(columnIndex);
    if (o != null && !cls.isInstance(o)) {
      throw new SQLDataException(
          "type mismatch, expected:" + cls.getName() + ", actual: " + o.getClass().getName());
    }
    return o;
  }

  private static Object convertFieldValue(Field field, ByteString value) throws SQLException {
    if (value == null) {
      // Only a MySQL NULL value should return null.
      // A zero-length value should return the appropriate type.
      return null;
    }

    // Note: We don't actually know the charset in which the value is encoded.
    // For dates and numeric values, we just assume UTF-8 because they (hopefully) don't contain
    // anything outside 7-bit ASCII, which (hopefully) is a subset of the actual charset.
    // For strings, we return byte[] and the application is responsible for using the right charset.
    switch (field.getType()) {
      case DECIMAL:
        return new BigDecimal(value.toStringUtf8());
      case INT8: // fall through
      case UINT8: // fall through
      case INT16: // fall through
      case UINT16: // fall through
      case INT24: // fall through
      case UINT24: // fall through
      case INT32:
        return Integer.valueOf(value.toStringUtf8());
      case UINT32: // fall through
      case INT64:
        return Long.valueOf(value.toStringUtf8());
      case UINT64:
        return UnsignedLong.valueOf(value.toStringUtf8());
      case FLOAT32:
        return Float.valueOf(value.toStringUtf8());
      case FLOAT64:
        return Double.valueOf(value.toStringUtf8());
      case NULL_TYPE:
        return null;
      case DATE:
        return DateTime.parse(value.toStringUtf8(), ISODateTimeFormat.date());
      case TIME:
        return DateTime.parse(value.toStringUtf8(), DateTimeFormat.forPattern("HH:mm:ss"));
      case DATETIME: // fall through
      case TIMESTAMP:
        return DateTime.parse(value.toStringUtf8().replace(' ', 'T'));
      case YEAR:
        return Short.valueOf(value.toStringUtf8());
      case ENUM: // fall through
      case SET: // fall through
      case BIT:
        return value.toStringUtf8();
      case TEXT: // fall through
      case BLOB: // fall through
      case VARCHAR: // fall through
      case VARBINARY: // fall through
      case CHAR: // fall through
      case BINARY:
        return value.toByteArray();
      default:
        throw new SQLDataException("unknown field type: " + field.getType());
    }
  }

  /**
   * Extract cell values from the single-buffer wire format.
   *
   * <p>See the docs for the {@code Row} message in {@code query.proto}.
   */
  private static List<ByteString> extractValues(List<Long> lengths, ByteString buf) {
    List<ByteString> list = new ArrayList<ByteString>(lengths.size());

    int start = 0;
    for (long len : lengths) {
      if (len < 0) {
        // This indicates a MySQL NULL value, to distinguish it from a zero-length string.
        list.add((ByteString) null);
      } else {
        // Lengths are returned as long, but ByteString.substring() only supports int.
        list.add(buf.substring(start, start + (int) len));
        start += len;
      }
    }

    return list;
  }
}
