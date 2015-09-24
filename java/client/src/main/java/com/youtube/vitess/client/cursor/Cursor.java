package com.youtube.vitess.client.cursor;

import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.UnsignedLong;
import com.google.protobuf.ByteString;

import com.youtube.vitess.proto.Query.Field;
import com.youtube.vitess.proto.Query.QueryResult;
import com.youtube.vitess.proto.Query.Row;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.ISODateTimeFormat;

import java.math.BigDecimal;
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
  public abstract long getRowsAffected() throws SQLException;

  public abstract long getInsertId() throws SQLException;

  public abstract List<Field> getFields() throws SQLException;

  public abstract boolean next() throws SQLException;

  private Map<String, Integer> fieldMap;

  public int findColumn(String columnLabel) throws SQLException {
    if (fieldMap == null) {
      ImmutableMap.Builder<String, Integer> builder = new ImmutableMap.Builder<>();
      List<Field> fields = getFields();
      for (int i = 0; i < fields.size(); ++i) {
        builder.put(fields.get(i).getName(), i);
      }
      fieldMap = builder.build();
    }
    if (!fieldMap.containsKey(columnLabel)) {
      throw new SQLDataException("column not found:" + columnLabel);
    }
    return fieldMap.get(columnLabel);
  }

  public Object getObject(String columnLabel) throws SQLException {
    return getObject(findColumn(columnLabel));
  }

  public Object getObject(int columnIndex) throws SQLException {
    Row row = getCurrentRow();
    if (columnIndex >= row.getValuesCount()) {
      throw new SQLDataException("invalid columnIndex: " + columnIndex);
    }
    return convertFieldValue(getFields().get(columnIndex), row.getValues(columnIndex));
  }

  public Integer getInt(String columnLabel) throws SQLException {
    return (Integer) getAndCheckType(columnLabel, Integer.class);
  }

  public Integer getInt(int columnIndex) throws SQLException {
    return (Integer) getAndCheckType(columnIndex, Integer.class);
  }

  public UnsignedLong getULong(String columnLabel) throws SQLException {
    return (UnsignedLong) getAndCheckType(columnLabel, UnsignedLong.class);
  }

  public UnsignedLong getULong(int columnIndex) throws SQLException {
    return (UnsignedLong) getAndCheckType(columnIndex, UnsignedLong.class);
  }

  public String getString(String columnLabel) throws SQLException {
    return (String) getAndCheckType(columnLabel, String.class);
  }

  public String getString(int columnIndex) throws SQLException {
    return (String) getAndCheckType(columnIndex, String.class);
  }

  public Long getLong(String columnLabel) throws SQLException {
    return (Long) getAndCheckType(columnLabel, Long.class);
  }

  public Long getLong(int columnIndex) throws SQLException {
    return (Long) getAndCheckType(columnIndex, Long.class);
  }

  public Double getDouble(String columnLabel) throws SQLException {
    return (Double) getAndCheckType(columnLabel, Double.class);
  }

  public Double getDouble(int columnIndex) throws SQLException {
    return (Double) getAndCheckType(columnIndex, Double.class);
  }

  public Float getFloat(String columnLabel) throws SQLException {
    return (Float) getAndCheckType(columnLabel, Float.class);
  }

  public Float getFloat(int columnIndex) throws SQLException {
    return (Float) getAndCheckType(columnIndex, Float.class);
  }

  public DateTime getDateTime(String columnLabel) throws SQLException {
    return (DateTime) getAndCheckType(columnLabel, DateTime.class);
  }

  public DateTime getDateTime(int columnIndex) throws SQLException {
    return (DateTime) getAndCheckType(columnIndex, DateTime.class);
  }

  public byte[] getBytes(String columnLabel) throws SQLException {
    return (byte[]) getAndCheckType(columnLabel, byte[].class);
  }

  public byte[] getBytes(int columnIndex) throws SQLException {
    return (byte[]) getAndCheckType(columnIndex, byte[].class);
  }

  public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
    return (BigDecimal) getAndCheckType(columnLabel, BigDecimal.class);
  }

  public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
    return (BigDecimal) getAndCheckType(columnIndex, BigDecimal.class);
  }

  public Short getShort(String columnLabel) throws SQLException {
    return (Short) getAndCheckType(columnLabel, Short.class);
  }

  public Short getShort(int columnIndex) throws SQLException {
    return (Short) getAndCheckType(columnIndex, Short.class);
  }

  protected abstract Row getCurrentRow() throws SQLException;

  private Object getAndCheckType(String columnLabel, Class<?> cls) throws SQLException {
    return getAndCheckType(findColumn(columnLabel), cls);
  }

  private Object getAndCheckType(int columnIndex, Class<?> cls) throws SQLException {
    Object o = getObject(columnIndex);
    if (o != null && !cls.isInstance(o)) {
      throw new SQLDataException(
          "type mismatch expected:" + cls.getName() + " actual: " + o.getClass().getName());
    }
    return o;
  }

  Object convertFieldValue(Field field, ByteString value) throws SQLException {
    if (value == null || value.size() == 0) {
      return null;
    }

    // Note: We don't actually know the charset in which the value is encoded.
    // For dates and numeric values, we just assume UTF-8 because they (hopefully) don't contain
    // anything outside 7-bit ASCII, which (hopefully) is a subset of the actual charset.
    // For strings, we return byte[] and the application is responsible for using the right charset.
    switch (field.getType()) {
      case TYPE_DECIMAL: // fall through
      case TYPE_NEWDECIMAL:
        return new BigDecimal(value.toStringUtf8());
      case TYPE_TINY: // fall through
      case TYPE_SHORT: // fall through
      case TYPE_INT24:
        return Integer.valueOf(value.toStringUtf8());
      case TYPE_LONG:
        return Long.valueOf(value.toStringUtf8());
      case TYPE_FLOAT:
        return Float.valueOf(value.toStringUtf8());
      case TYPE_DOUBLE:
        return Double.valueOf(value.toStringUtf8());
      case TYPE_NULL:
        return null;
      case TYPE_LONGLONG:
        // This can be an unsigned or a signed long
        if ((field.getFlags() & Field.Flag.VT_UNSIGNED_FLAG_VALUE) != 0) {
          return UnsignedLong.valueOf(value.toStringUtf8());
        } else {
          return Long.valueOf(value.toStringUtf8());
        }
      case TYPE_DATE: // fall through
      case TYPE_NEWDATE:
        return DateTime.parse(value.toStringUtf8(), ISODateTimeFormat.date());
      case TYPE_TIME:
        return DateTime.parse(value.toStringUtf8(), DateTimeFormat.forPattern("HH:mm:ss"));
      case TYPE_DATETIME: // fall through
      case TYPE_TIMESTAMP:
        return DateTime.parse(value.toStringUtf8().replace(' ', 'T'));
      case TYPE_YEAR:
        return Short.valueOf(value.toStringUtf8());
      case TYPE_ENUM: // fall through
      case TYPE_SET:
        return value.toStringUtf8();
      case TYPE_VARCHAR: // fall through
      case TYPE_BIT: // fall through
      case TYPE_TINY_BLOB: // fall through
      case TYPE_MEDIUM_BLOB: // fall through
      case TYPE_LONG_BLOB: // fall through
      case TYPE_BLOB: // fall through
      case TYPE_VAR_STRING: // fall through
      case TYPE_STRING: // fall through
      case TYPE_GEOMETRY:
        return value.toByteArray();
      default:
        throw new SQLDataException("unknown field type: " + field.getType());
    }
  }
}
