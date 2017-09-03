/*
 * Copyright 2017 Google Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.vitess.client.cursor;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.UnsignedLong;
import com.google.protobuf.ByteString;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import javax.annotation.concurrent.NotThreadSafe;

import io.vitess.mysql.DateTime;
import io.vitess.proto.Query;
import io.vitess.proto.Query.Field;
import io.vitess.proto.Query.Type;

/**
 * Type-converting wrapper around raw {@link io.vitess.proto.Query.Row} proto.
 *
 * <p>
 * Usually you get Row objects from a {@link Cursor}, which builds them by combining
 * {@link io.vitess.proto.Query.Row} with the list of {@link Field}s from the corresponding
 * {@link io.vitess.proto.Query.QueryResult}.
 *
 * <p>
 * Methods on {@code Row} are intended to be compatible with those on {@link java.sql.ResultSet}
 * where possible. This means {@code columnIndex} values start at 1 for the first column, and
 * {@code columnLabel} values are case-insensitive. If multiple columns have the same
 * case-insensitive {@code columnLabel}, the earliest one will be returned.
 */
@NotThreadSafe
public class Row {
  private final FieldMap fieldMap;
  private final List<ByteString> values;
  private final Query.Row rawRow;
  /**
   * Remembers whether the column referenced by the last {@code get*()} was MySQL {@code NULL}.
   *
   * <p>
   * Although {@link Row} is not supposed to be used by multiple threads, we defensively declare
   * {@code lastGetWasNull} as {@code volatile} since {@link Cursor} in general is used in
   * asynchronous callbacks.
   */
  private volatile boolean lastGetWasNull;

  /**
   * Construct a Row from {@link io.vitess.proto.Query.Row} proto with a pre-built
   * {@link FieldMap}.
   *
   * <p>
   * {@link Cursor} uses this to share a {@link FieldMap} among multiple rows.
   */
  public Row(FieldMap fieldMap, Query.Row rawRow) {
    this.fieldMap = fieldMap;
    this.rawRow = rawRow;
    this.values = extractValues(rawRow.getLengthsList(), rawRow.getValues());
  }

  /**
   * Construct a Row from {@link io.vitess.proto.Query.Row} proto.
   */
  public Row(List<Field> fields, Query.Row rawRow) {
    this.fieldMap = new FieldMap(fields);
    this.rawRow = rawRow;
    this.values = extractValues(rawRow.getLengthsList(), rawRow.getValues());
  }

  /**
   * Construct a Row manually (not from proto).
   *
   * <p>
   * The primary purpose of this Row class is to wrap the {@link io.vitess.proto.Query.Row}
   * proto, which stores values in a packed format. However, when writing tests you may want to
   * create a Row from unpacked data.
   *
   * <p>
   * Note that {@link #getRowProto()} will return null in this case, so a Row created in this way
   * can't be used with code that requires access to the raw row proto.
   */
  @VisibleForTesting
  public Row(List<Field> fields, List<ByteString> values) {
    this.fieldMap = new FieldMap(fields);
    this.rawRow = null;
    this.values = values;
  }

  /**
   * Returns the number of columns.
   */
  public int size() {
    return values.size();
  }

  public List<Field> getFields() {
    return fieldMap.getList();
  }

  public Query.Row getRowProto() {
    return rawRow;
  }

  public FieldMap getFieldMap() {
    return fieldMap;
  }

  /**
   * Returns 1-based column number.
   *
   * @param columnLabel case-insensitive column label
   */
  public int findColumn(String columnLabel) throws SQLException {
    Integer columnIndex = fieldMap.getIndex(columnLabel);
    if (columnIndex == null) {
      throw new SQLDataException("column not found:" + columnLabel);
    }
    return columnIndex;
  }

  /**
   * @param columnLabel case-insensitive column label
   */
  public Object getObject(String columnLabel) throws SQLException {
    return getObject(findColumn(columnLabel));
  }

  /**
   * @param columnIndex 1-based column number (0 is invalid)
   */
  public Object getObject(int columnIndex) throws SQLException {
    ByteString rawValue = getRawValue(columnIndex);
    if (rawValue == null) {
      // Only a MySQL NULL value should return null.
      // A zero-length value should return the appropriate type.
      return null;
    }
    return convertFieldValue(fieldMap.get(columnIndex), rawValue);
  }

  /**
   * Returns the raw {@link ByteString} for a column.
   *
   * @param columnLabel case-insensitive column label
   */
  public ByteString getRawValue(String columnLabel) throws SQLException {
    return getRawValue(findColumn(columnLabel));
  }

  /**
   * Returns the raw {@link ByteString} for a column.
   *
   * @param columnIndex 1-based column number (0 is invalid)
   */
  public ByteString getRawValue(int columnIndex) throws SQLException {
    checkArgument(columnIndex >= 1, "columnIndex out of range: %s", columnIndex);
    if (columnIndex > values.size()) {
      throw new SQLDataException("invalid columnIndex: " + columnIndex);
    }
    ByteString value = values.get(columnIndex - 1);
    lastGetWasNull = (value == null);
    return value;
  }

  /**
   * Returns the data at a given index as an InputStream.
   *
   * @param columnLabel case-insensitive column label
   */
  public InputStream getBinaryInputStream(String columnLabel) throws SQLException {
    return getBinaryInputStream(findColumn(columnLabel));
  }

  /**
   * Returns the data at a given index as an InputStream.
   *
   * @param columnIndex 1-based column number (0 is invalid)
   */
  public InputStream getBinaryInputStream(int columnIndex) throws SQLException {
    ByteString rawValue = getRawValue(columnIndex);
    if (rawValue == null) {
      return null;
    }
    return rawValue.newInput();
  }

  /**
   * Returns the column value, or 0 if the value is SQL NULL.
   *
   * <p>
   * To distinguish between 0 and SQL NULL, use either {@link #wasNull()} or
   * {@link #getObject(String,Class)}.
   *
   * @param columnLabel case-insensitive column label
   */
  public int getInt(String columnLabel) throws SQLException {
    return getInt(findColumn(columnLabel));
  }

  /**
   * Returns the column value, or 0 if the value is SQL NULL.
   *
   * <p>
   * To distinguish between 0 and SQL NULL, use either {@link #wasNull()} or
   * {@link #getObject(int,Class)}.
   *
   * @param columnIndex 1-based column number (0 is invalid)
   */
  public int getInt(int columnIndex) throws SQLException {
    Integer value = getObject(columnIndex, Integer.class);
    return value == null ? 0 : value;
  }

  /**
   * @param columnLabel case-insensitive column label
   */
  public UnsignedLong getULong(String columnLabel) throws SQLException {
    return getULong(findColumn(columnLabel));
  }

  /**
   * @param columnIndex 1-based column number (0 is invalid)
   */
  public UnsignedLong getULong(int columnIndex) throws SQLException {
    BigInteger l = getObject(columnIndex, BigInteger.class);
    return l == null ? null : UnsignedLong.fromLongBits(l.longValue());
  }

  /**
   * @param columnLabel case-insensitive column label
   */
  public String getString(String columnLabel) throws SQLException {
    return getString(findColumn(columnLabel));
  }

  /**
   * @param columnIndex 1-based column number (0 is invalid)
   */
  public String getString(int columnIndex) throws SQLException {
    return getObject(columnIndex, String.class);
  }

  /**
   * Returns the column value, or 0 if the value is SQL NULL.
   *
   * <p>
   * To distinguish between 0 and SQL NULL, use either {@link #wasNull()} or
   * {@link #getObject(String,Class)}.
   *
   * @param columnLabel case-insensitive column label
   */
  public long getLong(String columnLabel) throws SQLException {
    return getLong(findColumn(columnLabel));
  }

  /**
   * Returns the column value, or 0 if the value is SQL NULL.
   *
   * <p>
   * To distinguish between 0 and SQL NULL, use either {@link #wasNull()} or
   * {@link #getObject(int,Class)}.
   *
   * @param columnIndex 1-based column number (0 is invalid)
   */
  public long getLong(int columnIndex) throws SQLException {
    Long value = getObject(columnIndex, Long.class);
    return value == null ? 0 : value;
  }

  /**
   * Returns the column value, or 0 if the value is SQL NULL.
   *
   * <p>
   * To distinguish between 0 and SQL NULL, use either {@link #wasNull()} or
   * {@link #getObject(String,Class)}.
   *
   * @param columnLabel case-insensitive column label
   */
  public double getDouble(String columnLabel) throws SQLException {
    return getDouble(findColumn(columnLabel));
  }

  /**
   * Returns the column value, or 0 if the value is SQL NULL.
   *
   * <p>
   * To distinguish between 0 and SQL NULL, use either {@link #wasNull()} or
   * {@link #getObject(int,Class)}.
   *
   * @param columnIndex 1-based column number (0 is invalid)
   */
  public double getDouble(int columnIndex) throws SQLException {
    Double value = getObject(columnIndex, Double.class);
    return value == null ? 0 : value;
  }

  /**
   * Returns the column value, or 0 if the value is SQL NULL.
   *
   * <p>
   * To distinguish between 0 and SQL NULL, use either {@link #wasNull()} or
   * {@link #getObject(String,Class)}.
   *
   * @param columnLabel case-insensitive column label
   */
  public float getFloat(String columnLabel) throws SQLException {
    return getFloat(findColumn(columnLabel));
  }

  /**
   * Returns the column value, or 0 if the value is SQL NULL.
   *
   * <p>
   * To distinguish between 0 and SQL NULL, use either {@link #wasNull()} or
   * {@link #getObject(int,Class)}.
   *
   * @param columnIndex 1-based column number (0 is invalid)
   */
  public float getFloat(int columnIndex) throws SQLException {
    Float value = getObject(columnIndex, Float.class);
    return value == null ? 0 : value;
  }

  /**
   * Returns the column value as a {@link Date} with the default time zone.
   *
   * @param columnLabel case-insensitive column label
   */
  public Date getDate(String columnLabel) throws SQLException {
    return getDate(findColumn(columnLabel), Calendar.getInstance());
  }

  /**
   * Returns the column value as a {@link Date} with the given {@link Calendar}.
   *
   * @param columnLabel case-insensitive column label
   */
  public Date getDate(String columnLabel, Calendar cal) throws SQLException {
    return getDate(findColumn(columnLabel), cal);
  }

  /**
   * Returns the column value as a {@link Date} with the default time zone.
   *
   * @param columnIndex 1-based column number (0 is invalid)
   */
  public Date getDate(int columnIndex) throws SQLException {
    return getDate(columnIndex, Calendar.getInstance());
  }

  /**
   * Returns the column value as a {@link Date} with the given {@link Calendar}.
   *
   * @param columnIndex 1-based column number (0 is invalid)
   */
  public Date getDate(int columnIndex, Calendar cal) throws SQLException {
    ByteString rawValue = getRawValue(columnIndex);
    if (rawValue == null) {
      return null;
    }
    Field field = fieldMap.get(columnIndex);
    if (field.getType() != Type.DATE) {
      throw new SQLDataException(
          "type mismatch, expected: " + Type.DATE + ", actual: " + field.getType());
    }
    try {
      return DateTime.parseDate(rawValue.toStringUtf8(), cal);
    } catch (ParseException e) {
      throw new SQLDataException("Can't parse DATE: " + rawValue.toStringUtf8(), e);
    }
  }

  /**
   * Returns the column value as {@link Time} with the default time zone.
   *
   * @param columnLabel case-insensitive column label
   */
  public Time getTime(String columnLabel) throws SQLException {
    return getTime(findColumn(columnLabel), Calendar.getInstance());
  }

  /**
   * Returns the column value as {@link Time} with the given {@link Calendar}.
   *
   * @param columnLabel case-insensitive column label
   */
  public Time getTime(String columnLabel, Calendar cal) throws SQLException {
    return getTime(findColumn(columnLabel), cal);
  }

  /**
   * Returns the column value as {@link Time} with the default time zone.
   *
   * @param columnIndex 1-based column number (0 is invalid)
   */
  public Time getTime(int columnIndex) throws SQLException {
    return getTime(columnIndex, Calendar.getInstance());
  }

  /**
   * Returns the column value as {@link Time} with the given {@link Calendar}.
   *
   * @param columnIndex 1-based column number (0 is invalid)
   */
  public Time getTime(int columnIndex, Calendar cal) throws SQLException {
    ByteString rawValue = getRawValue(columnIndex);
    if (rawValue == null) {
      return null;
    }
    Field field = fieldMap.get(columnIndex);
    if (field.getType() != Type.TIME) {
      throw new SQLDataException(
          "type mismatch, expected: " + Type.TIME + ", actual: " + field.getType());
    }
    try {
      return DateTime.parseTime(rawValue.toStringUtf8(), cal);
    } catch (ParseException e) {
      throw new SQLDataException("Can't parse TIME: " + rawValue.toStringUtf8(), e);
    }
  }

  /**
   * Returns the column value as {@link Timestamp} with the default time zone.
   *
   * @param columnLabel case-insensitive column label
   */
  public Timestamp getTimestamp(String columnLabel) throws SQLException {
    return getTimestamp(findColumn(columnLabel), Calendar.getInstance());
  }

  /**
   * Returns the column value as {@link Timestamp} with the given {@link Calendar}.
   *
   * @param columnLabel case-insensitive column label
   */
  public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
    return getTimestamp(findColumn(columnLabel), cal);
  }

  /**
   * Returns the column value as {@link Timestamp} with the default time zone.
   *
   * @param columnIndex 1-based column number (0 is invalid)
   */
  public Timestamp getTimestamp(int columnIndex) throws SQLException {
    return getTimestamp(columnIndex, Calendar.getInstance());
  }

  /**
   * Returns the column value as {@link Timestamp} with the given {@link Calendar}.
   *
   * @param columnIndex 1-based column number (0 is invalid)
   */
  public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
    ByteString rawValue = getRawValue(columnIndex);
    if (rawValue == null) {
      return null;
    }
    Field field = fieldMap.get(columnIndex);
    if (field.getType() != Type.TIMESTAMP && field.getType() != Type.DATETIME) {
      throw new SQLDataException("type mismatch, expected: " + Type.TIMESTAMP + " or "
          + Type.DATETIME + ", actual: " + field.getType());
    }
    try {
      return DateTime.parseTimestamp(rawValue.toStringUtf8(), cal);
    } catch (ParseException e) {
      throw new SQLDataException("Can't parse TIMESTAMP: " + rawValue.toStringUtf8(), e);
    }
  }

  /**
   * @param columnLabel case-insensitive column label
   */
  public byte[] getBytes(String columnLabel) throws SQLException {
    return getBytes(findColumn(columnLabel));
  }

  /**
   * @param columnIndex 1-based column number (0 is invalid)
   */
  public byte[] getBytes(int columnIndex) throws SQLException {
    return getObject(columnIndex, byte[].class);
  }

  /**
   * @param columnLabel case-insensitive column label
   */
  public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
    return getBigDecimal(findColumn(columnLabel));
  }

  /**
   * @param columnIndex 1-based column number (0 is invalid)
   */
  public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
    return getObject(columnIndex, BigDecimal.class);
  }

  /**
   * Returns the column value, or 0 if the value is SQL NULL.
   *
   * <p>
   * To distinguish between 0 and SQL NULL, use either {@link #wasNull()} or
   * {@link #getObject(String,Class)}.
   *
   * @param columnLabel case-insensitive column label
   */
  public short getShort(String columnLabel) throws SQLException {
    return getShort(findColumn(columnLabel));
  }

  /**
   * Returns the column value, or 0 if the value is SQL NULL.
   *
   * <p>
   * To distinguish between 0 and SQL NULL, use either {@link #wasNull()} or
   * {@link #getObject(int,Class)}.
   *
   * @param columnIndex 1-based column number (0 is invalid)
   */
  public short getShort(int columnIndex) throws SQLException {
    Short value = getObject(columnIndex, Short.class);
    return value == null ? 0 : value;
  }

  /**
   * Returns the column value, cast to the specified type.
   *
   * <p>
   * This can be used as an alternative to getters that return primitive types, if you need to
   * distinguish between 0 and SQL NULL. For example:
   *
   * <blockquote>
   *
   * <pre>
   * Long value = row.getObject(0, Long.class);
   * if (value == null) {
   *   // The value was SQL NULL, not 0.
   * }
   * </pre>
   *
   * </blockquote>
   *
   * @param columnIndex 1-based column number (0 is invalid)
   * @throws SQLDataException if the type doesn't match the actual value.
   */
  @SuppressWarnings("unchecked") // by runtime check
  public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
    Object o = getObject(columnIndex);
    if (o != null && !type.isInstance(o)) {
      throw new SQLDataException(
          "type mismatch, expected: " + type.getName() + ", actual: " + o.getClass().getName());
    }
    return (T) o;
  }

  /**
   * Returns the column value, cast to the specified type.
   *
   * <p>
   * This can be used as an alternative to getters that return primitive types, if you need to
   * distinguish between 0 and SQL NULL. For example:
   *
   * <blockquote>
   *
   * <pre>
   * Long value = row.getObject("col0", Long.class);
   * if (value == null) {
   *   // The value was SQL NULL, not 0.
   * }
   * </pre>
   *
   * </blockquote>
   *
   * @param columnLabel case-insensitive column label
   * @throws SQLDataException if the type doesn't match the actual value.
   */
  public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
    return getObject(findColumn(columnLabel), type);
  }

  /**
   * Reports whether the last column read had a value of SQL NULL.
   *
   * <p>
   * Getter methods that return primitive types, such as {@link #getLong(int)}, will return 0 if the
   * value is SQL NULL. To distinguish 0 from SQL NULL, you can call {@code wasNull()} immediately
   * after retrieving the value.
   *
   * <p>
   * Note that this is not thread-safe: the value of {@code wasNull()} is only trustworthy if there
   * are no concurrent calls on this {@code Row} between the call to {@code get*()} and the call to
   * {@code wasNull()}.
   *
   * <p>
   * As an alternative to {@code wasNull()}, you can use {@link #getObject(int,Class)} (e.g.
   * {@code getObject(0, Long.class)} instead of {@code getLong(0)}) to get a wrapped {@code Long}
   * value that will be {@code null} if the column value was SQL NULL.
   *
   * @throws SQLException
   */
  public boolean wasNull() throws SQLException {
    // Note: lastGetWasNull is currently set only in getRawValue(),
    // which means this relies on the fact that all other get*() methods
    // eventually call into that. The unit tests help to ensure this by
    // checking wasNull() after each get*().
    return lastGetWasNull;
  }

  private static Object convertFieldValue(Field field, ByteString value) throws SQLException {
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
        return new BigInteger(value.toStringUtf8());
      case FLOAT32:
        return Float.valueOf(value.toStringUtf8());
      case FLOAT64:
        return Double.valueOf(value.toStringUtf8());
      case NULL_TYPE:
        return null;
      case DATE:
        // We don't get time zone information from the server,
        // so we use the default time zone.
        try {
          return DateTime.parseDate(value.toStringUtf8());
        } catch (ParseException e) {
          throw new SQLDataException("Can't parse DATE: " + value.toStringUtf8(), e);
        }
      case TIME:
        // We don't get time zone information from the server,
        // so we use the default time zone.
        try {
          return DateTime.parseTime(value.toStringUtf8());
        } catch (ParseException e) {
          throw new SQLDataException("Can't parse TIME: " + value.toStringUtf8(), e);
        }
      case DATETIME: // fall through
      case TIMESTAMP:
        // We don't get time zone information from the server,
        // so we use the default time zone.
        try {
          return DateTime.parseTimestamp(value.toStringUtf8());
        } catch (ParseException e) {
          throw new SQLDataException("Can't parse TIMESTAMP: " + value.toStringUtf8(), e);
        }
      case YEAR:
        return Short.valueOf(value.toStringUtf8());
      case ENUM: // fall through
      case SET:
        return value.toStringUtf8();
      case BIT: // fall through
      case TEXT: // fall through
      case BLOB: // fall through
      case VARCHAR: // fall through
      case VARBINARY: // fall through
      case CHAR: // fall through
      case BINARY:
      case GEOMETRY:
      case JSON:
        return value.toByteArray();
      default:
        throw new SQLDataException("unknown field type: " + field.getType());
    }
  }

  /**
   * Extract cell values from the single-buffer wire format.
   *
   * <p>
   * See the docs for the {@code Row} message in {@code query.proto}.
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
