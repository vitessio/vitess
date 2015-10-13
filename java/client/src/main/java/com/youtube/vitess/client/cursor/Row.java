package com.youtube.vitess.client.cursor;

import com.google.common.collect.Lists;
import com.google.common.io.BaseEncoding;
import com.google.common.primitives.UnsignedLong;
import com.google.protobuf.ByteString;
import com.youtube.vitess.proto.Query;
import com.youtube.vitess.proto.Query.Field;
import org.apache.hadoop.io.Writable;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.ISODateTimeFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class Row implements Writable {
  private Map<String, Integer> fieldMap;
  private List<Field> fields;
  private Query.Row rawRow;

  public Row(List<Field> fields, Query.Row rawRow, Map<String, Integer> fieldMap) {
    this.fields = fields;
    this.rawRow = rawRow;
    this.fieldMap = fieldMap;
  }

  public List<Field> getFields() {
    return fields;
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
    if (columnIndex >= rawRow.getValuesCount()) {
      throw new SQLDataException("invalid columnIndex: " + columnIndex);
    }
    return convertFieldValue(fields.get(columnIndex), rawRow.getValues(columnIndex));
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

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(fields.size());
    for (Field field : fields) {
      out.writeUTF(BaseEncoding.base64().encode(field.toByteArray()));
    }
    out.writeUTF(BaseEncoding.base64().encode(rawRow.toByteArray()));
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int fieldLen = in.readInt();
    fields = Lists.newArrayListWithCapacity(fieldLen);
    for (int i=0; i < fieldLen; i++) {
      fields.add(Field.parseFrom(BaseEncoding.base64().decode(in.readUTF())));
    }
    rawRow = Query.Row.parseFrom(BaseEncoding.base64().decode(in.readUTF()));
  }
}
