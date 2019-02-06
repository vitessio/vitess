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

package io.vitess.jdbc;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;

import io.vitess.client.cursor.Cursor;
import io.vitess.client.cursor.Row;
import io.vitess.client.cursor.SimpleCursor;
import io.vitess.proto.Query;
import io.vitess.util.Constants;
import io.vitess.util.StringUtils;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

import javax.sql.rowset.serial.SerialClob;

/**
 * Created by harshit.gangal on 23/01/16.
 */
public class VitessResultSet implements ResultSet {

  private Cursor cursor;
  private List<FieldWithMetadata> fields;
  private VitessStatement vitessStatement;
  private boolean closed = false;
  private Row row;
  private int currentRow;
  private int maxRows;
  private int fetchSize;
  /**
   * Last column name index read
   */
  private int lastIndexRead = -1;

  public VitessResultSet(Cursor cursor) throws SQLException {
    this(cursor, null);
  }

  public VitessResultSet(Cursor cursor, VitessStatement vitessStatement) throws SQLException {
    if (null == cursor) {
      throw new SQLException(Constants.SQLExceptionMessages.CURSOR_NULL);
    }

    this.cursor = cursor;
    this.vitessStatement = vitessStatement;
    try {
      this.fields = enhancedFieldsFromCursor(
          vitessStatement == null ? null : vitessStatement.getConnection());
    } catch (SQLException e) {
      throw new SQLException(Constants.SQLExceptionMessages.RESULT_SET_INIT_ERROR, e);
    }
    this.currentRow = 0;
    if (null != vitessStatement) {
      this.maxRows = vitessStatement.getMaxRows();
      this.fetchSize = vitessStatement.getFetchSize();
    }
  }

  public VitessResultSet(String[] columnNames, Query.Type[] columnTypes, String[][] data,
      ConnectionProperties connection) throws SQLException {

    if (columnNames.length != columnTypes.length) {
      throw new SQLException(Constants.SQLExceptionMessages.INVALID_RESULT_SET);
    }
    Query.QueryResult.Builder queryResultBuilder = Query.QueryResult.newBuilder();
    for (int columnCounter = 0; columnCounter < columnNames.length; columnCounter++) {
      Query.Field.Builder queryField = Query.Field.newBuilder().setName(columnNames[columnCounter])
          .setType(columnTypes[columnCounter]);
      queryResultBuilder.addFields(queryField.build());
    }
    if (null != data) {
      for (String[] rowData : data) {

        Query.Row.Builder queryRow = Query.Row.newBuilder();
        StringBuilder sb = new StringBuilder();
        for (String aRowData : rowData) {
          sb.append(aRowData);
          queryRow.addLengths(aRowData.length());
        }
        queryRow.setValues(ByteString.copyFromUtf8(sb.toString()));
        queryResultBuilder.addRows(queryRow);
        sb.delete(0, sb.length());
      }
    }
    this.cursor = new SimpleCursor(queryResultBuilder.build());
    this.vitessStatement = null;
    try {
      this.fields = enhancedFieldsFromCursor(connection);
    } catch (SQLException e) {
      throw new SQLException(Constants.SQLExceptionMessages.RESULT_SET_INIT_ERROR, e);
    }
    this.currentRow = 0;
  }

  public VitessResultSet(String[] columnNames, Query.Type[] columnTypes,
      ArrayList<ArrayList<String>> data, VitessConnection connection) throws SQLException {

    if (columnNames.length != columnTypes.length) {
      throw new SQLException(Constants.SQLExceptionMessages.INVALID_RESULT_SET);
    }
    Query.QueryResult.Builder queryResultBuilder = Query.QueryResult.newBuilder();
    for (int columnCounter = 0; columnCounter < columnNames.length; columnCounter++) {
      Query.Field.Builder queryField = Query.Field.newBuilder().setName(columnNames[columnCounter])
          .setType(columnTypes[columnCounter]);
      queryResultBuilder.addFields(queryField.build());
    }
    for (ArrayList<String> rowData : data) {

      Query.Row.Builder queryRow = Query.Row.newBuilder();
      StringBuilder sb = new StringBuilder();
      for (String aRowData : rowData) {
        if (null != aRowData) {
          sb.append(aRowData);
          queryRow.addLengths(aRowData.length());
        } else {
          queryRow.addLengths(-1);
        }
      }
      queryRow.setValues(ByteString.copyFromUtf8(sb.toString()));
      queryResultBuilder.addRows(queryRow);
      sb.delete(0, sb.length());
    }
    this.cursor = new SimpleCursor(queryResultBuilder.build());
    this.vitessStatement = null;
    try {
      this.fields = enhancedFieldsFromCursor(connection);
    } catch (SQLException e) {
      throw new SQLException(Constants.SQLExceptionMessages.RESULT_SET_INIT_ERROR, e);
    }
    this.currentRow = 0;
  }

  private List<FieldWithMetadata> enhancedFieldsFromCursor(ConnectionProperties connection)
      throws SQLException {
    if (cursor == null || cursor.getFields() == null) {
      throw new SQLException(Constants.SQLExceptionMessages.CURSOR_NULL);
    }
    List<Query.Field> rawFields = cursor.getFields();
    List<FieldWithMetadata> fields = new ArrayList<>(rawFields.size());
    for (Query.Field field : rawFields) {
      fields.add(new FieldWithMetadata(connection, field));
    }
    return fields;
  }

  public boolean next() throws SQLException {
    checkOpen();

    if (this.maxRows > 0 && this.currentRow >= this.maxRows) {
      return false;
    }

    this.row = this.cursor.next();
    ++this.currentRow;

    return row != null;
  }

  public void close() throws SQLException {
    if (!this.closed) {
      try {
        this.cursor.close();
      } catch (Exception e) {
        throw new SQLException(Constants.SQLExceptionMessages.VITESS_CURSOR_CLOSE_ERROR);
      } finally {
        //Dereferencing all the objects
        this.closed = true;
        this.cursor = null;
        this.vitessStatement = null;
        this.fields = null;
        this.row = null;
      }
    }
  }

  public boolean wasNull() throws SQLException {
    checkOpen();
    if (this.lastIndexRead == -1) {
      throw new SQLException(Constants.SQLExceptionMessages.NO_COLUMN_ACCESSED);
    }
    return this.row.wasNull();
  }

  public String getString(int columnIndex) throws SQLException {
    Object object;
    String columnValue;

    preAccessor(columnIndex);

    if (isNull(columnIndex)) {
      return null;
    }

    object = this.row.getObject(columnIndex);
    if (object instanceof byte[]) {
      FieldWithMetadata field = this.fields.get(columnIndex - 1);
      if (field.hasConnectionProperties() && field.getConnectionProperties().isIncludeAllFields()) {
        columnValue = convertBytesToString((byte[]) object, field.getEncoding());
      } else {
        columnValue = new String((byte[]) object);
      }
    } else {
      columnValue = String.valueOf(object);
    }
    return columnValue;
  }

  public boolean getBoolean(int columnIndex) throws SQLException {
    String boolString;
    int bool;

    preAccessor(columnIndex);

    if (isNull(columnIndex)) {
      return false;
    }

    // Mysql 5.0 and higher have a BIT Data Type, need to check for this as well.
    FieldWithMetadata field = this.fields.get(columnIndex - 1);

    if (field.getVitessTypeValue() == Query.Type.BIT_VALUE) {
      return byteArrayToBoolean(columnIndex);
    }

    boolString = this.getString(columnIndex);
    try {
      bool = Integer.valueOf(boolString);
    } catch (NumberFormatException nfe) {
      return null != boolString && "true".equalsIgnoreCase(boolString.trim());
    }
    return bool > 0;
  }

  public byte getByte(int columnIndex) throws SQLException {
    String byteString;
    byte value;

    preAccessor(columnIndex);
    if (isNull(columnIndex)) {
      return 0;
    }

    //If the return column type is of byte,
    // return byte otherwise typecast
    Object object = this.row.getObject(columnIndex);
    if (object instanceof Byte) {
      return (byte) object;
    }

    byteString = this.getString(columnIndex);
    try {
      value = Byte.parseByte(byteString);
    } catch (NumberFormatException nfe) {
      throw new SQLException(nfe);
    }

    return value;
  }

  public short getShort(int columnIndex) throws SQLException {
    String shortString;
    short value;

    preAccessor(columnIndex);

    if (isNull(columnIndex)) {
      return 0;
    }

    shortString = this.getString(columnIndex);

    try {
      value = Short.parseShort(shortString);
    } catch (NumberFormatException nfe) {
      throw new SQLException(nfe);
    }

    return value;
  }

  public int getInt(int columnIndex) throws SQLException {
    String intString;
    int value;

    preAccessor(columnIndex);

    if (isNull(columnIndex)) {
      return 0;
    }

    intString = this.getString(columnIndex);

    try {
      value = Integer.parseInt(intString);
    } catch (NumberFormatException nfe) {
      throw new SQLException(nfe);
    }

    return value;
  }

  public long getLong(int columnIndex) throws SQLException {
    String longString;
    long value;

    preAccessor(columnIndex);

    if (isNull(columnIndex)) {
      return 0;
    }

    longString = this.getString(columnIndex);

    try {
      value = Long.parseLong(longString);
    } catch (NumberFormatException nfe) {
      throw new SQLException(nfe);
    }

    return value;
  }

  public float getFloat(int columnIndex) throws SQLException {
    String floatString;
    float value;

    preAccessor(columnIndex);

    if (isNull(columnIndex)) {
      return 0;
    }

    floatString = this.getString(columnIndex);

    try {
      value = Float.parseFloat(floatString);
    } catch (NumberFormatException nfe) {
      throw new SQLException(nfe);
    }

    return value;
  }

  public double getDouble(int columnIndex) throws SQLException {
    String doubleString;
    double value;

    preAccessor(columnIndex);

    if (isNull(columnIndex)) {
      return 0;
    }

    doubleString = this.getString(columnIndex);

    try {
      value = Double.parseDouble(doubleString);
    } catch (NumberFormatException nfe) {
      throw new SQLException(nfe);
    }

    return value;
  }

  public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
    String bigDecimalString;
    BigDecimal value;

    preAccessor(columnIndex);

    if (isNull(columnIndex)) {
      return null;
    }

    bigDecimalString = this.getString(columnIndex);

    try {
      value = new BigDecimal(bigDecimalString);
      value = value.setScale(scale, BigDecimal.ROUND_HALF_UP);
    } catch (Exception ex) {
      throw new SQLException(ex);
    }

    return value;
  }

  public BigInteger getBigInteger(int columnIndex) throws SQLException {
    String bigIntegerString;
    BigInteger value;

    preAccessor(columnIndex);

    if (isNull(columnIndex)) {
      return null;
    }

    bigIntegerString = this.getString(columnIndex);

    try {
      value = new BigInteger(bigIntegerString);
    } catch (Exception ex) {
      throw new SQLException(ex);
    }

    return value;
  }

  public byte[] getBytes(int columnIndex) throws SQLException {
    String bytesString;
    byte[] value;

    preAccessor(columnIndex);
    if (isNull(columnIndex)) {
      return null;
    }
    //If the return column type is of byte[],
    // return byte[] otherwise typecast
    Object object = this.row.getObject(columnIndex);
    if (object instanceof byte[]) {
      return (byte[]) object;
    }

    bytesString = this.getString(columnIndex);
    try {
      value = bytesString.getBytes();
    } catch (Exception ex) {
      throw new SQLException(ex);
    }

    return value;
  }

  public Date getDate(int columnIndex) throws SQLException {
    preAccessor(columnIndex);

    NullDateTime nullDateTime = getNullDateTime(columnIndex);
    return getNullableDate(columnIndex, nullDateTime, null);
  }

  public Time getTime(int columnIndex) throws SQLException {
    preAccessor(columnIndex);

    if (isNull(columnIndex)) {
      return null;
    }

    return this.row.getTime(columnIndex);
  }

  public Timestamp getTimestamp(int columnIndex) throws SQLException {
    preAccessor(columnIndex);

    NullDateTime nullDateTime = getNullDateTime(columnIndex);
    return getNullableDateTime(columnIndex, nullDateTime, null);
  }

  public String getString(String columnLabel) throws SQLException {
    int columnIndex = this.findColumn(columnLabel);
    return getString(columnIndex);
  }

  public boolean getBoolean(String columnLabel) throws SQLException {
    int columnIndex = this.findColumn(columnLabel);
    return getBoolean(columnIndex);
  }

  public byte getByte(String columnLabel) throws SQLException {
    int columnIndex = this.findColumn(columnLabel);
    return getByte(columnIndex);
  }

  public short getShort(String columnLabel) throws SQLException {
    int columnIndex = this.findColumn(columnLabel);
    return getShort(columnIndex);
  }

  public int getInt(String columnLabel) throws SQLException {
    int columnIndex = this.findColumn(columnLabel);
    return getInt(columnIndex);
  }

  public long getLong(String columnLabel) throws SQLException {
    int columnIndex = this.findColumn(columnLabel);
    return getLong(columnIndex);
  }

  public float getFloat(String columnLabel) throws SQLException {
    int columnIndex = this.findColumn(columnLabel);
    return getFloat(columnIndex);
  }

  public double getDouble(String columnLabel) throws SQLException {
    int columnIndex = this.findColumn(columnLabel);
    return getDouble(columnIndex);
  }

  public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
    int columnIndex = this.findColumn(columnLabel);
    return getBigDecimal(columnIndex, scale);
  }

  public BigInteger getBigInteger(String columnLabel) throws SQLException {
    int columnIndex = this.findColumn(columnLabel);
    return getBigInteger(columnIndex);
  }

  public byte[] getBytes(String columnLabel) throws SQLException {
    int columnIndex = this.findColumn(columnLabel);
    return getBytes(columnIndex);
  }

  public Date getDate(String columnLabel) throws SQLException {
    int columnIndex = this.findColumn(columnLabel);
    return getDate(columnIndex);
  }

  public Time getTime(String columnLabel) throws SQLException {
    int columnIndex = this.findColumn(columnLabel);
    return getTime(columnIndex);
  }

  public Timestamp getTimestamp(String columnLabel) throws SQLException {
    int columnIndex = this.findColumn(columnLabel);
    return getTimestamp(columnIndex);
  }

  public SQLWarning getWarnings() throws SQLException {
    checkOpen();
    //no-op, All exceptions thrown, none kept as warning
    return null;
  }

  public void clearWarnings() throws SQLException {
    checkOpen();
    //no-op, All exceptions thrown, none kept as warning
  }


  public ResultSetMetaData getMetaData() throws SQLException {
    return new VitessResultSetMetaData(fields);
  }

  public Object getObject(int columnIndex) throws SQLException {
    preAccessor(columnIndex);

    if (isNull(columnIndex)) {
      return null;
    }

    Object retVal = this.row.getObject(columnIndex);

    FieldWithMetadata field = this.fields.get(columnIndex - 1);
    if (field.hasConnectionProperties() && field.getConnectionProperties().isIncludeAllFields()
        && retVal instanceof byte[]) {
      retVal = convertBytesIfPossible((byte[]) retVal, field);
    }

    return retVal;
  }

  private Object convertBytesIfPossible(byte[] bytes, FieldWithMetadata field) throws SQLException {
    String encoding = field.getEncoding();
    switch (field.getJavaType()) {
      case Types.BIT:
        if (!field.isSingleBit()) {
          return bytes;
        }
        return byteArrayToBoolean(bytes);
      case Types.CHAR:
      case Types.VARCHAR:
      case Types.LONGVARCHAR:
        if (!field.isOpaqueBinary()) {
          return convertBytesToString(bytes, encoding);
        }
        return bytes;
      case Types.BINARY:
      case Types.VARBINARY:
      case Types.LONGVARBINARY:
        return bytes;
      default:
        return convertBytesToString(bytes, encoding);
    }
  }

  private String convertBytesToString(byte[] bytes, String encoding) throws SQLException {
    if (encoding == null) {
      return StringUtils.toString(bytes);
    } else {
      try {
        return StringUtils.toString(bytes, 0, bytes.length, encoding);
      } catch (UnsupportedEncodingException e) {
        throw new SQLException("Unsupported character encoding: " + encoding, e);
      }
    }
  }

  public Object getObject(String columnLabel) throws SQLException {
    int columnIndex = this.findColumn(columnLabel);
    return getObject(columnIndex);
  }

  public int findColumn(String columnLabel) throws SQLException {
    return this.cursor.findColumn(columnLabel);
  }

  public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
    String bigDecimalString;
    BigDecimal value;

    preAccessor(columnIndex);

    if (isNull(columnIndex)) {
      return null;
    }

    bigDecimalString = this.getString(columnIndex);

    try {
      value = new BigDecimal(bigDecimalString);
    } catch (Exception ex) {
      throw new SQLException(ex);
    }

    return value;
  }

  public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
    int columnIndex = this.findColumn(columnLabel);
    return getBigDecimal(columnIndex);
  }

  public boolean isBeforeFirst() throws SQLException {
    checkOpen();
    return this.currentRow == 0;
  }

  public boolean isFirst() throws SQLException {
    checkOpen();
    return currentRow == 1;
  }

  public int getRow() throws SQLException {
    checkOpen();
    return this.currentRow;
  }

  public int getFetchDirection() throws SQLException {
    checkOpen();
    return ResultSet.FETCH_FORWARD;
  }

  public void setFetchDirection(int direction) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public int getFetchSize() throws SQLException {
    checkOpen();
    return this.fetchSize;
  }

  public void setFetchSize(int rows) throws SQLException {
    checkOpen();
    if (rows < 0) {
      throw new SQLException(Constants.SQLExceptionMessages.ILLEGAL_VALUE_FOR + "fetch size");
    }
    this.fetchSize = rows;
  }

  public int getType() throws SQLException {
    checkOpen();
    return ResultSet.TYPE_FORWARD_ONLY;
  }

  public int getConcurrency() throws SQLException {
    checkOpen();
    return ResultSet.CONCUR_READ_ONLY;
  }

  public Statement getStatement() throws SQLException {
    checkOpen();
    return this.vitessStatement;
  }

  public Date getDate(int columnIndex, Calendar cal) throws SQLException {
    preAccessor(columnIndex);

    NullDateTime nullDateTime = getNullDateTime(columnIndex);
    return getNullableDate(columnIndex, nullDateTime, cal);
  }

  public Date getDate(String columnLabel, Calendar cal) throws SQLException {
    int columnIndex = this.findColumn(columnLabel);
    return getDate(columnIndex, cal);
  }

  public Time getTime(int columnIndex, Calendar cal) throws SQLException {
    preAccessor(columnIndex);

    if (isNull(columnIndex)) {
      return null;
    }

    return this.row.getTime(columnIndex, cal);
  }

  public Time getTime(String columnLabel, Calendar cal) throws SQLException {
    int columnIndex = this.findColumn(columnLabel);
    return getTime(columnIndex, cal);
  }

  public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
    preAccessor(columnIndex);

    NullDateTime nullDateTime = getNullDateTime(columnIndex);
    return getNullableDateTime(columnIndex, nullDateTime, cal);
  }

  public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
    int columnIndex = this.findColumn(columnLabel);
    return getTimestamp(columnIndex, cal);
  }

  public boolean isClosed() {
    return this.closed;
  }

  public <T> T unwrap(Class<T> iface) throws SQLException {
    try {
      return iface.cast(this);
    } catch (ClassCastException cce) {
      throw new SQLException(Constants.SQLExceptionMessages.CLASS_CAST_EXCEPTION + iface.toString(),
          cce);
    }
  }

  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    checkOpen();
    return iface.isInstance(this);
  }

  private void checkOpen() throws SQLException {
    if (closed) {
      throw new SQLException(Constants.SQLExceptionMessages.CLOSED_RESULT_SET);
    }
  }

  private void preAccessor(int columnIndex) throws SQLException {
    checkOpen();

    if (columnIndex < 1 || columnIndex > this.fields.size()) {
      throw new SQLException(
          Constants.SQLExceptionMessages.INVALID_COLUMN_INDEX + ": " + columnIndex);
    }

    // set last read column index for wasNull()
    lastIndexRead = columnIndex;
  }

  protected enum NullDateTime {
    NO_CHANGE, NULL, ROUND,
  }

  private NullDateTime getNullDateTime(int columnIndex) throws SQLException {
    if (isNull(columnIndex)) {
      return NullDateTime.NULL;
    }

    if (!hasZeroDateTimePrefix(columnIndex)) {
      return NullDateTime.NO_CHANGE;
    }

    switch (this.vitessStatement.getConnection().getZeroDateTimeBehavior()) {
      case CONVERTTONULL:
        return NullDateTime.NULL;
      case EXCEPTION:
        throw new SQLException(Constants.SQLExceptionMessages.ZERO_TIMESTAMP + ": " + columnIndex);
      case ROUND:
        return NullDateTime.ROUND;
      case GARBLE:
      default:
        return NullDateTime.NO_CHANGE;
    }
  }

  private Date getNullableDate(int columnIndex, NullDateTime nullDateTime, Calendar cal)
      throws SQLException {
    switch (nullDateTime) {
      case NULL:
        return null;
      case ROUND:
        return new Date(-1900, 0, 1);
      case NO_CHANGE:
      default:
        if (cal == null) {
          return this.row.getDate(columnIndex);
        } else {
          return this.row.getDate(columnIndex, cal);
        }
    }
  }

  private Timestamp getNullableDateTime(int columnIndex, NullDateTime nullDateTime, Calendar cal)
      throws SQLException {
    switch (nullDateTime) {
      case NULL:
        return null;
      case ROUND:
        return new Timestamp(-1900, 0, 1, 0, 0, 0, 0);
      case NO_CHANGE:
      default:
        if (cal == null) {
          return this.row.getTimestamp(columnIndex);
        } else {
          return this.row.getTimestamp(columnIndex, cal);
        }
    }
  }

  private boolean hasZeroDateTimePrefix(int columnIndex) throws SQLException {
    return this.row.getRawValue(columnIndex).startsWith(Constants.ZERO_DATE_TIME_PREFIX);
  }

  private boolean isNull(int columnIndex) throws SQLException {
    return null == this.row.getObject(columnIndex);
  }

  //Unsupported Methods

  public InputStream getAsciiStream(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public InputStream getUnicodeStream(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public InputStream getBinaryStream(int columnIndex) throws SQLException {
    preAccessor(columnIndex);
    if (isNull(columnIndex)) {
      return null;
    }
    FieldWithMetadata field = this.fields.get(columnIndex - 1);
    switch (field.getJavaType()) {
      case Types.BIT:
      case Types.BINARY:
      case Types.VARBINARY:
      case Types.BLOB:
      case Types.LONGVARBINARY:
        return this.row.getBinaryInputStream(columnIndex);
    }

    byte[] bytes = getBytes(columnIndex);

    if (bytes != null) {
      return new ByteArrayInputStream(bytes);
    }
    return null;
  }

  public InputStream getAsciiStream(String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public InputStream getUnicodeStream(String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public InputStream getBinaryStream(String columnLabel) throws SQLException {
    int columnIndex = this.findColumn(columnLabel);
    return getBinaryStream(columnIndex);
  }

  public String getCursorName() throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public Reader getCharacterStream(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public Reader getCharacterStream(String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public boolean isAfterLast() throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public boolean isLast() throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void beforeFirst() throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void afterLast() throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public boolean first() throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public boolean last() throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public boolean absolute(int row) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public boolean relative(int rows) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public boolean previous() throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public boolean rowUpdated() throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public boolean rowInserted() throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public boolean rowDeleted() throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateNull(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateBoolean(int columnIndex, boolean x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateByte(int columnIndex, byte x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateShort(int columnIndex, short x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateInt(int columnIndex, int x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateLong(int columnIndex, long x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateFloat(int columnIndex, float x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateDouble(int columnIndex, double x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateString(int columnIndex, String x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateBytes(int columnIndex, byte[] x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateDate(int columnIndex, Date x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateTime(int columnIndex, Time x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateObject(int columnIndex, Object x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateNull(String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateBoolean(String columnLabel, boolean x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateByte(String columnLabel, byte x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateShort(String columnLabel, short x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateInt(String columnLabel, int x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateLong(String columnLabel, long x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateFloat(String columnLabel, float x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateDouble(String columnLabel, double x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateString(String columnLabel, String x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateBytes(String columnLabel, byte[] x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateDate(String columnLabel, Date x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateTime(String columnLabel, Time x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateBinaryStream(String columnLabel, InputStream x, int length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateCharacterStream(String columnLabel, Reader reader, int length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateObject(String columnLabel, Object x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void insertRow() throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateRow() throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void deleteRow() throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void refreshRow() throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void cancelRowUpdates() throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void moveToInsertRow() throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void moveToCurrentRow() throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public Ref getRef(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public Blob getBlob(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public Clob getClob(int columnIndex) throws SQLException {
    preAccessor(columnIndex);

    if (isNull(columnIndex)) {
      return null;
    }

    return new SerialClob(getString(columnIndex).toCharArray());
  }

  public Array getArray(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public Ref getRef(String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public Blob getBlob(String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public Clob getClob(String columnLabel) throws SQLException {
    int columnIndex = this.findColumn(columnLabel);
    return getClob(columnIndex);
  }

  public Array getArray(String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public URL getURL(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public URL getURL(String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateRef(int columnIndex, Ref x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateRef(String columnLabel, Ref x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateBlob(int columnIndex, Blob x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateBlob(String columnLabel, Blob x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateClob(int columnIndex, Clob x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateClob(String columnLabel, Clob x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateArray(int columnIndex, Array x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateArray(String columnLabel, Array x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public RowId getRowId(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public RowId getRowId(String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateRowId(int columnIndex, RowId x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateRowId(String columnLabel, RowId x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public int getHoldability() throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateNString(int columnIndex, String nString) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateNString(String columnLabel, String nString) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public NClob getNClob(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public NClob getNClob(String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public SQLXML getSQLXML(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public SQLXML getSQLXML(String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public String getNString(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public String getNString(String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public Reader getNCharacterStream(int columnIndex) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public Reader getNCharacterStream(String columnLabel) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateNCharacterStream(String columnLabel, Reader reader, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateAsciiStream(String columnLabel, InputStream x, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateBinaryStream(String columnLabel, InputStream x, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateCharacterStream(String columnLabel, Reader reader, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateBlob(int columnIndex, InputStream inputStream, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateBlob(String columnLabel, InputStream inputStream, long length)
      throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateClob(int columnIndex, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateClob(String columnLabel, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateNClob(int columnIndex, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public void updateNClob(String columnLabel, Reader reader) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
    throw new SQLFeatureNotSupportedException(
        Constants.SQLExceptionMessages.SQL_FEATURE_NOT_SUPPORTED);
  }

  @VisibleForTesting
  List<FieldWithMetadata> getFields() {
    return fields;
  }

  private boolean byteArrayToBoolean(int columnIndex) throws SQLException {
    return byteArrayToBoolean(this.row.getObject(columnIndex));
  }

  private boolean byteArrayToBoolean(Object value) {
    if (value == null) {
      return false;
    }

    if (((byte[]) value).length == 0) {
      return false;
    }

    byte boolVal = ((byte[]) value)[0];

    if (boolVal == (byte) '1') {
      return true;
    } else if (boolVal == (byte) '0') {
      return false;
    }

    return (boolVal == -1 || boolVal > 0);
  }

}
