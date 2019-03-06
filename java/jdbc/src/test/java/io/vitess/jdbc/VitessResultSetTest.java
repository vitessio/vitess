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

import static org.junit.Assert.assertEquals;

import com.google.protobuf.ByteString;

import io.vitess.client.cursor.Cursor;
import io.vitess.client.cursor.SimpleCursor;
import io.vitess.proto.Query;
import io.vitess.util.MysqlDefs;
import io.vitess.util.StringUtils;
import io.vitess.util.charset.CharsetMapping;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Properties;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.internal.verification.VerificationModeFactory;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Created by harshit.gangal on 19/01/16.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(VitessResultSet.class)
public class VitessResultSetTest extends BaseTest {

  public Cursor getCursorWithRows() {
        /*
        INT8(1, 257), -50
        UINT8(2, 770), 50
        INT16(3, 259), -23000
        UINT16(4, 772), 23000
        INT24(5, 261), -100
        UINT24(6, 774), 100
        INT32(7, 263), -100
        UINT32(8, 776), 100
        INT64(9, 265),  -1000
        UINT64(10, 778), 1000
        FLOAT32(11, 1035), 24.53
        FLOAT64(12, 1036), 100.43
        TIMESTAMP(13, 2061), 2016-02-06 14:15:16
        DATE(14, 2062), 2016-02-06
        TIME(15, 2063), 12:34:56
        DATETIME(16, 2064), 2016-02-06 14:15:16
        YEAR(17, 785),  2016
        DECIMAL(18, 18), 1234.56789
        TEXT(19, 6163), HELLO TDS TEAM
        BLOB(20, 10260),  HELLO TDS TEAM
        VARCHAR(21, 6165), HELLO TDS TEAM
        VARBINARY(22, 10262), HELLO TDS TEAM
        CHAR(23, 6167), N
        BINARY(24, 10264), HELLO TDS TEAM
        BIT(25, 2073), 1
        ENUM(26, 2074), val123
        SET(27, 2075), val123
        TUPLE(28, 28),
        UNRECOGNIZED(-1, -1);
        */
    return new SimpleCursor(
        Query.QueryResult.newBuilder().addFields(getField("col1", Query.Type.INT8))
            .addFields(getField("col2", Query.Type.UINT8))
            .addFields(getField("col3", Query.Type.INT16))
            .addFields(getField("col4", Query.Type.UINT16))
            .addFields(getField("col5", Query.Type.INT24))
            .addFields(getField("col6", Query.Type.UINT24))
            .addFields(getField("col7", Query.Type.INT32))
            .addFields(getField("col8", Query.Type.UINT32))
            .addFields(getField("col9", Query.Type.INT64))
            .addFields(getField("col10", Query.Type.UINT64))
            .addFields(getField("col11", Query.Type.FLOAT32))
            .addFields(getField("col12", Query.Type.FLOAT64))
            .addFields(getField("col13", Query.Type.TIMESTAMP))
            .addFields(getField("col14", Query.Type.DATE))
            .addFields(getField("col15", Query.Type.TIME))
            .addFields(getField("col16", Query.Type.DATETIME))
            .addFields(getField("col17", Query.Type.YEAR))
            .addFields(getField("col18", Query.Type.DECIMAL))
            .addFields(getField("col19", Query.Type.TEXT))
            .addFields(getField("col20", Query.Type.BLOB))
            .addFields(getField("col21", Query.Type.VARCHAR))
            .addFields(getField("col22", Query.Type.VARBINARY))
            .addFields(getField("col23", Query.Type.CHAR))
            .addFields(getField("col24", Query.Type.BINARY))
            .addFields(getField("col25", Query.Type.BIT))
            .addFields(getField("col26", Query.Type.ENUM))
            .addFields(getField("col27", Query.Type.SET))
            .addFields(getField("col28", Query.Type.TIMESTAMP)).addRows(
            Query.Row.newBuilder().addLengths("-50".length()).addLengths("50".length())
                .addLengths("-23000".length()).addLengths("23000".length())
                .addLengths("-100".length()).addLengths("100".length()).addLengths("-100".length())
                .addLengths("100".length()).addLengths("-1000".length()).addLengths("1000".length())
                .addLengths("24.52".length()).addLengths("100.43".length())
                .addLengths("2016-02-06 14:15:16".length()).addLengths("2016-02-06".length())
                .addLengths("12:34:56".length()).addLengths("2016-02-06 14:15:16".length())
                .addLengths("2016".length()).addLengths("1234.56789".length())
                .addLengths("HELLO TDS TEAM".length()).addLengths("HELLO TDS TEAM".length())
                .addLengths("HELLO TDS TEAM".length()).addLengths("HELLO TDS TEAM".length())
                .addLengths("N".length()).addLengths("HELLO TDS TEAM".length())
                .addLengths("1".length()).addLengths("val123".length())
                .addLengths("val123".length()).addLengths("0000-00-00 00:00:00".length()).setValues(
                ByteString.copyFromUtf8(
                    "-5050-2300023000-100100-100100-1000100024.52100.432016-02-06 "
                        + "14:15:162016-02-0612:34:562016-02-06 14:15:1620161234.56789HELLO TDS "
                        + "TEAMHELLO TDS TEAMHELLO"
                        + " TDS TEAMHELLO TDS TEAMNHELLO TDS TEAM1val123val1230000-00-00 "
                        + "00:00:00")))
            .build());
  }

  private Query.Field getField(String fieldName, Query.Type typ) {
    return Query.Field.newBuilder().setName(fieldName).setType(typ).build();
  }

  private Query.Field getField(String fieldName) {
    return Query.Field.newBuilder().setName(fieldName).build();
  }

  public Cursor getCursorWithRowsAsNull() {
        /*
        INT8(1, 257), -50
        UINT8(2, 770), 50
        INT16(3, 259), -23000
        UINT16(4, 772), 23000
        INT24(5, 261), -100
        UINT24(6, 774), 100
        INT32(7, 263), -100
        UINT32(8, 776), 100
        INT64(9, 265),  -1000
        UINT64(10, 778), 1000
        FLOAT32(11, 1035), 24.53
        FLOAT64(12, 1036), 100.43
        TIMESTAMP(13, 2061), 2016-02-06 14:15:16
        DATE(14, 2062), 2016-02-06
        TIME(15, 2063), 12:34:56
        DATETIME(16, 2064), 2016-02-06 14:15:16
        YEAR(17, 785),  2016
        DECIMAL(18, 18), 1234.56789
        TEXT(19, 6163), HELLO TDS TEAM
        BLOB(20, 10260),  HELLO TDS TEAM
        VARCHAR(21, 6165), HELLO TDS TEAM
        VARBINARY(22, 10262), HELLO TDS TEAM
        CHAR(23, 6167), N
        BINARY(24, 10264), HELLO TDS TEAM
        BIT(25, 2073), 0
        ENUM(26, 2074), val123
        SET(27, 2075), val123
        TUPLE(28, 28),
        UNRECOGNIZED(-1, -1);
        */
    return new SimpleCursor(
        Query.QueryResult.newBuilder().addFields(getField("col1", Query.Type.INT8))
            .addFields(getField("col2", Query.Type.UINT8))
            .addFields(getField("col3", Query.Type.INT16))
            .addFields(getField("col4", Query.Type.UINT16))
            .addFields(getField("col5", Query.Type.INT24))
            .addFields(getField("col6", Query.Type.UINT24))
            .addFields(getField("col7", Query.Type.INT32))
            .addFields(getField("col8", Query.Type.UINT32))
            .addFields(getField("col9", Query.Type.INT64))
            .addFields(getField("col10", Query.Type.UINT64))
            .addFields(getField("col11", Query.Type.FLOAT32))
            .addFields(getField("col12", Query.Type.FLOAT64))
            .addFields(getField("col13", Query.Type.TIMESTAMP))
            .addFields(getField("col14", Query.Type.DATE))
            .addFields(getField("col15", Query.Type.TIME))
            .addFields(getField("col16", Query.Type.DATETIME))
            .addFields(getField("col17", Query.Type.YEAR))
            .addFields(getField("col18", Query.Type.DECIMAL))
            .addFields(getField("col19", Query.Type.TEXT))
            .addFields(getField("col20", Query.Type.BLOB))
            .addFields(getField("col21", Query.Type.VARCHAR))
            .addFields(getField("col22", Query.Type.VARBINARY))
            .addFields(getField("col23", Query.Type.CHAR))
            .addFields(getField("col24", Query.Type.BINARY))
            .addFields(getField("col25", Query.Type.BIT))
            .addFields(getField("col26", Query.Type.ENUM))
            .addFields(getField("col27", Query.Type.SET)).addRows(
            Query.Row.newBuilder().addLengths("-50".length()).addLengths("50".length())
                .addLengths("-23000".length()).addLengths("23000".length())
                .addLengths("-100".length()).addLengths("100".length()).addLengths("-100".length())
                .addLengths("100".length()).addLengths("-1000".length()).addLengths("1000".length())
                .addLengths("24.52".length()).addLengths("100.43".length())
                .addLengths("2016-02-06 14:15:16".length()).addLengths("2016-02-06".length())
                .addLengths("12:34:56".length()).addLengths("2016-02-06 14:15:16".length())
                .addLengths("2016".length()).addLengths("1234.56789".length())
                .addLengths("HELLO TDS TEAM".length()).addLengths("HELLO TDS TEAM".length())
                .addLengths("HELLO TDS TEAM".length()).addLengths("HELLO TDS TEAM".length())
                .addLengths("N".length()).addLengths("HELLO TDS TEAM".length())
                .addLengths("0".length()).addLengths("val123".length()).addLengths(-1).setValues(
                ByteString.copyFromUtf8(
                    "-5050-2300023000-100100-100100-1000100024.52100.432016-02-06 "
                        + "14:15:162016-02-0612:34:562016-02-06 14:15:1620161234.56789HELLO TDS "
                        + "TEAMHELLO TDS "
                        + "TEAMHELLO TDS TEAMHELLO TDS TEAMNHELLO TDS TEAM0val123"))).build());
  }

  @Test
  public void testNextWithZeroRows() throws Exception {
    Cursor cursor = new SimpleCursor(
        Query.QueryResult.newBuilder().addFields(getField("col0")).addFields(getField("col1"))
            .addFields(getField("col2")).build());

    VitessResultSet vitessResultSet = new VitessResultSet(cursor);
    assertEquals(false, vitessResultSet.next());
  }

  @Test
  public void testNextWithNonZeroRows() throws Exception {
    Cursor cursor = getCursorWithRows();
    VitessResultSet vitessResultSet = new VitessResultSet(cursor);
    assertEquals(true, vitessResultSet.next());
    assertEquals(false, vitessResultSet.next());
  }

  @Test
  public void testgetString() throws SQLException {
    Cursor cursor = getCursorWithRowsAsNull();
    VitessResultSet vitessResultSet = new VitessResultSet(cursor, getVitessStatement());
    vitessResultSet.next();
    assertEquals("-50", vitessResultSet.getString(1));
    assertEquals("50", vitessResultSet.getString(2));
    assertEquals("-23000", vitessResultSet.getString(3));
    assertEquals("23000", vitessResultSet.getString(4));
    assertEquals("-100", vitessResultSet.getString(5));
    assertEquals("100", vitessResultSet.getString(6));
    assertEquals("-100", vitessResultSet.getString(7));
    assertEquals("100", vitessResultSet.getString(8));
    assertEquals("-1000", vitessResultSet.getString(9));
    assertEquals("1000", vitessResultSet.getString(10));
    assertEquals("24.52", vitessResultSet.getString(11));
    assertEquals("100.43", vitessResultSet.getString(12));
    assertEquals("2016-02-06 14:15:16.0", vitessResultSet.getString(13));
    assertEquals("2016-02-06", vitessResultSet.getString(14));
    assertEquals("12:34:56", vitessResultSet.getString(15));
    assertEquals("2016-02-06 14:15:16.0", vitessResultSet.getString(16));
    assertEquals("2016", vitessResultSet.getString(17));
    assertEquals("1234.56789", vitessResultSet.getString(18));
    assertEquals("HELLO TDS TEAM", vitessResultSet.getString(19));
    assertEquals("HELLO TDS TEAM", vitessResultSet.getString(20));
    assertEquals("HELLO TDS TEAM", vitessResultSet.getString(21));
    assertEquals("HELLO TDS TEAM", vitessResultSet.getString(22));
    assertEquals("N", vitessResultSet.getString(23));
    assertEquals("HELLO TDS TEAM", vitessResultSet.getString(24));
    assertEquals("0", vitessResultSet.getString(25));
    assertEquals("val123", vitessResultSet.getString(26));
    assertEquals(null, vitessResultSet.getString(27));
  }

  @Test
  public void getObjectUint64AsBigInteger() throws SQLException {
    Cursor cursor = getCursorWithRowsAsNull();
    VitessResultSet vitessResultSet = new VitessResultSet(cursor, getVitessStatement());
    vitessResultSet.next();

    assertEquals(new BigInteger("1000"), vitessResultSet.getObject(10));
  }

  @Test
  public void getBigInteger() throws SQLException {
    Cursor cursor = getCursorWithRowsAsNull();
    VitessResultSet vitessResultSet = new VitessResultSet(cursor, getVitessStatement());
    vitessResultSet.next();

    assertEquals(new BigInteger("1000"), vitessResultSet.getBigInteger(10));
  }

  @Test
  public void testgetBoolean() throws SQLException {
    Cursor cursor = getCursorWithRows();
    Cursor cursorWithRowsAsNull = getCursorWithRowsAsNull();
    VitessResultSet vitessResultSet = new VitessResultSet(cursor, getVitessStatement());
    vitessResultSet.next();
    assertEquals(true, vitessResultSet.getBoolean(25));
    assertEquals(false, vitessResultSet.getBoolean(1));
    vitessResultSet = new VitessResultSet(cursorWithRowsAsNull, getVitessStatement());
    vitessResultSet.next();
    assertEquals(false, vitessResultSet.getBoolean(25));
    assertEquals(false, vitessResultSet.getBoolean(1));
  }

  @Test
  public void testgetByte() throws SQLException {
    Cursor cursor = getCursorWithRows();
    VitessResultSet vitessResultSet = new VitessResultSet(cursor, getVitessStatement());
    vitessResultSet.next();
    assertEquals(-50, vitessResultSet.getByte(1));
    assertEquals(1, vitessResultSet.getByte(25));
  }

  @Test
  public void testgetShort() throws SQLException {
    Cursor cursor = getCursorWithRows();
    VitessResultSet vitessResultSet = new VitessResultSet(cursor, getVitessStatement());
    vitessResultSet.next();
    assertEquals(-23000, vitessResultSet.getShort(3));
  }

  @Test
  public void testgetInt() throws SQLException {
    Cursor cursor = getCursorWithRows();
    VitessResultSet vitessResultSet = new VitessResultSet(cursor, getVitessStatement());
    vitessResultSet.next();
    assertEquals(-100, vitessResultSet.getInt(7));
  }

  @Test
  public void testgetLong() throws SQLException {
    Cursor cursor = getCursorWithRows();
    VitessResultSet vitessResultSet = new VitessResultSet(cursor, getVitessStatement());
    vitessResultSet.next();
    assertEquals(-1000, vitessResultSet.getInt(9));
  }

  @Test
  public void testgetFloat() throws SQLException {
    Cursor cursor = getCursorWithRows();
    VitessResultSet vitessResultSet = new VitessResultSet(cursor, getVitessStatement());
    vitessResultSet.next();
    assertEquals(24.52f, vitessResultSet.getFloat(11), 0.001);
  }

  @Test
  public void testgetDouble() throws SQLException {
    Cursor cursor = getCursorWithRows();
    VitessResultSet vitessResultSet = new VitessResultSet(cursor, getVitessStatement());
    vitessResultSet.next();
    assertEquals(100.43, vitessResultSet.getFloat(12), 0.001);
  }

  @Test
  public void testBigDecimal() throws SQLException {
    Cursor cursor = getCursorWithRows();
    VitessResultSet vitessResultSet = new VitessResultSet(cursor, getVitessStatement());
    vitessResultSet.next();
    assertEquals(new BigDecimal(BigInteger.valueOf(123456789), 5),
        vitessResultSet.getBigDecimal(18));
  }

  @Test
  public void testgetBytes() throws SQLException, UnsupportedEncodingException {
    Cursor cursor = getCursorWithRows();
    VitessResultSet vitessResultSet = new VitessResultSet(cursor, getVitessStatement());
    vitessResultSet.next();
    Assert.assertArrayEquals("HELLO TDS TEAM".getBytes("UTF-8"), vitessResultSet.getBytes(19));
  }

  @Test
  public void testgetDate() throws SQLException, UnsupportedEncodingException {
    Cursor cursor = getCursorWithRows();
    VitessResultSet vitessResultSet = new VitessResultSet(cursor, getVitessStatement());
    vitessResultSet.next();
    assertEquals(new java.sql.Date(116, 1, 6), vitessResultSet.getDate(14));
  }

  @Test
  public void testgetTime() throws SQLException, UnsupportedEncodingException {
    Cursor cursor = getCursorWithRows();
    VitessResultSet vitessResultSet = new VitessResultSet(cursor, getVitessStatement());
    vitessResultSet.next();
    assertEquals(new Time(12, 34, 56), vitessResultSet.getTime(15));
  }

  @Test
  public void testgetTimestamp() throws SQLException, UnsupportedEncodingException {
    Cursor cursor = getCursorWithRows();
    VitessResultSet vitessResultSet = new VitessResultSet(cursor, getVitessStatement());
    vitessResultSet.next();
    assertEquals(new Timestamp(116, 1, 6, 14, 15, 16, 0), vitessResultSet.getTimestamp(13));
  }

  @Test
  public void testgetZeroTimestampGarble() throws SQLException, UnsupportedEncodingException {
    Cursor cursor = getCursorWithRows();
    VitessResultSet vitessResultSet = new VitessResultSet(cursor, new VitessStatement(
        new VitessConnection(
            "jdbc:vitess://locahost:9000/vt_keyspace/keyspace?zeroDateTimeBehavior=garble",
            new Properties())));
    vitessResultSet.next();
    assertEquals("0002-11-30 00:00:00.0", vitessResultSet.getTimestamp(28).toString());
  }

  @Test
  public void testgetZeroTimestampConvertToNill()
      throws SQLException, UnsupportedEncodingException {
    Cursor cursor = getCursorWithRows();
    VitessResultSet vitessResultSet = new VitessResultSet(cursor, new VitessStatement(
        new VitessConnection(
            "jdbc:vitess://locahost:9000/vt_keyspace/keyspace?zeroDateTimeBehavior=convertToNull",
            new Properties())));
    vitessResultSet.next();
    Assert.assertNull(vitessResultSet.getTimestamp(28));
  }

  @Test
  public void testgetZeroTimestampException() throws SQLException, UnsupportedEncodingException {
    Cursor cursor = getCursorWithRows();
    VitessResultSet vitessResultSet = new VitessResultSet(cursor, new VitessStatement(
        new VitessConnection(
            "jdbc:vitess://locahost:9000/vt_keyspace/keyspace?zeroDateTimeBehavior=exception",
            new Properties())));
    vitessResultSet.next();
    try {
      vitessResultSet.getTimestamp(28);
      Assert.fail("expected getTimestamp to throw an exception");
    } catch (SQLException e) {
    }
  }

  @Test
  public void testgetZeroTimestampRound() throws SQLException, UnsupportedEncodingException {
    Cursor cursor = getCursorWithRows();
    VitessResultSet vitessResultSet = new VitessResultSet(cursor, new VitessStatement(
        new VitessConnection(
            "jdbc:vitess://locahost:9000/vt_keyspace/keyspace?zeroDateTimeBehavior=round",
            new Properties())));
    vitessResultSet.next();
    assertEquals("0001-01-01 00:00:00.0", vitessResultSet.getTimestamp(28).toString());
  }

  @Test
  public void testgetZeroDateRound() throws SQLException, UnsupportedEncodingException {
    Cursor cursor = getCursorWithRows();
    VitessResultSet vitessResultSet = new VitessResultSet(cursor, new VitessStatement(
        new VitessConnection(
            "jdbc:vitess://locahost:9000/vt_keyspace/keyspace?zeroDateTimeBehavior=round",
            new Properties())));
    vitessResultSet.next();
    assertEquals("0001-01-01", vitessResultSet.getDate(28).toString());
  }

  @Test
  public void testgetStringbyColumnLabel() throws SQLException {
    Cursor cursor = getCursorWithRows();
    VitessResultSet vitessResultSet = new VitessResultSet(cursor, getVitessStatement());
    vitessResultSet.next();
    assertEquals("-50", vitessResultSet.getString("col1"));
    assertEquals("50", vitessResultSet.getString("col2"));
    assertEquals("-23000", vitessResultSet.getString("col3"));
    assertEquals("23000", vitessResultSet.getString("col4"));
    assertEquals("-100", vitessResultSet.getString("col5"));
    assertEquals("100", vitessResultSet.getString("col6"));
    assertEquals("-100", vitessResultSet.getString("col7"));
    assertEquals("100", vitessResultSet.getString("col8"));
    assertEquals("-1000", vitessResultSet.getString("col9"));
    assertEquals("1000", vitessResultSet.getString("col10"));
    assertEquals("24.52", vitessResultSet.getString("col11"));
    assertEquals("100.43", vitessResultSet.getString("col12"));
    assertEquals("2016-02-06 14:15:16.0", vitessResultSet.getString("col13"));
    assertEquals("2016-02-06", vitessResultSet.getString("col14"));
    assertEquals("12:34:56", vitessResultSet.getString("col15"));
    assertEquals("2016-02-06 14:15:16.0", vitessResultSet.getString("col16"));
    assertEquals("2016", vitessResultSet.getString("col17"));
    assertEquals("1234.56789", vitessResultSet.getString("col18"));
    assertEquals("HELLO TDS TEAM", vitessResultSet.getString("col19"));
    assertEquals("HELLO TDS TEAM", vitessResultSet.getString("col20"));
    assertEquals("HELLO TDS TEAM", vitessResultSet.getString("col21"));
    assertEquals("HELLO TDS TEAM", vitessResultSet.getString("col22"));
    assertEquals("N", vitessResultSet.getString("col23"));
    assertEquals("HELLO TDS TEAM", vitessResultSet.getString("col24"));
    assertEquals("1", vitessResultSet.getString("col25"));
    assertEquals("val123", vitessResultSet.getString("col26"));
    assertEquals("val123", vitessResultSet.getString("col27"));
  }

  @Test
  public void testgetBooleanbyColumnLabel() throws SQLException {
    Cursor cursor = getCursorWithRows();
    VitessResultSet vitessResultSet = new VitessResultSet(cursor, getVitessStatement());
    vitessResultSet.next();
    assertEquals(true, vitessResultSet.getBoolean("col25"));
    assertEquals(false, vitessResultSet.getBoolean("col1"));
  }

  @Test
  public void testgetBytebyColumnLabel() throws SQLException {
    Cursor cursor = getCursorWithRows();
    VitessResultSet vitessResultSet = new VitessResultSet(cursor, getVitessStatement());
    vitessResultSet.next();
    assertEquals(-50, vitessResultSet.getByte("col1"));
    assertEquals(1, vitessResultSet.getByte("col25"));
  }

  @Test
  public void testgetShortbyColumnLabel() throws SQLException {
    Cursor cursor = getCursorWithRows();
    VitessResultSet vitessResultSet = new VitessResultSet(cursor, getVitessStatement());
    vitessResultSet.next();
    assertEquals(-23000, vitessResultSet.getShort("col3"));
  }

  @Test
  public void testgetIntbyColumnLabel() throws SQLException {
    Cursor cursor = getCursorWithRows();
    VitessResultSet vitessResultSet = new VitessResultSet(cursor, getVitessStatement());
    vitessResultSet.next();
    assertEquals(-100, vitessResultSet.getInt("col7"));
  }

  @Test
  public void testgetLongbyColumnLabel() throws SQLException {
    Cursor cursor = getCursorWithRows();
    VitessResultSet vitessResultSet = new VitessResultSet(cursor, getVitessStatement());
    vitessResultSet.next();
    assertEquals(-1000, vitessResultSet.getInt("col9"));
  }

  @Test
  public void testBigIntegerbyColumnLabel() throws SQLException {
    Cursor cursor = getCursorWithRows();
    VitessResultSet vitessResultSet = new VitessResultSet(cursor, getVitessStatement());
    vitessResultSet.next();
    assertEquals(new BigInteger("1000"), vitessResultSet.getBigInteger("col10"));
  }

  @Test
  public void testgetFloatbyColumnLabel() throws SQLException {
    Cursor cursor = getCursorWithRows();
    VitessResultSet vitessResultSet = new VitessResultSet(cursor, getVitessStatement());
    vitessResultSet.next();
    assertEquals(24.52f, vitessResultSet.getFloat("col11"), 0.001);
  }

  @Test
  public void testgetDoublebyColumnLabel() throws SQLException {
    Cursor cursor = getCursorWithRows();
    VitessResultSet vitessResultSet = new VitessResultSet(cursor, getVitessStatement());
    vitessResultSet.next();
    assertEquals(100.43, vitessResultSet.getFloat("col12"), 0.001);
  }

  @Test
  public void testBigDecimalbyColumnLabel() throws SQLException {
    Cursor cursor = getCursorWithRows();
    VitessResultSet vitessResultSet = new VitessResultSet(cursor, getVitessStatement());
    vitessResultSet.next();
    assertEquals(new BigDecimal(BigInteger.valueOf(123456789), 5),
        vitessResultSet.getBigDecimal("col18"));
  }

  @Test
  public void testgetBytesbyColumnLabel() throws SQLException, UnsupportedEncodingException {
    Cursor cursor = getCursorWithRows();
    VitessResultSet vitessResultSet = new VitessResultSet(cursor, getVitessStatement());
    vitessResultSet.next();
    Assert.assertArrayEquals("HELLO TDS TEAM".getBytes("UTF-8"), vitessResultSet.getBytes("col19"));
  }

  @Test
  public void testgetDatebyColumnLabel() throws SQLException, UnsupportedEncodingException {
    Cursor cursor = getCursorWithRows();
    VitessResultSet vitessResultSet = new VitessResultSet(cursor, getVitessStatement());
    vitessResultSet.next();
    assertEquals(new java.sql.Date(116, 1, 6), vitessResultSet.getDate("col14"));
  }

  @Test
  public void testgetTimebyColumnLabel() throws SQLException, UnsupportedEncodingException {
    Cursor cursor = getCursorWithRows();
    VitessResultSet vitessResultSet = new VitessResultSet(cursor, getVitessStatement());
    vitessResultSet.next();
    assertEquals(new Time(12, 34, 56), vitessResultSet.getTime("col15"));
  }

  @Test
  public void testgetTimestampbyColumnLabel() throws SQLException, UnsupportedEncodingException {
    Cursor cursor = getCursorWithRows();
    VitessResultSet vitessResultSet = new VitessResultSet(cursor, getVitessStatement());
    vitessResultSet.next();
    assertEquals(new Timestamp(116, 1, 6, 14, 15, 16, 0), vitessResultSet.getTimestamp("col13"));
  }

  @Test
  public void testgetAsciiStream() throws SQLException, UnsupportedEncodingException {
    Cursor cursor = getCursorWithRows();
    VitessResultSet vitessResultSet = new VitessResultSet(cursor);
    vitessResultSet.next();
    // Need to implement AssertEquivalant
    //Assert.assertEquals((InputStream)(new ByteArrayInputStream("HELLO TDS TEAM".getBytes())),
    // vitessResultSet
    // .getAsciiStream(19));
  }

  @Test
  public void testGetBinaryStream() throws SQLException, IOException {
    Cursor cursor = getCursorWithRowsAsNull();
    VitessResultSet vitessResultSet = new VitessResultSet(cursor, getVitessStatement());
    vitessResultSet.next();
    byte[] ba1 = new byte[128];
    new ByteArrayInputStream("HELLO TDS TEAM".getBytes()).read(ba1, 0, 128);
    byte[] ba2 = new byte[128];
    vitessResultSet.getBinaryStream(19).read(ba2, 0, 128);
    Assert.assertArrayEquals(ba1, ba2);

    byte[] ba3 = new byte[128];
    vitessResultSet.getBinaryStream(22).read(ba3, 0, 128);
    Assert.assertArrayEquals(ba1, ba3);

    assertEquals(null, vitessResultSet.getBinaryStream(27));
  }

  @Test
  public void testEnhancedFieldsFromCursor() throws Exception {
    Cursor cursor = getCursorWithRows();
    VitessResultSet vitessResultSet = new VitessResultSet(cursor, getVitessStatement());
    assertEquals(cursor.getFields().size(), vitessResultSet.getFields().size());
  }

  @Test
  public void testGetStringUsesEncoding() throws Exception {
    VitessConnection conn = getVitessConnection();
    VitessResultSet resultOne = PowerMockito
        .spy(new VitessResultSet(getCursorWithRows(), new VitessStatement(conn)));
    resultOne.next();
    // test all ways to get to convertBytesToString

    // Verify that we're going through convertBytesToString for column types that return bytes
    // (string-like),
    // but not for those that return a real object
    resultOne.getString("col21"); // is a string, should go through convert bytes
    resultOne.getString("col13"); // is a datetime, should not
    PowerMockito.verifyPrivate(resultOne, VerificationModeFactory.times(1))
        .invoke("convertBytesToString", Matchers.any(byte[].class), Matchers.anyString());

    conn.setIncludedFields(Query.ExecuteOptions.IncludedFields.TYPE_AND_NAME);
    VitessResultSet resultTwo = PowerMockito
        .spy(new VitessResultSet(getCursorWithRows(), new VitessStatement(conn)));
    resultTwo.next();

    // neither of these should go through convertBytesToString, because we didn't include all fields
    resultTwo.getString("col21");
    resultTwo.getString("col13");
    PowerMockito.verifyPrivate(resultTwo, VerificationModeFactory.times(0))
        .invoke("convertBytesToString", Matchers.any(byte[].class), Matchers.anyString());
  }

  @Test
  public void testGetObjectForBitValues() throws Exception {
    VitessConnection conn = getVitessConnection();

    ByteString.Output value = ByteString.newOutput();
    value.write(new byte[]{1});
    value.write(new byte[]{0});
    value.write(new byte[]{1, 2, 3, 4});

    Query.QueryResult result = Query.QueryResult.newBuilder().addFields(
        Query.Field.newBuilder().setName("col1").setColumnLength(1).setType(Query.Type.BIT))
        .addFields(
            Query.Field.newBuilder().setName("col2").setColumnLength(1).setType(Query.Type.BIT))
        .addFields(
            Query.Field.newBuilder().setName("col3").setColumnLength(4).setType(Query.Type.BIT))
        .addRows(Query.Row.newBuilder().addLengths(1).addLengths(1).addLengths(4)
            .setValues(value.toByteString())).build();

    VitessResultSet vitessResultSet = PowerMockito
        .spy(new VitessResultSet(new SimpleCursor(result), new VitessStatement(conn)));
    vitessResultSet.next();

    assertEquals(true, vitessResultSet.getObject(1));
    assertEquals(false, vitessResultSet.getObject(2));
    Assert.assertArrayEquals(new byte[]{1, 2, 3, 4}, (byte[]) vitessResultSet.getObject(3));

    PowerMockito.verifyPrivate(vitessResultSet, VerificationModeFactory.times(3))
        .invoke("convertBytesIfPossible", Matchers.any(byte[].class),
            Matchers.any(FieldWithMetadata.class));

    conn.setIncludedFields(Query.ExecuteOptions.IncludedFields.TYPE_AND_NAME);
    vitessResultSet = PowerMockito
        .spy(new VitessResultSet(new SimpleCursor(result), new VitessStatement(conn)));
    vitessResultSet.next();

    Assert.assertArrayEquals(new byte[]{1}, (byte[]) vitessResultSet.getObject(1));
    Assert.assertArrayEquals(new byte[]{0}, (byte[]) vitessResultSet.getObject(2));
    Assert.assertArrayEquals(new byte[]{1, 2, 3, 4}, (byte[]) vitessResultSet.getObject(3));

    PowerMockito.verifyPrivate(vitessResultSet, VerificationModeFactory.times(0))
        .invoke("convertBytesIfPossible", Matchers.any(byte[].class),
            Matchers.any(FieldWithMetadata.class));
  }

  @Test
  public void testGetObjectForVarBinLikeValues() throws Exception {
    VitessConnection conn = getVitessConnection();

    ByteString.Output value = ByteString.newOutput();

    byte[] binary = new byte[]{1, 2, 3, 4};
    byte[] varbinary = new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13};
    byte[] blob = new byte[MysqlDefs.LENGTH_BLOB];
    for (int i = 0; i < blob.length; i++) {
      blob[i] = 1;
    }
    byte[] fakeGeometry = new byte[]{2, 3, 4};

    value.write(binary);
    value.write(varbinary);
    value.write(blob);
    value.write(fakeGeometry);

    Query.QueryResult result = Query.QueryResult.newBuilder().addFields(
        Query.Field.newBuilder().setName("col1").setColumnLength(4)
            .setCharset(CharsetMapping.MYSQL_COLLATION_INDEX_binary).setType(Query.Type.BINARY)
            .setFlags(Query.MySqlFlag.BINARY_FLAG_VALUE)).addFields(
        Query.Field.newBuilder().setName("col2").setColumnLength(13)
            .setCharset(CharsetMapping.MYSQL_COLLATION_INDEX_binary).setType(Query.Type.VARBINARY)
            .setFlags(Query.MySqlFlag.BINARY_FLAG_VALUE)).addFields(
        Query.Field.newBuilder().setName("col3") // should go to LONGVARBINARY due to below settings
            .setColumnLength(MysqlDefs.LENGTH_BLOB)
            .setCharset(CharsetMapping.MYSQL_COLLATION_INDEX_binary)
            .setFlags(Query.MySqlFlag.BINARY_FLAG_VALUE).setType(Query.Type.BLOB)).addFields(
        Query.Field.newBuilder().setName("col4").setType(Query.Type.GEOMETRY)
            .setCharset(CharsetMapping.MYSQL_COLLATION_INDEX_binary).setType(Query.Type.BINARY)
            .setFlags(Query.MySqlFlag.BINARY_FLAG_VALUE)).addRows(
        Query.Row.newBuilder().addLengths(4).addLengths(13).addLengths(MysqlDefs.LENGTH_BLOB)
            .addLengths(3).setValues(value.toByteString())).build();

    VitessResultSet vitessResultSet = PowerMockito
        .spy(new VitessResultSet(new SimpleCursor(result), new VitessStatement(conn)));
    vitessResultSet.next();

    // All of these types should pass straight through, returning the direct bytes
    Assert.assertArrayEquals(binary, (byte[]) vitessResultSet.getObject(1));
    Assert.assertArrayEquals(varbinary, (byte[]) vitessResultSet.getObject(2));
    Assert.assertArrayEquals(blob, (byte[]) vitessResultSet.getObject(3));
    Assert.assertArrayEquals(fakeGeometry, (byte[]) vitessResultSet.getObject(4));

    // We should still call the function 4 times
    PowerMockito.verifyPrivate(vitessResultSet, VerificationModeFactory.times(4))
        .invoke("convertBytesIfPossible", Matchers.any(byte[].class),
            Matchers.any(FieldWithMetadata.class));

    conn.setIncludedFields(Query.ExecuteOptions.IncludedFields.TYPE_AND_NAME);
    vitessResultSet = PowerMockito
        .spy(new VitessResultSet(new SimpleCursor(result), new VitessStatement(conn)));
    vitessResultSet.next();

    // Same as above since this doesn't really do much but pass right through for the varbinary type
    Assert.assertArrayEquals(binary, (byte[]) vitessResultSet.getObject(1));
    Assert.assertArrayEquals(varbinary, (byte[]) vitessResultSet.getObject(2));
    Assert.assertArrayEquals(blob, (byte[]) vitessResultSet.getObject(3));
    Assert.assertArrayEquals(fakeGeometry, (byte[]) vitessResultSet.getObject(4));

    // Never call because not including all
    PowerMockito.verifyPrivate(vitessResultSet, VerificationModeFactory.times(0))
        .invoke("convertBytesIfPossible", Matchers.any(byte[].class),
            Matchers.any(FieldWithMetadata.class));
  }

  @Test
  public void testGetObjectForStringLikeValues() throws Exception {
    ByteString.Output value = ByteString.newOutput();

    String trimmedCharStr = "wasting space";
    String varcharStr = "i have a variable length!";
    String masqueradingBlobStr = "look at me, im a blob";
    String textStr = "an enthralling string of TEXT in some foreign language";
    String jsonStr = "{\"status\": \"ok\"}";

    int paddedCharColLength = 20;
    byte[] trimmedChar = StringUtils.getBytes(trimmedCharStr, "UTF-16");
    byte[] varchar = StringUtils.getBytes(varcharStr, "UTF-8");
    byte[] masqueradingBlob = StringUtils.getBytes(masqueradingBlobStr, "US-ASCII");
    byte[] text = StringUtils.getBytes(textStr, "ISO8859_8");
    byte[] opaqueBinary = new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9};
    byte[] json = StringUtils.getBytes(jsonStr, "UTF-8");

    value.write(trimmedChar);
    value.write(varchar);
    value.write(opaqueBinary);
    value.write(masqueradingBlob);
    value.write(text);
    value.write(json);

    Query.QueryResult result = Query.QueryResult.newBuilder()
        // This tests CHAR
        .addFields(Query.Field.newBuilder().setName("col1").setColumnLength(paddedCharColLength)
            .setCharset(/* utf-16 collation index from CharsetMapping */ 54)
            .setType(Query.Type.CHAR))
        // This tests VARCHAR
        .addFields(Query.Field.newBuilder().setName("col2").setColumnLength(varchar.length)
            .setCharset(CharsetMapping.MYSQL_COLLATION_INDEX_utf8).setType(Query.Type.VARCHAR))
        // This tests VARCHAR that is an opaque binary
        .addFields(Query.Field.newBuilder().setName("col2").setColumnLength(opaqueBinary.length)
            .setCharset(CharsetMapping.MYSQL_COLLATION_INDEX_binary)
            .setFlags(Query.MySqlFlag.BINARY_FLAG_VALUE).setType(Query.Type.VARCHAR))
        // This tests LONGVARCHAR
        .addFields(Query.Field.newBuilder().setName("col3").setColumnLength(masqueradingBlob.length)
            .setCharset(/* us-ascii collation index from CharsetMapping */11)
            .setType(Query.Type.BLOB))
        // This tests TEXT, which falls through the default case of the switch
        .addFields(Query.Field.newBuilder().setName("col4").setColumnLength(text.length)
            .setCharset(/* corresponds to greek, from CharsetMapping */25).setType(Query.Type.TEXT))
        .addFields(Query.Field.newBuilder().setName("col5").setColumnLength(json.length)
            .setCharset(CharsetMapping.MYSQL_COLLATION_INDEX_utf8).setType(Query.Type.JSON))
        .addRows(Query.Row.newBuilder().addLengths(trimmedChar.length).addLengths(varchar.length)
            .addLengths(opaqueBinary.length).addLengths(masqueradingBlob.length)
            .addLengths(text.length).addLengths(json.length).setValues(value.toByteString()))
        .build();

    VitessConnection conn = getVitessConnection();
    VitessResultSet vitessResultSet = PowerMockito
        .spy(new VitessResultSet(new SimpleCursor(result), new VitessStatement(conn)));
    vitessResultSet.next();

    assertEquals(trimmedCharStr, vitessResultSet.getObject(1));
    assertEquals(varcharStr, vitessResultSet.getObject(2));
    Assert.assertArrayEquals(opaqueBinary, (byte[]) vitessResultSet.getObject(3));
    assertEquals(masqueradingBlobStr, vitessResultSet.getObject(4));
    assertEquals(textStr, vitessResultSet.getObject(5));
    assertEquals(jsonStr, vitessResultSet.getObject(6));

    PowerMockito.verifyPrivate(vitessResultSet, VerificationModeFactory.times(6))
        .invoke("convertBytesIfPossible", Matchers.any(byte[].class),
            Matchers.any(FieldWithMetadata.class));

    conn.setIncludedFields(Query.ExecuteOptions.IncludedFields.TYPE_AND_NAME);
    vitessResultSet = PowerMockito
        .spy(new VitessResultSet(new SimpleCursor(result), new VitessStatement(conn)));
    vitessResultSet.next();

    Assert.assertArrayEquals(trimmedChar, (byte[]) vitessResultSet.getObject(1));
    Assert.assertArrayEquals(varchar, (byte[]) vitessResultSet.getObject(2));
    Assert.assertArrayEquals(opaqueBinary, (byte[]) vitessResultSet.getObject(3));
    Assert.assertArrayEquals(masqueradingBlob, (byte[]) vitessResultSet.getObject(4));
    Assert.assertArrayEquals(text, (byte[]) vitessResultSet.getObject(5));
    Assert.assertArrayEquals(json, (byte[]) vitessResultSet.getObject(6));

    PowerMockito.verifyPrivate(vitessResultSet, VerificationModeFactory.times(0))
        .invoke("convertBytesIfPossible", Matchers.any(byte[].class),
            Matchers.any(FieldWithMetadata.class));
  }

  @Test
  public void testGetClob() throws SQLException {
    VitessResultSet vitessResultSet = new VitessResultSet(new String[]{"clob"},
        new Query.Type[]{Query.Type.VARCHAR}, new String[][]{new String[]{"clobValue"}},
        new ConnectionProperties());
    Assert.assertTrue(vitessResultSet.next());

    Clob clob = vitessResultSet.getClob(1);
    assertEquals("clobValue", clob.getSubString(1, (int) clob.length()));

    clob = vitessResultSet.getClob("clob");
    assertEquals("clobValue", clob.getSubString(1, (int) clob.length()));
  }
}
