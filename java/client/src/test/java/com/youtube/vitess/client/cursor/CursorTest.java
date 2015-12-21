package com.youtube.vitess.client.cursor;

import com.google.common.primitives.UnsignedLong;
import com.google.protobuf.ByteString;

import com.youtube.vitess.proto.Query;
import com.youtube.vitess.proto.Query.Field;
import com.youtube.vitess.proto.Query.QueryResult;

import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;

@RunWith(JUnit4.class)
public class CursorTest {
  @Test
  public void testFindColumn() throws Exception {
    try (
        Cursor cursor = new SimpleCursor(
            QueryResult.newBuilder()
                .addFields(Field.newBuilder().setName("col0").build())
                .addFields(Field.newBuilder().setName("col1").build())
                .addFields(Field.newBuilder().setName("col2").build())
                .build())) {
      Assert.assertEquals(0, cursor.findColumn("col0"));
      Assert.assertEquals(1, cursor.findColumn("col1"));
      Assert.assertEquals(2, cursor.findColumn("col2"));
    }
  }

  @Test
  public void testGetInt() throws Exception {
    List<Query.Type> types = Arrays.asList(Query.Type.INT8, Query.Type.UINT8, Query.Type.INT16,
        Query.Type.UINT16, Query.Type.INT24, Query.Type.UINT24, Query.Type.INT32);
    for (Query.Type type : types) {
      try (
          Cursor cursor = new SimpleCursor(
              QueryResult.newBuilder()
                  .addFields(Field.newBuilder().setName("col0").setType(type).build())
                  .addRows(
                      Query.Row.newBuilder()
                          .addLengths("12345".length())
                          .setValues(ByteString.copyFromUtf8("12345")))
                  .build())) {
        Row row = cursor.next();
        Assert.assertNotNull(row);
        Assert.assertEquals(12345, row.getInt("col0"));
      }
    }
  }

  @Test
  public void testGetULong() throws Exception {
    try (
        Cursor cursor = new SimpleCursor(
            QueryResult.newBuilder()
                .addFields(Field.newBuilder().setName("col0").setType(Query.Type.UINT64).build())
                .addRows(
                    Query.Row.newBuilder()
                        .addLengths("18446744073709551615".length())
                        .setValues(ByteString.copyFromUtf8("18446744073709551615")))
                .build())) {
      Row row = cursor.next();
      Assert.assertNotNull(row);
      Assert.assertEquals(UnsignedLong.fromLongBits(-1), row.getULong("col0"));
    }
  }

  @Test
  public void testGetString() throws Exception {
    List<Query.Type> types = Arrays.asList(Query.Type.ENUM, Query.Type.SET, Query.Type.BIT);
    for (Query.Type type : types) {
      try (
          Cursor cursor = new SimpleCursor(
              QueryResult.newBuilder()
                  .addFields(Field.newBuilder().setName("col0").setType(type).build())
                  .addRows(
                      Query.Row.newBuilder()
                          .addLengths("val123".length())
                          .setValues(ByteString.copyFromUtf8("val123")))
                  .build())) {
        Row row = cursor.next();
        Assert.assertNotNull(row);
        Assert.assertEquals("val123", row.getString("col0"));
      }
    }
  }

  @Test
  public void testGetLong() throws Exception {
    List<Query.Type> types = Arrays.asList(Query.Type.UINT32, Query.Type.INT64);
    for (Query.Type type : types) {
      try (
          Cursor cursor = new SimpleCursor(
              QueryResult.newBuilder()
                  .addFields(Field.newBuilder().setName("col0").setType(type).build())
                  .addRows(
                      Query.Row.newBuilder()
                          .addLengths("12345".length())
                          .setValues(ByteString.copyFromUtf8("12345")))
                  .build())) {
        Row row = cursor.next();
        Assert.assertNotNull(row);
        Assert.assertEquals(12345L, row.getLong("col0"));
      }
    }
  }

  @Test
  public void testGetDouble() throws Exception {
    try (
        Cursor cursor = new SimpleCursor(
            QueryResult.newBuilder()
                .addFields(Field.newBuilder().setName("col0").setType(Query.Type.FLOAT64).build())
                .addRows(
                    Query.Row.newBuilder()
                        .addLengths("2.5".length())
                        .setValues(ByteString.copyFromUtf8("2.5")))
                .build())) {
      Row row = cursor.next();
      Assert.assertNotNull(row);
      Assert.assertEquals(2.5, row.getDouble("col0"), 0.01);
    }
  }

  @Test
  public void testGetFloat() throws Exception {
    try (
        Cursor cursor = new SimpleCursor(
            QueryResult.newBuilder()
                .addFields(Field.newBuilder().setName("col0").setType(Query.Type.FLOAT32).build())
                .addRows(
                    Query.Row.newBuilder()
                        .addLengths("2.5".length())
                        .setValues(ByteString.copyFromUtf8("2.5")))
                .build())) {
      Row row = cursor.next();
      Assert.assertNotNull(row);
      Assert.assertEquals(2.5f, row.getFloat("col0"), 0.01f);
    }
  }

  @Test
  public void testGetDateTime() throws Exception {
    List<Query.Type> types = Arrays.asList(Query.Type.DATETIME, Query.Type.TIMESTAMP);
    for (Query.Type type : types) {
      try (
          Cursor cursor = new SimpleCursor(
              QueryResult.newBuilder()
                  .addFields(Field.newBuilder().setName("col0").setType(type).build())
                  .addRows(
                      Query.Row.newBuilder()
                          .addLengths("2008-01-02 14:15:16".length())
                          .setValues(ByteString.copyFromUtf8("2008-01-02 14:15:16")))
                  .build())) {
        Row row = cursor.next();
        Assert.assertNotNull(row);
        Assert.assertEquals(new DateTime(2008, 1, 2, 14, 15, 16), row.getDateTime("col0"));
      }
    }

    types = Arrays.asList(Query.Type.DATE);
    for (Query.Type type : types) {
      try (
          Cursor cursor = new SimpleCursor(
              QueryResult.newBuilder()
                  .addFields(Field.newBuilder().setName("col0").setType(type).build())
                  .addRows(
                      Query.Row.newBuilder()
                          .addLengths("2008-01-02".length())
                          .setValues(ByteString.copyFromUtf8("2008-01-02")))
                  .build())) {
        Row row = cursor.next();
        Assert.assertNotNull(row);
        Assert.assertEquals(new DateTime(2008, 1, 2, 0, 0, 0), row.getDateTime("col0"));
      }
    }

    try (
        Cursor cursor = new SimpleCursor(
            QueryResult.newBuilder()
                .addFields(Field.newBuilder().setName("col0").setType(Query.Type.TIME).build())
                .addRows(
                    Query.Row.newBuilder()
                        .addLengths("12:34:56".length())
                        .setValues(ByteString.copyFromUtf8("12:34:56")))
                .build())) {
      Row row = cursor.next();
      Assert.assertNotNull(row);
      Assert.assertEquals(new DateTime(1970, 1, 1, 12, 34, 56), row.getDateTime("col0"));
    }
  }

  @Test
  public void testGetBytes() throws Exception {
    List<Query.Type> types = Arrays.asList(Query.Type.TEXT, Query.Type.BLOB, Query.Type.VARCHAR,
        Query.Type.VARBINARY, Query.Type.CHAR, Query.Type.BINARY);
    for (Query.Type type : types) {
      try (
          Cursor cursor = new SimpleCursor(
              QueryResult.newBuilder()
                  .addFields(Field.newBuilder().setName("col0").setType(type).build())
                  .addRows(
                      Query.Row.newBuilder()
                          .addLengths("hello world".length())
                          .setValues(ByteString.copyFromUtf8("hello world")))
                  .build())) {
        Row row = cursor.next();
        Assert.assertNotNull(row);
        Assert.assertArrayEquals("hello world".getBytes("UTF-8"), row.getBytes("col0"));
      }
    }
  }

  @Test
  public void testGetBigDecimal() throws Exception {
    List<Query.Type> types = Arrays.asList(Query.Type.DECIMAL);
    for (Query.Type type : types) {
      try (
          Cursor cursor = new SimpleCursor(
              QueryResult.newBuilder()
                  .addFields(Field.newBuilder().setName("col0").setType(type).build())
                  .addRows(
                      Query.Row.newBuilder()
                          .addLengths("1234.56789".length())
                          .setValues(ByteString.copyFromUtf8("1234.56789")))
                  .build())) {
        Row row = cursor.next();
        Assert.assertNotNull(row);
        Assert.assertEquals(
            new BigDecimal(BigInteger.valueOf(123456789), 5), row.getBigDecimal("col0"));
      }
    }
  }

  @Test
  public void testGetShort() throws Exception {
    try (
        Cursor cursor = new SimpleCursor(
            QueryResult.newBuilder()
                .addFields(Field.newBuilder().setName("col0").setType(Query.Type.YEAR).build())
                .addRows(
                    Query.Row.newBuilder()
                        .addLengths("1234".length())
                        .setValues(ByteString.copyFromUtf8("1234")))
                .build())) {
      Row row = cursor.next();
      Assert.assertNotNull(row);
      Assert.assertEquals(1234, row.getShort("col0"));
    }
  }

  @Test
  public void testNull() throws Exception {
    try (
        Cursor cursor = new SimpleCursor(
            QueryResult.newBuilder()
                .addFields(Field.newBuilder().setName("col0").setType(Query.Type.NULL_TYPE).build())
                .addRows(
                    Query.Row.newBuilder()
                        .addLengths("1234".length())
                        .setValues(ByteString.copyFromUtf8("1234")))
                .build())) {
      Row row = cursor.next();
      Assert.assertNotNull(row);
      Assert.assertEquals(null, row.getObject("col0"));
    }
  }
}
