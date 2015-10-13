package com.youtube.vitess.client.cursor;

import com.google.common.primitives.UnsignedLong;
import com.google.protobuf.ByteString;

import com.youtube.vitess.proto.Query.Field;
import com.youtube.vitess.proto.Query.QueryResult;
import com.youtube.vitess.proto.Query;

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
    List<Field.Type> types =
        Arrays.asList(Field.Type.TYPE_TINY, Field.Type.TYPE_SHORT, Field.Type.TYPE_INT24);
    for (Field.Type type : types) {
      try (
          Cursor cursor = new SimpleCursor(
              QueryResult.newBuilder()
                  .addFields(Field.newBuilder().setName("col0").setType(type).build())
                  .addRows(Query.Row.newBuilder().addValues(ByteString.copyFromUtf8("12345")))
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
                .addFields(
                    Field.newBuilder()
                        .setName("col0")
                        .setType(Field.Type.TYPE_LONGLONG)
                        .setFlags(Field.Flag.VT_UNSIGNED_FLAG_VALUE)
                        .build())
                .addRows(
                    Query.Row.newBuilder().addValues(ByteString.copyFromUtf8("18446744073709551615")))
                .build())) {
      Row row = cursor.next();
      Assert.assertNotNull(row);
      Assert.assertEquals(UnsignedLong.fromLongBits(-1), row.getULong("col0"));
    }
  }

  @Test
  public void testGetString() throws Exception {
    List<Field.Type> types = Arrays.asList(Field.Type.TYPE_ENUM, Field.Type.TYPE_SET);
    for (Field.Type type : types) {
      try (
          Cursor cursor = new SimpleCursor(
              QueryResult.newBuilder()
                  .addFields(Field.newBuilder().setName("col0").setType(type).build())
                  .addRows(Query.Row.newBuilder().addValues(ByteString.copyFromUtf8("val123")))
                  .build())) {
        Row row = cursor.next();
        Assert.assertNotNull(row);
        Assert.assertEquals("val123", row.getString("col0"));
      }
    }
  }

  @Test
  public void testGetLong() throws Exception {
    List<Field.Type> types = Arrays.asList(Field.Type.TYPE_LONG, Field.Type.TYPE_LONGLONG);
    for (Field.Type type : types) {
      try (
          Cursor cursor = new SimpleCursor(
              QueryResult.newBuilder()
                  .addFields(Field.newBuilder().setName("col0").setType(type).build())
                  .addRows(Query.Row.newBuilder().addValues(ByteString.copyFromUtf8("12345")))
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
                .addFields(
                    Field.newBuilder().setName("col0").setType(Field.Type.TYPE_DOUBLE).build())
                .addRows(Query.Row.newBuilder().addValues(ByteString.copyFromUtf8("2.5")))
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
                .addFields(
                    Field.newBuilder().setName("col0").setType(Field.Type.TYPE_FLOAT).build())
                .addRows(Query.Row.newBuilder().addValues(ByteString.copyFromUtf8("2.5")))
                .build())) {
      Row row = cursor.next();
      Assert.assertNotNull(row);
      Assert.assertEquals(2.5f, row.getFloat("col0"), 0.01f);
    }
  }

  @Test
  public void testGetDateTime() throws Exception {
    List<Field.Type> types = Arrays.asList(Field.Type.TYPE_DATETIME, Field.Type.TYPE_TIMESTAMP);
    for (Field.Type type : types) {
      try (
          Cursor cursor = new SimpleCursor(
              QueryResult.newBuilder()
                  .addFields(Field.newBuilder().setName("col0").setType(type).build())
                  .addRows(
                      Query.Row.newBuilder().addValues(ByteString.copyFromUtf8("2008-01-02 14:15:16")))
                  .build())) {
        Row row = cursor.next();
        Assert.assertNotNull(row);
        Assert.assertEquals(new DateTime(2008, 1, 2, 14, 15, 16), row.getDateTime("col0"));
      }
    }

    types = Arrays.asList(Field.Type.TYPE_DATE, Field.Type.TYPE_NEWDATE);
    for (Field.Type type : types) {
      try (
          Cursor cursor = new SimpleCursor(
              QueryResult.newBuilder()
                  .addFields(Field.newBuilder().setName("col0").setType(type).build())
                  .addRows(Query.Row.newBuilder().addValues(ByteString.copyFromUtf8("2008-01-02")))
                  .build())) {
        Row row = cursor.next();
        Assert.assertNotNull(row);
        Assert.assertEquals(new DateTime(2008, 1, 2, 0, 0, 0), row.getDateTime("col0"));
      }
    }

    try (
        Cursor cursor = new SimpleCursor(
            QueryResult.newBuilder()
                .addFields(Field.newBuilder().setName("col0").setType(Field.Type.TYPE_TIME).build())
                .addRows(Query.Row.newBuilder().addValues(ByteString.copyFromUtf8("12:34:56")))
                .build())) {
      Row row = cursor.next();
      Assert.assertNotNull(row);
      Assert.assertEquals(new DateTime(1970, 1, 1, 12, 34, 56), row.getDateTime("col0"));
    }
  }

  @Test
  public void testGetBytes() throws Exception {
    List<Field.Type> types =
        Arrays.asList(Field.Type.TYPE_VARCHAR, Field.Type.TYPE_BIT, Field.Type.TYPE_TINY_BLOB,
            Field.Type.TYPE_MEDIUM_BLOB, Field.Type.TYPE_LONG_BLOB, Field.Type.TYPE_BLOB,
            Field.Type.TYPE_VAR_STRING, Field.Type.TYPE_STRING, Field.Type.TYPE_GEOMETRY);
    for (Field.Type type : types) {
      try (
          Cursor cursor = new SimpleCursor(
              QueryResult.newBuilder()
                  .addFields(Field.newBuilder().setName("col0").setType(type).build())
                  .addRows(Query.Row.newBuilder().addValues(ByteString.copyFromUtf8("hello world")))
                  .build())) {
        Row row = cursor.next();
        Assert.assertNotNull(row);
        Assert.assertArrayEquals("hello world".getBytes("UTF-8"), row.getBytes("col0"));
      }
    }
  }

  @Test
  public void testGetBigDecimal() throws Exception {
    List<Field.Type> types = Arrays.asList(Field.Type.TYPE_DECIMAL, Field.Type.TYPE_NEWDECIMAL);
    for (Field.Type type : types) {
      try (
          Cursor cursor = new SimpleCursor(
              QueryResult.newBuilder()
                  .addFields(Field.newBuilder().setName("col0").setType(type).build())
                  .addRows(Query.Row.newBuilder().addValues(ByteString.copyFromUtf8("1234.56789")))
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
                .addFields(Field.newBuilder().setName("col0").setType(Field.Type.TYPE_YEAR).build())
                .addRows(Query.Row.newBuilder().addValues(ByteString.copyFromUtf8("1234")))
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
                .addFields(Field.newBuilder().setName("col0").setType(Field.Type.TYPE_NULL).build())
                .addRows(Query.Row.newBuilder().addValues(ByteString.copyFromUtf8("1234")))
                .build())) {
      Row row = cursor.next();
      Assert.assertNotNull(row);
      Assert.assertEquals(null, row.getObject("col0"));
    }
  }
}
