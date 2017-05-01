package io.vitess.client.cursor;

import com.google.common.primitives.UnsignedLong;
import com.google.protobuf.ByteString;
import io.vitess.proto.Query;
import io.vitess.proto.Query.Field;
import io.vitess.proto.Query.QueryResult;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class CursorTest {
  private static final Calendar GMT = Calendar.getInstance(TimeZone.getTimeZone("GMT"));

  @Test
  public void testFindColumn() throws Exception {
    try (Cursor cursor = new SimpleCursor(
        QueryResult.newBuilder().addFields(Field.newBuilder().setName("col1").build())
            .addFields(Field.newBuilder().setName("COL2").build()) // case-insensitive
            .addFields(Field.newBuilder().setName("col1").build()) // duplicate
            .addFields(Field.newBuilder().setName("col4").build()) // skip duplicate
            .build())) {
      Assert.assertEquals(1, cursor.findColumn("col1")); // should return first col1
      Assert.assertEquals(2, cursor.findColumn("Col2")); // should be case-insensitive
      Assert.assertEquals(4, cursor.findColumn("col4")); // index should skip over duplicate
    }
  }

  @Test
  public void testGetInt() throws Exception {
    List<Query.Type> types = Arrays.asList(Query.Type.INT8, Query.Type.UINT8, Query.Type.INT16,
        Query.Type.UINT16, Query.Type.INT24, Query.Type.UINT24, Query.Type.INT32);
    for (Query.Type type : types) {
      try (Cursor cursor = new SimpleCursor(QueryResult.newBuilder()
          .addFields(Field.newBuilder().setName("col1").setType(type).build())
          .addFields(Field.newBuilder().setName("null").setType(type).build())
          .addRows(Query.Row.newBuilder().addLengths("12345".length()).addLengths(-1) // SQL NULL
              .setValues(ByteString.copyFromUtf8("12345")))
          .build())) {
        Row row = cursor.next();
        Assert.assertNotNull(row);
        Assert.assertEquals(12345, row.getInt("col1"));
        Assert.assertFalse(row.wasNull());
        Assert.assertEquals(0, row.getInt("null"));
        Assert.assertTrue(row.wasNull());
        Assert.assertEquals(null, row.getObject("null", Integer.class));
        Assert.assertTrue(row.wasNull());
      }
    }
  }

  @Test
  public void testGetULong() throws Exception {
    try (Cursor cursor = new SimpleCursor(QueryResult.newBuilder()
        .addFields(Field.newBuilder().setName("col1").setType(Query.Type.UINT64).build())
        .addFields(Field.newBuilder().setName("null").setType(Query.Type.UINT64).build())
        .addRows(Query.Row.newBuilder().addLengths("18446744073709551615".length()).addLengths(-1) // SQL
                                                                                                   // NULL
            .setValues(ByteString.copyFromUtf8("18446744073709551615")))
        .build())) {
      Row row = cursor.next();
      Assert.assertNotNull(row);
      Assert.assertEquals(UnsignedLong.fromLongBits(-1), row.getULong("col1"));
      Assert.assertFalse(row.wasNull());
      Assert.assertEquals(null, row.getULong("null"));
      Assert.assertTrue(row.wasNull());
    }
  }

  @Test
  public void testGetString() throws Exception {
    List<Query.Type> types = Arrays.asList(Query.Type.ENUM, Query.Type.SET);
    for (Query.Type type : types) {
      try (Cursor cursor = new SimpleCursor(QueryResult.newBuilder()
          .addFields(Field.newBuilder().setName("col1").setType(type).build())
          .addFields(Field.newBuilder().setName("null").setType(type).build())
          .addRows(Query.Row.newBuilder().addLengths("val123".length()).addLengths(-1) // SQL NULL
              .setValues(ByteString.copyFromUtf8("val123")))
          .build())) {
        Row row = cursor.next();
        Assert.assertNotNull(row);
        Assert.assertEquals("val123", row.getString("col1"));
        Assert.assertFalse(row.wasNull());
        Assert.assertEquals(null, row.getString("null"));
        Assert.assertTrue(row.wasNull());
      }
    }
  }

  @Test
  public void testGetLong() throws Exception {
    List<Query.Type> types = Arrays.asList(Query.Type.UINT32, Query.Type.INT64);
    for (Query.Type type : types) {
      try (Cursor cursor = new SimpleCursor(QueryResult.newBuilder()
          .addFields(Field.newBuilder().setName("col1").setType(type).build())
          .addFields(Field.newBuilder().setName("null").setType(type).build())
          .addRows(Query.Row.newBuilder().addLengths("12345".length()).addLengths(-1) // SQL NULL
              .setValues(ByteString.copyFromUtf8("12345")))
          .build())) {
        Row row = cursor.next();
        Assert.assertNotNull(row);
        Assert.assertEquals(12345L, row.getLong("col1"));
        Assert.assertFalse(row.wasNull());
        Assert.assertEquals(0L, row.getLong("null"));
        Assert.assertTrue(row.wasNull());
        Assert.assertEquals(null, row.getObject("null", Long.class));
        Assert.assertTrue(row.wasNull());
      }
    }
  }

  @Test
  public void testGetDouble() throws Exception {
    try (Cursor cursor = new SimpleCursor(QueryResult.newBuilder()
        .addFields(Field.newBuilder().setName("col1").setType(Query.Type.FLOAT64).build())
        .addFields(Field.newBuilder().setName("null").setType(Query.Type.FLOAT64).build())
        .addRows(Query.Row.newBuilder().addLengths("2.5".length()).addLengths(-1) // SQL NULL
            .setValues(ByteString.copyFromUtf8("2.5")))
        .build())) {
      Row row = cursor.next();
      Assert.assertNotNull(row);
      Assert.assertEquals(2.5, row.getDouble("col1"), 0.01);
      Assert.assertFalse(row.wasNull());
      Assert.assertEquals(0.0, row.getDouble("null"), 0.0);
      Assert.assertTrue(row.wasNull());
      Assert.assertEquals(null, row.getObject("null", Double.class));
      Assert.assertTrue(row.wasNull());
    }
  }

  @Test
  public void testGetFloat() throws Exception {
    try (Cursor cursor = new SimpleCursor(QueryResult.newBuilder()
        .addFields(Field.newBuilder().setName("col1").setType(Query.Type.FLOAT32).build())
        .addFields(Field.newBuilder().setName("null").setType(Query.Type.FLOAT32).build())
        .addRows(Query.Row.newBuilder().addLengths("2.5".length()).addLengths(-1) // SQL NULL
            .setValues(ByteString.copyFromUtf8("2.5")))
        .build())) {
      Row row = cursor.next();
      Assert.assertNotNull(row);
      Assert.assertEquals(2.5f, row.getFloat("col1"), 0.01f);
      Assert.assertFalse(row.wasNull());
      Assert.assertEquals(0.0f, row.getFloat("null"), 0.0f);
      Assert.assertTrue(row.wasNull());
      Assert.assertEquals(null, row.getObject("null", Float.class));
      Assert.assertTrue(row.wasNull());
    }
  }

  @Test
  public void testGetDate() throws Exception {
    List<Query.Type> types = Arrays.asList(Query.Type.DATE);
    for (Query.Type type : types) {
      try (Cursor cursor = new SimpleCursor(QueryResult.newBuilder()
          .addFields(Field.newBuilder().setName("col1").setType(type).build())
          .addFields(Field.newBuilder().setName("null").setType(type).build())
          .addRows(Query.Row.newBuilder().addLengths("2008-01-02".length()).addLengths(-1) // SQL
                                                                                           // NULL
              .setValues(ByteString.copyFromUtf8("2008-01-02")))
          .build())) {
        Row row = cursor.next();
        Assert.assertNotNull(row);
        Assert.assertEquals(Date.valueOf("2008-01-02"), row.getObject("col1"));
        Assert.assertEquals(Date.valueOf("2008-01-02"), row.getDate("col1"));
        Assert.assertEquals(new Date(1199232000000L), row.getDate("col1", GMT));
        Assert.assertFalse(row.wasNull());
        Assert.assertEquals(null, row.getDate("null"));
        Assert.assertTrue(row.wasNull());
      }
    }
  }

  @Test
  public void testGetTime() throws Exception {
    try (Cursor cursor = new SimpleCursor(QueryResult.newBuilder()
        .addFields(Field.newBuilder().setName("col1").setType(Query.Type.TIME).build())
        .addFields(Field.newBuilder().setName("null").setType(Query.Type.TIME).build())
        .addRows(Query.Row.newBuilder().addLengths("12:34:56".length()).addLengths(-1) // SQL NULL
            .setValues(ByteString.copyFromUtf8("12:34:56")))
        .build())) {
      Row row = cursor.next();
      Assert.assertNotNull(row);
      Assert.assertEquals(Time.valueOf("12:34:56"), row.getObject("col1"));
      Assert.assertEquals(Time.valueOf("12:34:56"), row.getTime("col1"));
      Assert.assertEquals(new Time(45296000L), row.getTime("col1", GMT));
      Assert.assertFalse(row.wasNull());
      Assert.assertEquals(null, row.getTime("null"));
      Assert.assertTrue(row.wasNull());
    }
  }

  @Test
  public void testGetTimestamp() throws Exception {
    List<Query.Type> types = Arrays.asList(Query.Type.DATETIME, Query.Type.TIMESTAMP);
    for (Query.Type type : types) {
      try (Cursor cursor = new SimpleCursor(QueryResult.newBuilder()
          .addFields(Field.newBuilder().setName("col1").setType(type).build())
          .addFields(Field.newBuilder().setName("null").setType(type).build())
          .addRows(Query.Row.newBuilder().addLengths("2008-01-02 14:15:16.123456".length())
              .addLengths(-1) // SQL NULL
              .setValues(ByteString.copyFromUtf8("2008-01-02 14:15:16.123456")))
          .build())) {
        Row row = cursor.next();
        Assert.assertNotNull(row);
        Assert.assertEquals(Timestamp.valueOf("2008-01-02 14:15:16.123456"), row.getObject("col1"));
        Assert.assertEquals(Timestamp.valueOf("2008-01-02 14:15:16.123456"),
            row.getTimestamp("col1"));
        Timestamp ts = new Timestamp(1199283316000L);
        ts.setNanos(123456000);
        Assert.assertEquals(ts, row.getTimestamp("col1", GMT));
        Assert.assertFalse(row.wasNull());
        Assert.assertEquals(null, row.getTimestamp("null"));
        Assert.assertTrue(row.wasNull());
      }
    }
  }

  @Test
  public void testGetBytes() throws Exception {
    List<Query.Type> types = Arrays.asList(Query.Type.TEXT, Query.Type.BLOB, Query.Type.VARCHAR,
        Query.Type.VARBINARY, Query.Type.CHAR, Query.Type.BINARY, Query.Type.BIT);
    for (Query.Type type : types) {
      try (Cursor cursor = new SimpleCursor(QueryResult.newBuilder()
          .addFields(Field.newBuilder().setName("col1").setType(type).build())
          .addFields(Field.newBuilder().setName("null").setType(type).build())
          .addRows(Query.Row.newBuilder().addLengths("hello world".length()).addLengths(-1) // SQL
                                                                                            // NULL
              .setValues(ByteString.copyFromUtf8("hello world")))
          .build())) {
        Row row = cursor.next();
        Assert.assertNotNull(row);
        Assert.assertArrayEquals("hello world".getBytes("UTF-8"), row.getBytes("col1"));
        Assert.assertFalse(row.wasNull());
        Assert.assertEquals(null, row.getBytes("null"));
        Assert.assertTrue(row.wasNull());
      }
    }
  }

  @Test
  public void testGetBigDecimal() throws Exception {
    List<Query.Type> types = Arrays.asList(Query.Type.DECIMAL);
    for (Query.Type type : types) {
      try (Cursor cursor = new SimpleCursor(QueryResult.newBuilder()
          .addFields(Field.newBuilder().setName("col1").setType(type).build())
          .addFields(Field.newBuilder().setName("null").setType(type).build())
          .addRows(Query.Row.newBuilder().addLengths("1234.56789".length()).addLengths(-1) // SQL
                                                                                           // NULL
              .setValues(ByteString.copyFromUtf8("1234.56789")))
          .build())) {
        Row row = cursor.next();
        Assert.assertNotNull(row);
        Assert.assertEquals(new BigDecimal(BigInteger.valueOf(123456789), 5),
            row.getBigDecimal("col1"));
        Assert.assertFalse(row.wasNull());
        Assert.assertEquals(null, row.getBigDecimal("null"));
        Assert.assertTrue(row.wasNull());
      }
    }
  }

  @Test
  public void testGetShort() throws Exception {
    try (Cursor cursor = new SimpleCursor(QueryResult.newBuilder()
        .addFields(Field.newBuilder().setName("col1").setType(Query.Type.YEAR).build())
        .addFields(Field.newBuilder().setName("null").setType(Query.Type.YEAR).build())
        .addRows(Query.Row.newBuilder().addLengths("1234".length()).addLengths(-1) // SQL NULL
            .setValues(ByteString.copyFromUtf8("1234")))
        .build())) {
      Row row = cursor.next();
      Assert.assertNotNull(row);
      Assert.assertEquals(1234, row.getShort("col1"));
      Assert.assertFalse(row.wasNull());
      Assert.assertEquals(0, row.getShort("null"));
      Assert.assertTrue(row.wasNull());
      Assert.assertEquals(null, row.getObject("null", Short.class));
      Assert.assertTrue(row.wasNull());
    }
  }

  @Test
  public void testGetRawValue() throws Exception {
    try (Cursor cursor = new SimpleCursor(QueryResult.newBuilder()
        .addFields(Field.newBuilder().setName("col1").setType(Query.Type.INT32).build())
        .addRows(Query.Row.newBuilder().addLengths("12345".length())
            .setValues(ByteString.copyFromUtf8("12345")))
        .build())) {
      Row row = cursor.next();
      Assert.assertNotNull(row);
      Assert.assertEquals(ByteString.copyFromUtf8("12345"), row.getRawValue("col1"));
      Assert.assertEquals(ByteString.copyFromUtf8("12345"), row.getRawValue(1));
    }
  }

  @Test
  public void testNull() throws Exception {
    try (Cursor cursor = new SimpleCursor(QueryResult.newBuilder()
        .addFields(Field.newBuilder().setName("null").setType(Query.Type.NULL_TYPE).build())
        .addRows(Query.Row.newBuilder().addLengths(-1) // SQL NULL
        ).build())) {
      Row row = cursor.next();
      Assert.assertNotNull(row);
      Assert.assertEquals(null, row.getObject("null"));
      Assert.assertTrue(row.wasNull());
    }
  }
}
