package com.youtube.vitess.vtgate;

import com.google.common.collect.Lists;
import com.google.common.primitives.UnsignedLong;

import com.youtube.vitess.vtgate.Field.FieldType;
import com.youtube.vitess.vtgate.Field.Flag;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.math.BigDecimal;
import java.util.List;



@RunWith(JUnit4.class)
public class FieldTest {
  @Test
  public void testDouble() {
    List<FieldType> typesToTest = Lists.newArrayList(FieldType.VT_DOUBLE);
    String val = "1000.01";
    for (FieldType type : typesToTest) {
      Field f = Field.newFieldForTest(type, Flag.VT_ZEROVALUE_FLAG);
      Object o = f.convert(val.getBytes());
      Assert.assertEquals(Double.class, o.getClass());
      Assert.assertEquals(1000.01, ((Double) o).doubleValue(), 0.01);
    }
  }

  @Test
  public void testBigDecimal() {
    List<FieldType> typesToTest = Lists.newArrayList(FieldType.VT_DECIMAL, FieldType.VT_NEWDECIMAL);
    String val = "1000.01";
    for (FieldType type : typesToTest) {
      Field f = Field.newFieldForTest(type, Flag.VT_ZEROVALUE_FLAG);
      Object o = f.convert(val.getBytes());
      Assert.assertEquals(BigDecimal.class, o.getClass());
      Assert.assertEquals(1000.01, ((BigDecimal) o).doubleValue(), 0.01);
    }
  }

  @Test
  public void testInteger() {
    List<FieldType> typesToTest =
        Lists.newArrayList(FieldType.VT_TINY, FieldType.VT_SHORT, FieldType.VT_INT24);
    String val = "1000";
    for (FieldType type : typesToTest) {
      Field f = Field.newFieldForTest(type, Flag.VT_ZEROVALUE_FLAG);
      Object o = f.convert(val.getBytes());
      Assert.assertEquals(Integer.class, o.getClass());
      Assert.assertEquals(1000, ((Integer) o).intValue());
    }
  }

  @Test
  public void testLong_LONG() {
    String val = "1000";
    Field f = Field.newFieldForTest(FieldType.VT_LONG, Flag.VT_ZEROVALUE_FLAG);
    Object o = f.convert(val.getBytes());
    Assert.assertEquals(Long.class, o.getClass());
    Assert.assertEquals(1000L, ((Long) o).longValue());
  }

  @Test
  public void testLong_LONGLONG() {
    String val = "10000000000000";
    Field f = Field.newFieldForTest(FieldType.VT_LONGLONG, Flag.VT_ZEROVALUE_FLAG);
    Object o = f.convert(val.getBytes());
    Assert.assertEquals(Long.class, o.getClass());
    Assert.assertEquals(10000000000000L, ((Long) o).longValue());
  }

  @Test
  public void testLong_LONGLONG_UNSIGNED() {
    String val = "10000000000000";
    Field f = Field.newFieldForTest(FieldType.VT_LONGLONG, Flag.VT_UNSIGNED_FLAG);
    Object o = f.convert(val.getBytes());
    Assert.assertEquals(UnsignedLong.class, o.getClass());
    Assert.assertEquals(10000000000000L, ((UnsignedLong) o).longValue());
  }

  @Test
  public void testNull() {
    Field f = Field.newFieldForTest(FieldType.VT_NULL, Flag.VT_ZEROVALUE_FLAG);
    Object o = f.convert(null);
    Assert.assertEquals(null, o);
  }

  @Test
  public void testFloat() {
    String val = "1000.01";
    Field f = Field.newFieldForTest(FieldType.VT_FLOAT, Flag.VT_ZEROVALUE_FLAG);
    Object o = f.convert(val.getBytes());
    Assert.assertEquals(Float.class, o.getClass());
    Assert.assertEquals(1000.01, ((Float) o).floatValue(), 0.1);
  }
}
