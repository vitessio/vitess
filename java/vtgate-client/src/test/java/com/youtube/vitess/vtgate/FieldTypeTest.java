package com.youtube.vitess.vtgate;

import com.google.common.collect.Lists;
import com.google.common.primitives.UnsignedLong;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.math.BigDecimal;
import java.util.List;

@RunWith(JUnit4.class)
public class FieldTypeTest {
  @Test
  public void testDouble() {
    List<FieldType> typesToTest = Lists.newArrayList(FieldType.VT_DOUBLE);
    String val = "1000.01";
    for (FieldType type : typesToTest) {
      Object o = type.convert(val.getBytes());
      Assert.assertEquals(Double.class, o.getClass());
      Assert.assertEquals(1000.01, ((Double) o).doubleValue(), 0.01);
    }
  }

  @Test
  public void testBigDecimal() {
    List<FieldType> typesToTest = Lists.newArrayList(FieldType.VT_DECIMAL, FieldType.VT_NEWDECIMAL);
    String val = "1000.01";
    for (FieldType type : typesToTest) {
      Object o = type.convert(val.getBytes());
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
      Object o = type.convert(val.getBytes());
      Assert.assertEquals(Integer.class, o.getClass());
      Assert.assertEquals(1000, ((Integer) o).intValue());
    }
  }

  @Test
  public void testLong() {
    String val = "1000";
    Object o = FieldType.VT_LONG.convert(val.getBytes());
    Assert.assertEquals(Long.class, o.getClass());
    Assert.assertEquals(1000L, ((Long) o).longValue());
  }

  @Test
  public void testNull() {
    Object o = FieldType.VT_NULL.convert(null);
    Assert.assertEquals(null, o);
  }

  @Test
  public void testFloat() {
    String val = "1000.01";
    Object o = FieldType.VT_FLOAT.convert(val.getBytes());
    Assert.assertEquals(Float.class, o.getClass());
    Assert.assertEquals(1000.01, ((Float) o).floatValue(), 0.1);
  }

  @Test
  public void testULong() {
    String val = "10000000000000";
    Object o = FieldType.VT_LONGLONG.convert(val.getBytes());
    Assert.assertEquals(UnsignedLong.class, o.getClass());
    Assert.assertEquals(10000000000000L, ((UnsignedLong) o).longValue());
  }
}
