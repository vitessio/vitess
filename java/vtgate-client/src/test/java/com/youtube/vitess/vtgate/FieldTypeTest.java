package com.youtube.vitess.vtgate;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.primitives.UnsignedLong;

public class FieldTypeTest {
	@Test
	public void testDouble() {
		List<FieldType> typesToTest = Lists.newArrayList(FieldType.VT_DECIMAL,
				FieldType.VT_DOUBLE, FieldType.VT_NEWDECIMAL);
		String val = "1000.01";
		for (FieldType type : typesToTest) {
			Object o = type.convert(val);
			Assert.assertEquals(Double.class, o.getClass());
			Assert.assertEquals(1000.01, ((Double) o).doubleValue(), 0.01);
		}
	}

	@Test
	public void testInteger() {
		List<FieldType> typesToTest = Lists.newArrayList(FieldType.VT_TINY,
				FieldType.VT_SHORT, FieldType.VT_INT24);
		String val = "1000";
		for (FieldType type : typesToTest) {
			Object o = type.convert(val);
			Assert.assertEquals(Integer.class, o.getClass());
			Assert.assertEquals(1000, ((Integer) o).intValue());
		}
	}

	@Test
	public void testLong() {
		String val = "1000";
		Object o = FieldType.VT_LONG.convert(val);
		Assert.assertEquals(Long.class, o.getClass());
		Assert.assertEquals(1000L, ((Long) o).longValue());
	}

	@Test
	public void testFloat() {
		String val = "1000.01";
		Object o = FieldType.VT_FLOAT.convert(val);
		Assert.assertEquals(Float.class, o.getClass());
		Assert.assertEquals(1000.01, ((Float) o).floatValue(), 0.1);
	}

	@Test
	public void testULong() {
		String val = "10000000000000";
		Object o = FieldType.VT_LONGLONG.convert(val);
		Assert.assertEquals(UnsignedLong.class, o.getClass());
		Assert.assertEquals(10000000000000L, ((UnsignedLong) o).longValue());
	}
}
