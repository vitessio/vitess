package com.youtube.vitess.vtgate;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.junit.Assert;
import org.junit.Test;

public class DateTypesTest {
	@Test
	public void testDateTimes() throws Exception {
		String val = "2013-12-01 09:23:10.234";
		SimpleDateFormat formatter = new SimpleDateFormat(
				"yyyy-MM-dd HH:mm:ss.SSS");
		check(FieldType.VT_TIMESTAMP, formatter, val);
		check(FieldType.VT_DATETIME, formatter, val);
	}

	@Test
	public void testDate() throws Exception {
		String val = "2013-12-01";
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
		check(FieldType.VT_DATE, formatter, val);
	}

	@Test
	public void testTime() throws Exception {
		String val = "09:23:10";
		SimpleDateFormat formatter = new SimpleDateFormat("HH:mm:ss");
		check(FieldType.VT_TIME, formatter, val);
	}

	private void check(FieldType typeUnderTest, SimpleDateFormat formatter,
			String val) throws ParseException {
		Date date = formatter.parse(val);
		Object o = typeUnderTest.convert(val);
		Assert.assertEquals(Date.class, o.getClass());
		Assert.assertEquals(date, (Date) o);
	}
}
