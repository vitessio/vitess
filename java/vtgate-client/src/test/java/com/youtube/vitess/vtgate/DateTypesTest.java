package com.youtube.vitess.vtgate;

import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.text.ParseException;

@RunWith(JUnit4.class)
public class DateTypesTest {
  @Test
  public void testDateTimes() throws Exception {
    DateTime dt = DateTime.now();
    byte[] bytes = BindVariable.forDateTime("", dt).getByteArrayVal();
    check(FieldType.VT_TIMESTAMP, dt, bytes);
    check(FieldType.VT_DATETIME, dt, bytes);
  }

  @Test
  public void testDate() throws Exception {
    DateTime now = DateTime.now();
    byte[] bytes = BindVariable.forDate("", now).getByteArrayVal();
    DateTime date =
        now.withMillisOfSecond(0).withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0);
    check(FieldType.VT_DATE, date, bytes);
    check(FieldType.VT_NEWDATE, date, bytes);
  }

  @Test
  public void testTime() throws Exception {
    DateTime now = DateTime.now();
    byte[] bytes = BindVariable.forTime("", now).getByteArrayVal();
    DateTime time = now.withMillisOfSecond(0).withYear(1970).withMonthOfYear(1).withDayOfMonth(1);
    check(FieldType.VT_TIME, time, bytes);
  }

  private void check(FieldType typeUnderTest, DateTime dt, byte[] bytes) throws ParseException {
    Object o = typeUnderTest.convert(bytes);
    Assert.assertEquals(DateTime.class, o.getClass());
    Assert.assertEquals(dt, (DateTime) o);
  }
}
