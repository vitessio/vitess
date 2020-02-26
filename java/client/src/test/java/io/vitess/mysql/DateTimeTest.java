/*
 * Copyright 2019 The Vitess Authors.

 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

 *     http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.vitess.mysql;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DateTimeTest {

  private static final Calendar GMT = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
  private static final Calendar PST = Calendar.getInstance(TimeZone.getTimeZone("GMT-8"));
  private static final Calendar IST = Calendar.getInstance(TimeZone.getTimeZone("GMT+0530"));

  private static final ImmutableMap<String, Date> TEST_DATES =
      new ImmutableMap.Builder<String, Date>()
          .put("1970-01-01", new Date(0L))
          .put("2008-01-02", new Date(1199232000000L))
          .build();

  private static Timestamp makeTimestamp(long millis, int nanos) {
    Timestamp ts = new Timestamp(millis);
    ts.setNanos(nanos);
    return ts;
  }

  @Test
  public void testParseDate() throws Exception {
    // Check that our default time zone matches valueOf().
    for (String dateString : TEST_DATES.keySet()) {
      assertEquals(dateString, Date.valueOf(dateString), DateTime.parseDate(dateString));
    }
  }

  @Test
  public void testFormatDate() throws Exception {
    // Check that our default time zone matches toString().
    for (Date date : TEST_DATES.values()) {
      assertEquals(date.toString(), DateTime.formatDate(date));
    }
  }

  @Test
  public void testParseDateGMT() throws Exception {
    // Test absolute UNIX epoch values in GMT.
    for (Map.Entry<String, Date> entry : TEST_DATES.entrySet()) {
      String dateString = entry.getKey();
      Date date = entry.getValue();
      assertEquals(dateString, date, DateTime.parseDate(dateString, GMT));
    }
  }

  @Test
  public void testFormatDateGMT() throws Exception {
    // Test absolute UNIX epoch values in GMT.
    for (Map.Entry<String, Date> entry : TEST_DATES.entrySet()) {
      String dateString = entry.getKey();
      Date date = entry.getValue();
      assertEquals(dateString, DateTime.formatDate(date, GMT));
    }
  }

  @Test
  public void testParseTime() throws Exception {
    // Check that our default time zone matches valueOf().
    // We can only check syntax that Time.valueOf() supports.
    final List<String> TEST_TIMES =
        new ImmutableList.Builder<String>().add("00:00:00").add("01:23:45").add("12:34:56").build();

    for (String timeString : TEST_TIMES) {
      assertEquals(timeString, Time.valueOf(timeString), DateTime.parseTime(timeString));
    }
  }

  @Test
  public void testFormatTime() throws Exception {
    // Check that our default time zone matches toString().
    // We can only check syntax that Time.toString() supports.
    final List<String> TEST_TIMES =
        new ImmutableList.Builder<String>().add("00:00:00").add("01:23:45").add("12:34:56").build();

    for (String timeString : TEST_TIMES) {
      Time time = Time.valueOf(timeString);
      assertEquals(timeString, time.toString(), DateTime.formatTime(time));
    }
  }

  @Test
  public void testParseTimeGMT() throws Exception {
    // Test absolute UNIX epoch values in GMT,
    // including MySQL-specific syntax that Time.valueOf() doesn't support.
    final Map<String, Time> TEST_TIMES =
        new ImmutableMap.Builder<String, Time>()
            .put("00:00:00", new Time(0))
            .put("12:34", new Time(45240000L))
            .put("01:23", new Time(4980000L))
            .put("1:23", new Time(4980000L))
            .put("12:34:56", new Time(45296000L))
            .put("-12:34:56", new Time(-45296000L))
            .put("812:34:56.", new Time(2925296000L))
            .put("812:34:56.123", new Time(2925296123L))
            .put("-812:34:56.123", new Time(-2925296123L))
            .put("812:34:56.123456", new Time(2925296123L))
            .build();

    for (Map.Entry<String, Time> entry : TEST_TIMES.entrySet()) {
      String timeString = entry.getKey();
      Time time = entry.getValue();
      assertEquals(time, DateTime.parseTime(timeString, GMT));
    }
  }

  @Test
  public void testParseTimePST() throws Exception {
    // Test absolute UNIX epoch values in a negative GMT offset.
    final Map<String, Time> TEST_TIMES =
        new ImmutableMap.Builder<String, Time>()
            .put("-08:00:00", new Time(0))
            .put("04:34:56", new Time(45296000L))
            .put("-20:34:56", new Time(-45296000L))
            .build();

    for (Map.Entry<String, Time> entry : TEST_TIMES.entrySet()) {
      String timeString = entry.getKey();
      Time time = entry.getValue();
      assertEquals(time, DateTime.parseTime(timeString, PST));
    }
  }

  @Test
  public void testParseTimeIST() throws Exception {
    // Test absolute UNIX epoch values in a positive GMT offset.
    final Map<String, Time> TEST_TIMES =
        new ImmutableMap.Builder<String, Time>()
            .put("05:30:00", new Time(0))
            .put("18:04:56", new Time(45296000L))
            .put("-07:04:56", new Time(-45296000L))
            .build();

    for (Map.Entry<String, Time> entry : TEST_TIMES.entrySet()) {
      String timeString = entry.getKey();
      Time time = entry.getValue();
      assertEquals(time, DateTime.parseTime(timeString, IST));
    }
  }

  @Test
  public void testFormatTimeGMT() throws Exception {
    // Test absolute UNIX epoch values in GMT,
    // including MySQL-specific syntax that Time.toString() doesn't support.
    final Map<String, Time> TEST_TIMES =
        new ImmutableMap.Builder<String, Time>()
            .put("00:00:00", new Time(0))
            .put("12:34:00", new Time(45240000L))
            .put("01:23:00", new Time(4980000L))
            .put("12:34:56", new Time(45296000L))
            .put("-01:23:00", new Time(-4980000L))
            .put("-12:34:56", new Time(-45296000L))
            .put("812:34:56", new Time(2925296000L))
            .put("-812:34:56", new Time(-2925296000L))
            .put("812:34:56.010", new Time(2925296010L))
            .build();

    for (Map.Entry<String, Time> entry : TEST_TIMES.entrySet()) {
      String timeString = entry.getKey();
      Time time = entry.getValue();
      assertEquals(timeString, DateTime.formatTime(time, GMT));
    }
  }

  @Test
  public void testFormatTimePST() throws Exception {
    // Test absolute UNIX epoch values in a negative GMT offset.
    final Map<String, Time> TEST_TIMES =
        new ImmutableMap.Builder<String, Time>()
            .put("-08:00:00", new Time(0))
            .put("04:34:56", new Time(45296000L))
            .put("-20:34:56", new Time(-45296000L))
            .build();

    for (Map.Entry<String, Time> entry : TEST_TIMES.entrySet()) {
      String timeString = entry.getKey();
      Time time = entry.getValue();
      assertEquals(timeString, DateTime.formatTime(time, PST));
    }
  }

  @Test
  public void testFormatTimeIST() throws Exception {
    // Test absolute UNIX epoch values in a positive GMT offset.
    final Map<String, Time> TEST_TIMES =
        new ImmutableMap.Builder<String, Time>()
            .put("05:30:00", new Time(0))
            .put("18:04:56", new Time(45296000L))
            .put("-07:04:56", new Time(-45296000L))
            .build();

    for (Map.Entry<String, Time> entry : TEST_TIMES.entrySet()) {
      String timeString = entry.getKey();
      Time time = entry.getValue();
      assertEquals(timeString, DateTime.formatTime(time, IST));
    }
  }

  @Test
  public void testParseTimestamp() throws Exception {
    // Check that our default time zone matches valueOf().
    final List<String> TEST_TIMESTAMPS =
        new ImmutableList.Builder<String>()
            .add("1970-01-01 00:00:00")
            .add("2008-01-02 14:15:16")
            .add("2008-01-02 14:15:16.123456")
            .build();

    for (String tsString : TEST_TIMESTAMPS) {
      assertEquals(tsString, Timestamp.valueOf(tsString), DateTime.parseTimestamp(tsString));
    }
  }

  @Test
  public void testFormatTimestamp() throws Exception {
    // Check that our default time zone matches toString().
    final List<String> TEST_TIMESTAMPS =
        new ImmutableList.Builder<String>()
            .add("1970-01-01 00:00:00")
            .add("2008-01-02 14:15:16")
            .add("2008-01-02 14:15:16.001")
            .add("2008-01-02 14:15:16.123456")
            .build();

    for (String tsString : TEST_TIMESTAMPS) {
      Timestamp ts = Timestamp.valueOf(tsString);
      assertEquals(tsString, ts.toString(), DateTime.formatTimestamp(ts));
    }
  }

  @Test
  public void testParseTimestampGMT() throws Exception {
    // Test absolute UNIX epoch values in GMT.
    final Map<String, Timestamp> TEST_TIMESTAMPS =
        new ImmutableMap.Builder<String, Timestamp>()
            .put("1970-01-01 00:00:00", makeTimestamp(0, 0))
            .put("2008-01-02 14:15:16", makeTimestamp(1199283316000L, 0))
            .put("2008-01-02 14:15:16.0", makeTimestamp(1199283316000L, 0))
            .put("2008-01-02 14:15:16.123", makeTimestamp(1199283316000L, 123000000))
            .put("2008-01-02 14:15:16.123456", makeTimestamp(1199283316000L, 123456000))
            .put("2008-01-02 14:15:16.123456789", makeTimestamp(1199283316000L, 123456789))
            .put("2008-01-02 14:15:16.1234567890123", makeTimestamp(1199283316000L, 123456789))
            .build();

    for (Map.Entry<String, Timestamp> entry : TEST_TIMESTAMPS.entrySet()) {
      String tsString = entry.getKey();
      Timestamp ts = entry.getValue();
      assertEquals(ts, DateTime.parseTimestamp(tsString, GMT));
    }
  }

  @Test
  public void testFormatTimestampGMT() throws Exception {
    // Test absolute UNIX epoch values in GMT.
    // This also tests MySQL-specific features, like truncating to microseconds.
    final Map<String, Timestamp> TEST_TIMESTAMPS =
        new ImmutableMap.Builder<String, Timestamp>()
            .put("1970-01-01 00:00:00.0", makeTimestamp(0, 0))
            .put("2008-01-02 14:15:16.0", makeTimestamp(1199283316000L, 0))
            .put("2008-01-02 14:15:16.001", makeTimestamp(1199283316000L, 1000000))
            .put("2008-01-02 14:15:16.123", makeTimestamp(1199283316000L, 123000000))
            .put("2008-01-02 14:15:16.123456", makeTimestamp(1199283316000L, 123456000))
            .put("2008-01-02 14:15:16.234567", makeTimestamp(1199283316000L, 234567891))
            .build();

    for (Map.Entry<String, Timestamp> entry : TEST_TIMESTAMPS.entrySet()) {
      String tsString = entry.getKey();
      Timestamp ts = entry.getValue();
      assertEquals(tsString, DateTime.formatTimestamp(ts, GMT));
    }
  }
}
