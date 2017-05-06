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

package io.vitess.mysql;

import com.google.common.math.IntMath;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * Utility methods for processing MySQL TIME, DATE, DATETIME, and TIMESTAMP.
 *
 * <p>These provide functionality similar to {@code valueOf()} and {@code toString()}
 * in {@link java.sql.Date} et al. The difference is that these support MySQL-specific
 * syntax like fractional seconds, negative times, and hours > 24 for elapsed time.
 */
public class DateTime {
  private static final String DATE_FORMAT = "yyyy-MM-dd";
  private static final String DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";

  private static final long SECONDS_TO_MILLIS = 1000L;
  private static final long MINUTES_TO_MILLIS = 60L * SECONDS_TO_MILLIS;
  private static final long HOURS_TO_MILLIS = 60L * MINUTES_TO_MILLIS;

  /**
   * Parse a MySQL DATE format into a {@link Date} with the default time zone.
   *
   * <p>This should match {@link Date#valueOf(String)}.
   */
  public static Date parseDate(String value) throws ParseException {
    return parseDate(value, Calendar.getInstance());
  }

  /**
   * Parse a MySQL DATE format into a {@link Date} with the given {@link Calendar}.
   */
  public static Date parseDate(String value, Calendar cal) throws ParseException {
    DateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
    dateFormat.setCalendar(cal);
    return new Date(dateFormat.parse(value).getTime());
  }

  /**
   * Format a {@link Date} as a MySQL DATE with the default time zone.
   *
   * <p>This should match {@link Date#toString()}.
   */
  public static String formatDate(Date value) {
    return formatDate(value, Calendar.getInstance());
  }

  /**
   * Format a {@link Date} as a MySQL DATE with the given {@link Calendar}.
   */
  public static String formatDate(Date value, Calendar cal) {
    DateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
    dateFormat.setCalendar(cal);
    return dateFormat.format(value);
  }

  /**
   * Parse a MySQL TIME format into a {@link Time} with the default time zone.
   *
   * <p>This should match {@link Time#valueOf(String)} for the format it supports.
   * For MySQL-specific syntax, this will succeed where {@code valueOf()} fails.
   */
  public static Time parseTime(String value) throws ParseException {
    return parseTime(value, Calendar.getInstance());
  }

  /**
   * Parse a MySQL TIME format into a {@link Time} with the given {@link Calendar}.
   *
   * <p>The range for TIME values is '-838:59:59.000000' to '838:59:59.000000'
   * <a href="http://dev.mysql.com/doc/refman/5.6/en/time.html">[1]</a>.
   *
   * <p>Note that this is meant to parse only valid values, assumed to be
   * returned by MySQL. Results are undefined for invalid input.
   */
  public static Time parseTime(String value, Calendar cal) throws ParseException {
    // MySQL TIME can be negative and have hours > 24,
    // because it can also represent elapsed time.
    // So we just parse the integers rather than using DateFormat.
    long hours = 0;
    long minutes = 0;
    long seconds = 0;
    long millis = 0;

    try {
      // Hours
      int sepIndex1 = value.indexOf(':');
      if (sepIndex1 == -1) {
        throw new ParseException("Invalid MySQL TIME format: " + value, value.length());
      }
      hours = Long.parseLong(value.substring(0, sepIndex1));

      // Minutes and seconds
      int sepIndex2 = value.indexOf(':', sepIndex1 + 1);
      if (sepIndex2 == -1) {
        // There's no seconds.
        minutes = Long.parseLong(value.substring(sepIndex1 + 1));
      } else {
        minutes = Long.parseLong(value.substring(sepIndex1 + 1, sepIndex2));

        // Fractional seconds
        int dotIndex = value.lastIndexOf('.');
        if (dotIndex == -1) {
          // There's no fraction.
          seconds = Long.parseLong(value.substring(sepIndex2 + 1));
        } else {
          seconds = Long.parseLong(value.substring(sepIndex2 + 1, dotIndex));

          String fraction = value.substring(dotIndex + 1, value.length());

          if (fraction.length() > 0) {
            // Convert the fraction to millis.
            if (fraction.length() > 3) {
              fraction = fraction.substring(0, 3);
            }
            millis = Long.parseLong(fraction) * IntMath.pow(10, 3 - fraction.length());
          }
        }
      }
    } catch (NumberFormatException e) {
      throw new ParseException("Invalid MYSQL TIME format: " + value, 0);
    }

    if (hours < 0) {
      // If hours are negative, make the whole thing negative.
      minutes = -minutes;
      seconds = -seconds;
      millis = -millis;
    }

    // Convert to millis since epoch (UTC).
    long time =
        hours * HOURS_TO_MILLIS
            + minutes * MINUTES_TO_MILLIS
            + seconds * SECONDS_TO_MILLIS
            + millis;
    // Adjust time zone.
    time -= cal.get(Calendar.ZONE_OFFSET);
    return new Time(time);
  }

  /**
   * Format a {@link Time} as a MySQL TIME with the default time zone.
   *
   * <p>This should match {@link Time#toString()} for the values it supports.
   * For MySQL-specific syntax (like fractional seconds, negative times,
   * and hours > 24) the results will differ.
   */
  public static String formatTime(Time value) {
    return formatTime(value, Calendar.getInstance());
  }

  /**
   * Format a {@link Time} as a MySQL TIME with the given {@link Calendar}.
   *
   * <p>The range for TIME values is '-838:59:59.000000' to '838:59:59.000000'
   * <a href="http://dev.mysql.com/doc/refman/5.6/en/time.html">[1]</a>.
   * We don't enforce that range, but we do print >24 hours rather than
   * wrapping around to the next day.
   */
  public static String formatTime(Time value, Calendar cal) {
    long millis = value.getTime();

    // Adjust for time zone.
    millis += cal.get(Calendar.ZONE_OFFSET);

    String sign = "";
    if (millis < 0) {
      sign = "-";
      millis = -millis;
    }

    long hours = millis / HOURS_TO_MILLIS;
    millis -= hours * HOURS_TO_MILLIS;
    long minutes = millis / MINUTES_TO_MILLIS;
    millis -= minutes * MINUTES_TO_MILLIS;
    long seconds = millis / SECONDS_TO_MILLIS;
    millis -= seconds * SECONDS_TO_MILLIS;

    if (millis == 0) {
      return String.format("%s%02d:%02d:%02d", sign, hours, minutes, seconds);
    } else {
      return String.format("%s%02d:%02d:%02d.%03d", sign, hours, minutes, seconds, millis);
    }
  }

  /**
   * Parse a MySQL TIMESTAMP format into a {@link Timestamp} with the default time zone.
   *
   * <p>This should match {@link Timestamp#valueOf(String)} for the format it supports.
   * For MySQL-specific syntax, this will succeed where {@code valueOf()} fails.
   */
  public static Timestamp parseTimestamp(String value) throws ParseException {
    return parseTimestamp(value, Calendar.getInstance());
  }

  /**
   * Parse a MySQL TIMESTAMP format into a {@link Timestamp} with the given {@link Calendar}.
   *
   * <p>The format is 'YYYY-MM-DD HH:MM:SS[.fraction]' where the fraction
   * can be up to 6 digits.
   * <a href="http://dev.mysql.com/doc/refman/5.6/en/datetime.html">[1]</a>.
   *
   * <p>Note that this is meant to parse only valid values, assumed to be
   * returned by MySQL. Results are undefined for invalid input.
   */
  public static Timestamp parseTimestamp(String value, Calendar cal) throws ParseException {
    // Value to pass to Timestamp.setNanos().
    int nanos = 0;

    // Strip the fraction (if any) before using DateFormat.
    // Timestamp stores second-level precision separate from the fraction.
    int dotIndex = value.lastIndexOf('.');
    if (dotIndex != -1) {
      String fraction = value.substring(dotIndex + 1, value.length());

      if (fraction.length() > 0) {
        // Convert the fraction to nanoseconds.
        if (fraction.length() > 9) {
          fraction = fraction.substring(0, 9);
        }
        try {
          nanos = Integer.parseInt(fraction) * IntMath.pow(10, 9 - fraction.length());
        } catch (NumberFormatException e) {
          throw new ParseException("Invalid MySQL TIMESTAMP format: " + value, dotIndex + 1);
        }
      }

      value = value.substring(0, dotIndex);
    }

    // Parse with second precision, then add nanos.
    DateFormat dateFormat = new SimpleDateFormat(DATETIME_FORMAT);
    dateFormat.setCalendar(cal);
    Timestamp result = new Timestamp(dateFormat.parse(value).getTime());
    result.setNanos(nanos);
    return result;
  }

  /**
   * Format a {@link Timestamp} as a MySQL TIMESTAMP with the default time zone.
   *
   * <p>This should match {@link Timestamp#toString()} for the values it supports.
   * For MySQL-specific syntax, the results will differ.
   */
  public static String formatTimestamp(Timestamp value) {
    return formatTimestamp(value, Calendar.getInstance());
  }

  /**
   * Format a {@link Timestamp} as a MySQL TIMESTAMP with the given {@link Calendar}.
   */
  public static String formatTimestamp(Timestamp value, Calendar cal) {
    // The java.util.Date portion of a Timestamp only contains second-level precision.
    // When printing the nanos, limit to microseconds since that's all MySQL allows.
    long millis = value.getNanos() / 1000;
    DateFormat dateFormat = new SimpleDateFormat(DATETIME_FORMAT);
    dateFormat.setCalendar(cal);
    String dateString = dateFormat.format(value);
    if (millis == 0) {
      // For whole numbered seconds, we add ".0" to match Timestamp.toString().
      return dateString + ".0";
    } else {
      // Otherwise, we print the full fraction, then trim off trailing zeros.
      // This is also done to match Timestamp.toString().
      String result = String.format("%s.%06d", dateString, millis);
      int end = result.length();
      while (end > 0 && result.charAt(end - 1) == '0') {
        --end;
      }
      return result.substring(0, end);
    }
  }
}
