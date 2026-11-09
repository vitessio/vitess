/*
Copyright 2026 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package datetime

import "time"

// CheckFlags select the checks MySQL applies to the date part of a DATE or DATETIME
// value (check_date in MySQL's mysys/my_time.cc). MySQL derives them from the
// NO_ZERO_IN_DATE, NO_ZERO_DATE and ALLOW_INVALID_DATES sql_modes.
type CheckFlags uint8

const (
	// CheckNoZeroInDate rejects a value with a zero month or day, such as '2020-00-15',
	// unless the whole value is zero.
	CheckNoZeroInDate CheckFlags = 1 << iota
	// CheckNoZeroDate rejects the zero value, such as '0000-00-00'.
	CheckNoZeroDate
	// CheckAllowInvalidDates accepts a day beyond the last day of its month, up to 31,
	// such as '2020-02-31'.
	CheckAllowInvalidDates
)

// CheckDate reports whether MySQL accepts s as a DATE literal (DATE 's') under flags.
// It accepts the layouts ParseDate accepts and judges the date the way MySQL does,
// including values whose month or day is zero, which ParseDate cannot represent.
func CheckDate(s string, flags CheckFlags) bool {
	if _, ok := isNumber(s); ok {
		if len(s) >= 8 {
			return checkFormat(Date_YYYYMMDD, s, false, flags)
		}
		return checkFormat(Date_YYMMDD, s, true, flags)
	}
	if len(s) >= 8 && checkFormat(Date_YYYY_M_D, s, false, flags) {
		return true
	}
	return len(s) >= 6 && checkFormat(Date_YY_M_D, s, true, flags)
}

// CheckDateTime reports whether MySQL accepts s as a TIMESTAMP literal
// (TIMESTAMP 's') under flags. It accepts the layouts ParseDateTime accepts and judges
// the date part the way MySQL does, including values whose month or day is zero, which
// ParseDateTime cannot represent.
func CheckDateTime(s string, flags CheckFlags) bool {
	if sl, ok := isNumber(s); ok {
		if sl >= 14 {
			return checkFormat(DateTime_YYYYMMDDhhmmss, s, false, flags)
		}
		return checkFormat(DateTime_YYMMDDhhmmss, s, true, flags)
	}
	return checkFormat(DateTime_YYYY_M_D_h_m_s, s, false, flags) ||
		checkFormat(DateTime_YY_M_D_h_m_s, s, true, flags)
}

// checkFormat parses s with f and judges the result like MySQL's check_date.
// shortYear is set for layouts with a two-digit year, which the parser maps to a year
// from 1970 to 2069 even when the value is all zero; MySQL leaves the year of an
// all-zero value at zero, making it the zero value.
func checkFormat(f *Strftime, s string, shortYear bool, flags CheckFlags) bool {
	tp, rest, ok := f.parseParts(s)
	if !ok || len(rest) > 0 {
		return false
	}

	zeroYear := tp.year == 0 || (shortYear && tp.year == 2000)
	if zeroYear && tp.month == 0 && tp.day == 0 && tp.hour == 0 && tp.min == 0 && tp.sec == 0 && tp.nsec == 0 {
		return flags&CheckNoZeroDate == 0
	}
	if tp.month < 0 || tp.month > 12 || tp.day < 0 || tp.day > 31 {
		return false
	}
	if tp.month == 0 || tp.day == 0 {
		if flags&CheckNoZeroInDate != 0 {
			return false
		}
		// MySQL accepts the zero month or day: check the remaining fields on their own.
		return validRemainder(tp)
	}
	if tp.day > daysIn(time.Month(tp.month), tp.year) {
		if flags&CheckAllowInvalidDates == 0 {
			return false
		}
		// MySQL accepts the day beyond the end of the month: check the remaining
		// fields on their own.
		return validRemainder(tp)
	}
	_, _, ok = tp.toDateTime(-1)
	return ok
}

// validRemainder reports whether the fields of tp other than its month and day form a
// valid value, for a value whose month or day the caller has accepted itself.
func validRemainder(tp timeparts) bool {
	tp.month = 1
	tp.day = 1
	_, _, ok := tp.toDateTime(-1)
	return ok
}
