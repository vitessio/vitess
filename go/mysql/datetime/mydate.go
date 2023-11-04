/*
Copyright 2023 The Vitess Authors.

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

// mysqlDayNumber converts a date into an absolute day number.
// This is an algorithm that has been reverse engineered from MySQL;
// the tables used as a reference can be found in `testdata/year_to_daynr.json`.
// It is worth noting that this absolute day number does not match the
// day count that traditional datetime systems use (e.g. the daycount
// algorithm in the Go standard library). It is often off by one, possibly
// because of incorrect leap year handling, but the inverse of the algorithm
// in mysqlDateFromDayNumber takes this into account. Hence, the results
// of this function can ONLY be passed to mysqlDateFromDayNumber; using
// a day number with one of Go's datetime APIs will return incorrect results.
// This API should only be used when performing datetime calculations (addition
// and subtraction), so that the results match MySQL's. All other date handling
// operations must use our helpers based on Go's standard library.
func mysqlDayNumber(year, month, day int) int {
	if year == 0 && month == 0 {
		return 0
	}

	days := 365*year + 31*(month-1) + day
	switch month {
	case 1, 2:
		year = year - 1
	default:
		days = days - (month*4+23)/10
	}

	leapAdjust := ((year/100 + 1) * 3) / 4
	return days + year/4 - leapAdjust
}

// mysqlDateFromDayNumber converts an absolute day number into a date (a year, month, day triplet).
// This is an algorithm that has been reverse engineered from MySQL;
// the tables used as a reference can be found in `testdata/daynr_to_date.json`.
// See the warning from mysqlDayNumber: the day number used as an argument to
// this function must come from mysqlDayNumber or the results won't be correct.
// This API should only be used when performing datetime calculations (addition
// and subtraction), so that the results match MySQL's. All other date handling
// operations must use our helpers based on Go's standard library.
func mysqlDateFromDayNumber(daynr int) (uint16, uint8, uint8) {
	if daynr <= 365 || daynr >= 3652500 {
		return 0, 0, 0
	}

	year := daynr * 100 / 36525
	leapAdjust := (((year-1)/100 + 1) * 3) / 4
	yday := (daynr - year*365) - (year-1)/4 + leapAdjust

	if diy := daysInYear(year); yday > diy {
		yday -= diy
		year++
	}

	daycount := daysInMonth
	if isLeap(year) {
		daycount = daysInMonthLeap
	}
	for month, dim := range daycount {
		if yday <= dim {
			return uint16(year), uint8(month + 1), uint8(yday)
		}
		yday -= dim
	}

	panic("unreachable: yday is too large?")
}
