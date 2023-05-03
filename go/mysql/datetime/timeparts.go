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

import "time"

type timeparts struct {
	year  int
	month int
	day   int
	yday  int
	hour  int
	min   int
	sec   int
	nsec  int

	pmset bool
	amset bool

	prec uint8
}

func (tp *timeparts) toDateTime(prec int) (DateTime, int, bool) {
	if tp.isZero() {
		// zero date
		return DateTime{}, 0, true
	}

	if tp.pmset && tp.hour < 12 {
		tp.hour += 12
	} else if tp.amset && tp.hour == 12 {
		tp.hour = 0
	}
	if tp.yday > 0 {
		return DateTime{}, 0, false
	} else {
		if tp.month < 0 {
			tp.month = int(time.January)
		}
		if tp.day < 0 {
			tp.day = 1
		}
	}
	if tp.day < 1 || tp.day > daysIn(time.Month(tp.month), tp.year) {
		return DateTime{}, 0, false
	}

	dt := DateTime{
		Date: Date{
			year:  uint16(tp.year),
			month: uint8(tp.month),
			day:   uint8(tp.day),
		},
		Time: Time{
			hour:       uint16(tp.hour),
			minute:     uint8(tp.min),
			second:     uint8(tp.sec),
			nanosecond: uint32(tp.nsec),
		},
	}

	l := prec
	if prec < 0 {
		l = int(tp.prec)
	} else {
		dt = dt.Round(prec)
	}

	return dt, l, true
}

func (tp *timeparts) isZero() bool {
	return tp.year == 0 && tp.month == 0 && tp.day == 0 && tp.hour == 0 && tp.min == 0 && tp.sec == 0 && tp.nsec == 0
}
