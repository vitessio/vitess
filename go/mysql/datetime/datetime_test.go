/*
Copyright 2024 The Vitess Authors.

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

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/mysql/decimal"
	"vitess.io/vitess/go/vt/vthash"
)

var testGoTime = time.Date(2024, 03, 12, 12, 30, 20, 987654321, time.UTC)

func TestNewTimeFromStd(t *testing.T) {
	time := NewTimeFromStd(testGoTime)

	assert.Equal(t, uint16(12), time.hour)
	assert.Equal(t, uint8(30), time.minute)
	assert.Equal(t, uint8(20), time.second)
	assert.Equal(t, uint32(987654321), time.nanosecond)
}

func TestNewDateFromStd(t *testing.T) {
	date := NewDateFromStd(testGoTime)

	assert.Equal(t, uint16(2024), date.year)
	assert.Equal(t, uint8(03), date.month)
	assert.Equal(t, uint8(12), date.day)
}

func TestNewDateTimeFromStd(t *testing.T) {
	dt := NewDateTimeFromStd(testGoTime)

	assert.Equal(t, uint16(2024), dt.Date.year)
	assert.Equal(t, uint8(03), dt.Date.month)
	assert.Equal(t, uint8(12), dt.Date.day)

	assert.Equal(t, uint16(12), dt.Time.hour)
	assert.Equal(t, uint8(30), dt.Time.minute)
	assert.Equal(t, uint8(20), dt.Time.second)
	assert.Equal(t, uint32(987654321), dt.Time.nanosecond)
}

func TestAppendFormat(t *testing.T) {
	time := NewTimeFromStd(testGoTime)
	b := []byte("test AppendFormat: ")

	testCases := []struct {
		prec uint8
		want []byte
	}{
		{0, []byte("test AppendFormat: 12:30:20")},
		{1, []byte("test AppendFormat: 12:30:20.9")},
		{3, []byte("test AppendFormat: 12:30:20.987")},
	}

	for _, tc := range testCases {
		nb := time.AppendFormat(b, tc.prec)
		assert.Equal(t, tc.want, nb)
	}

	// Neg-time case
	time = Time{
		hour:   1<<15 + 12,
		minute: 30,
		second: 20,
	}
	nb := time.AppendFormat(b, 0)
	assert.Equal(t, []byte("test AppendFormat: -12:30:20"), nb)
}

func TestFormat(t *testing.T) {
	time := NewTimeFromStd(testGoTime)

	testCases := []struct {
		prec uint8
		want []byte
	}{
		{0, []byte("12:30:20")},
		{1, []byte("12:30:20.9")},
		{3, []byte("12:30:20.987")},
	}

	for _, tc := range testCases {
		nb := time.Format(tc.prec)
		assert.Equal(t, tc.want, nb)
	}

	// Neg-time case
	time = Time{
		hour:   1<<15 + 12,
		minute: 30,
		second: 20,
	}
	nb := time.Format(0)
	assert.Equal(t, []byte("-12:30:20"), nb)
}

func TestFormats(t *testing.T) {
	testCases := []struct {
		hour        uint16
		minute      uint8
		second      uint8
		nanosecond  uint32
		wantInt64   int64
		wantFloat64 float64
		wantDecimal decimal.Decimal
	}{
		{
			hour:        12,
			minute:      30,
			second:      20,
			nanosecond:  987654321,
			wantInt64:   123021,
			wantFloat64: 123020.987654321,
			wantDecimal: decimal.NewFromFloat(123020.987654321),
		},
		{
			hour:        1<<15 + 12,
			minute:      30,
			second:      20,
			nanosecond:  987654321,
			wantInt64:   -123021,
			wantFloat64: -123020.987654321,
			wantDecimal: decimal.NewFromFloat(-123020.987654321),
		},
		{
			hour:        1<<15 + 123,
			minute:      9,
			second:      9,
			nanosecond:  123456789,
			wantInt64:   -1230909,
			wantFloat64: -1230909.123456789,
			wantDecimal: decimal.NewFromFloat(-1230909.123456789),
		},
	}

	for _, tc := range testCases {
		time := Time{
			hour:       tc.hour,
			minute:     tc.minute,
			second:     tc.second,
			nanosecond: tc.nanosecond,
		}

		n := time.FormatInt64()
		assert.Equal(t, tc.wantInt64, n)

		f := time.FormatFloat64()
		assert.Equal(t, tc.wantFloat64, f)

		d := time.FormatDecimal()
		assert.Equal(t, tc.wantDecimal, d)
	}
}

func TestToDateTime(t *testing.T) {
	time := NewTimeFromStd(testGoTime)

	want := DateTime{
		Date: Date{
			year:  2024,
			month: 3,
			day:   12,
		},
		Time: Time{
			hour:       12,
			minute:     30,
			second:     20,
			nanosecond: 987654321,
		},
	}

	got := time.ToDateTime(testGoTime)

	assert.Equal(t, want, got)
}

func TestTimeIsZero(t *testing.T) {
	testCases := []struct {
		hour       uint16
		minute     uint8
		second     uint8
		nanosecond uint32
		wantZero   bool
	}{
		{0, 0, 0, 0, true},
		{0, 0, 0, 123, false},
		{12, 12, 23, 0, false},
	}

	for _, tc := range testCases {
		time := Time{
			hour:       tc.hour,
			minute:     tc.minute,
			second:     tc.second,
			nanosecond: tc.nanosecond,
		}

		z := time.IsZero()
		if tc.wantZero {
			assert.True(t, z, "Time %v should be considered as zero time", time)
		} else {
			assert.False(t, z, "Time %v should not be considered as zero time", time)
		}
	}
}

func TestRoundForJSON(t *testing.T) {
	testCases := []struct {
		hour       uint16
		minute     uint8
		second     uint8
		nanosecond uint32
		want       Time
	}{
		{
			hour:       12,
			minute:     30,
			second:     20,
			nanosecond: 987654321,
			want:       Time{12, 30, 20, 987654321},
		},
		{
			hour:       1<<15 + 123,
			minute:     9,
			second:     9,
			nanosecond: 123456789,
			want:       Time{1<<15 + 27, 9, 9, 123456789},
		},
	}

	for _, tc := range testCases {
		time := Time{
			hour:       tc.hour,
			minute:     tc.minute,
			second:     tc.second,
			nanosecond: tc.nanosecond,
		}

		res := time.RoundForJSON()
		assert.Equal(t, tc.want, res)
	}
}

func TestCompare(t *testing.T) {
	time := NewTimeFromStd(testGoTime)

	testCases := []struct {
		hour       uint16
		minute     uint8
		second     uint8
		nanosecond uint32
		want       int
	}{
		{12, 30, 20, 987654321, 0},
		{1<<15 + 12, 30, 20, 987654321, 1},
		{12, 29, 20, 987654321, 1},
		{12, 31, 20, 987654321, -1},
		{13, 30, 20, 987654321, -1},
		{11, 30, 20, 987654321, 1},
		{12, 30, 19, 98765, 1},
		{12, 30, 21, 98765432, -1},
		{12, 30, 20, 123123231, 1},
		{12, 30, 20, 987654322, -1},
		{12, 30, 20, 987654322, -1},
	}

	for _, tc := range testCases {
		t2 := Time{
			hour:       tc.hour,
			minute:     tc.minute,
			second:     tc.second,
			nanosecond: tc.nanosecond,
		}

		res := time.Compare(t2)
		assert.Equal(t, tc.want, res)

		// If we use `t2` to call Compare, then result should be negative
		// of what we wanted when `time` was used to call Compare
		res = t2.Compare(time)
		assert.Equal(t, -tc.want, res)
	}

	// Case when both Time are negative
	time = Time{
		hour:       1<<15 + 12,
		minute:     30,
		second:     20,
		nanosecond: 987654321,
	}
	t2 := Time{
		hour:       1<<15 + 13,
		minute:     30,
		second:     20,
		nanosecond: 987654321,
	}
	res := time.Compare(t2)
	assert.Equal(t, 1, res)
}

func TestRound(t *testing.T) {
	time := NewTimeFromStd(testGoTime)

	testCases := []struct {
		time  Time
		round int
		want  Time
	}{
		{
			time:  time,
			round: 9,
			want:  time,
		},
		{
			time:  time,
			round: 5,
			want: Time{
				hour:       12,
				minute:     30,
				second:     20,
				nanosecond: 987650000,
			},
		},
		{
			time:  time,
			round: 0,
			want: Time{
				hour:       12,
				minute:     30,
				second:     21,
				nanosecond: 0,
			},
		},
		{
			time: Time{
				hour:   12,
				minute: 30,
				second: 20,
			},
			round: 0,
			want: Time{
				hour:   12,
				minute: 30,
				second: 20,
			},
		},
		{
			time: Time{
				hour:       12,
				minute:     59,
				second:     59,
				nanosecond: 987654321,
			},
			round: 0,
			want: Time{
				hour:       13,
				minute:     0,
				second:     0,
				nanosecond: 0,
			},
		},
	}

	for _, tc := range testCases {
		res := tc.time.Round(tc.round)

		assert.Equal(t, tc.want, res)
	}
}

func TestDateIsZero(t *testing.T) {
	testCases := []struct {
		year     uint16
		month    uint8
		day      uint8
		wantZero bool
	}{
		{0, 0, 0, true},
		{0, 0, 1, false},
		{2023, 12, 23, false},
	}

	for _, tc := range testCases {
		date := Date{
			year:  tc.year,
			month: tc.month,
			day:   tc.day,
		}

		z := date.IsZero()
		if tc.wantZero {
			assert.True(t, z, "Date %v should be considered as zero date", date)
		} else {
			assert.False(t, z, "Date %v should not be considered as zero date", date)
		}
	}
}

func TestWeekday(t *testing.T) {
	testCases := []struct {
		year  uint16
		month uint8
		day   uint8
		want  int
	}{
		{0, 1, 1, 0},
		{0, 2, 28, 2},
		{0, 3, 1, 3},
		{2024, 3, 13, 3},
	}

	for _, tc := range testCases {
		date := Date{
			year:  tc.year,
			month: tc.month,
			day:   tc.day,
		}

		wd := date.Weekday()
		assert.Equal(t, time.Weekday(tc.want), wd)
	}
}

func TestMondayWeekAndSunday4DayWeek(t *testing.T) {
	testCases := []struct {
		year        uint16
		month       uint8
		day         uint8
		wantWeekDay int
		wantYear    int
	}{
		{0, 1, 1, 52, -1},
		{0, 2, 28, 9, 0},
		{0, 3, 1, 9, 0},
		{2024, 3, 13, 11, 2024},
	}

	for _, tc := range testCases {
		date := Date{
			year:  tc.year,
			month: tc.month,
			day:   tc.day,
		}

		y, wd := date.MondayWeek()
		assert.Equal(t, tc.wantWeekDay, wd)
		assert.Equal(t, tc.wantYear, y)

		y, wd = date.Sunday4DayWeek()
		assert.Equal(t, tc.wantWeekDay, wd)
		assert.Equal(t, tc.wantYear, y)
	}
}

func TestWeek(t *testing.T) {
	testCases := []struct {
		year  uint16
		month uint8
		day   uint8
		mode  int
		want  int
	}{
		{0, 1, 1, 0, 0},
		{0, 1, 1, 1, 0},
		{0, 2, 28, 2, 9},
		{0, 3, 1, 3, 9},
		{2001, 3, 14, 4, 11},
		{2000, 7, 12, 5, 28},
		{2024, 3, 13, 6, 11},
		{2024, 3, 13, 7, 11},
	}

	for _, tc := range testCases {
		date := Date{
			year:  tc.year,
			month: tc.month,
			day:   tc.day,
		}

		wd := date.Week(tc.mode)
		assert.Equal(t, tc.want, wd)
	}
}

func TestYearWeek(t *testing.T) {
	testCases := []struct {
		year  uint16
		month uint8
		day   uint8
		mode  int
		want  int
	}{
		{0, 1, 1, 0, -48},
		{0, 1, 1, 1, -48},
		{0, 2, 28, 2, 9},
		{0, 3, 1, 3, 9},
		{2001, 3, 14, 4, 200111},
		{2000, 7, 12, 5, 200028},
		{2024, 3, 13, 6, 202411},
		{2024, 3, 13, 7, 202411},
		{2024, 3, 13, 8, 202410},
	}

	for _, tc := range testCases {
		date := Date{
			year:  tc.year,
			month: tc.month,
			day:   tc.day,
		}

		wd := date.YearWeek(tc.mode)
		assert.Equal(t, tc.want, wd)
	}
}

func TestToDuration(t *testing.T) {
	tt := NewTimeFromStd(testGoTime)

	res := tt.ToDuration()
	assert.Equal(t, 45020987654321, int(res))

	// Neg Time Case
	tt.hour = 1<<15 | tt.hour
	res = tt.ToDuration()

	assert.Equal(t, -45020987654321, int(res))
}

func TestToSeconds(t *testing.T) {
	tt := NewTimeFromStd(testGoTime)

	res := tt.ToSeconds()
	assert.Equal(t, 45020, int(res))

	// Neg Time Case
	tt.hour = 1<<15 | tt.hour
	res = tt.ToSeconds()

	assert.Equal(t, -45020, int(res))
}

func TestToStdTime(t *testing.T) {
	testCases := []struct {
		year       int
		month      int
		day        int
		hour       int
		minute     int
		second     int
		nanosecond int
	}{
		{2024, 3, 15, 12, 23, 34, 45},
		{2024, 3, 15, 0, 0, 0, 0},
		{0, 0, 0, 12, 23, 34, 45},
		{0, 0, 0, 0, 0, 0, 0},
	}

	for _, tc := range testCases {
		dt := DateTime{
			Date: Date{uint16(tc.year), uint8(tc.month), uint8(tc.day)},
			Time: Time{uint16(tc.hour), uint8(tc.minute), uint8(tc.second), uint32(tc.nanosecond)},
		}

		res := dt.ToStdTime(time.Now())

		if dt.IsZero() {
			assert.Equal(t, 1, res.Day())
			assert.Equal(t, time.Month(1), res.Month())
			assert.Equal(t, 1, res.Year())
		} else if dt.Date.IsZero() {
			assert.Equal(t, time.Now().Day(), res.Day())
			assert.Equal(t, time.Now().Month(), res.Month())
			assert.Equal(t, time.Now().Year(), res.Year())
		} else {
			assert.Equal(t, tc.day, res.Day())
			assert.Equal(t, time.Month(tc.month), res.Month())
			assert.Equal(t, tc.year, res.Year())
		}

		assert.Equal(t, tc.hour, res.Hour())
		assert.Equal(t, tc.minute, res.Minute())
		assert.Equal(t, tc.second, res.Second())
		assert.Equal(t, tc.nanosecond, res.Nanosecond())
	}
}

func TestDateFormats(t *testing.T) {
	testCases := []struct {
		year          int
		month         int
		day           int
		hour          int
		minute        int
		second        int
		nanosecond    int
		wantDate      string
		wantDateInt64 int64
	}{
		{2024, 3, 15, 12, 23, 34, 45, "2024-03-15", 20240315},
		{2024, 3, 15, 0, 0, 0, 0, "2024-03-15", 20240315},
		{0, 0, 0, 12, 23, 34, 45000, "0000-00-00", 0},
		{0, 0, 0, 0, 0, 0, 0, "0000-00-00", 0},
	}

	for _, tc := range testCases {
		d := Date{uint16(tc.year), uint8(tc.month), uint8(tc.day)}

		b := d.Format()
		assert.Equal(t, tc.wantDate, string(b))

		f := d.FormatInt64()
		assert.Equal(t, tc.wantDateInt64, f)
	}
}

func TestDateCompare(t *testing.T) {
	testCases := []struct {
		d1   Date
		d2   Date
		want int
	}{
		{Date{2024, 03, 12}, Date{2023, 02, 28}, 1},
		{Date{2023, 02, 28}, Date{2024, 03, 12}, -1},
		{Date{2024, 03, 12}, Date{2024, 02, 28}, 1},
		{Date{2024, 02, 28}, Date{2024, 03, 12}, -1},
		{Date{2024, 02, 28}, Date{2024, 02, 12}, 1},
		{Date{2024, 02, 12}, Date{2024, 02, 28}, -1},
		{Date{2024, 03, 12}, Date{2024, 03, 12}, 0},
	}

	for _, tc := range testCases {
		got := tc.d1.Compare(tc.d2)
		assert.Equal(t, tc.want, got)
	}
}

func TestAddInterval(t *testing.T) {
	testCases := []struct {
		d    Date
		in   Interval
		want Date
		ok   bool
	}{
		{
			d: Date{2024, 03, 12},
			in: Interval{
				timeparts: timeparts{
					sec:  (maxDay + 1) * 24 * 60 * 60,
					prec: 6,
				},
				unit: IntervalSecond,
			},
			want: Date{2024, 03, 12},
			ok:   false,
		},
		{
			d: Date{2023, 02, 12},
			in: Interval{
				timeparts: timeparts{
					day:  18,
					sec:  12,
					prec: 6,
				},
				unit: IntervalSecond,
			},
			want: Date{2023, 03, 02},
			ok:   true,
		},
		{
			d: Date{2024, 03, 12},
			in: Interval{
				timeparts: timeparts{
					sec:  3600 * 24,
					prec: 6,
				},
				unit: IntervalSecond,
			},
			want: Date{2024, 03, 13},
			ok:   true,
		},
		{
			d: Date{2024, 03, 12},
			in: Interval{
				timeparts: timeparts{
					day:  maxDay + 1,
					prec: 6,
				},
				unit: IntervalDay,
			},
			want: Date{0, 0, 0},
			ok:   true,
		},
		{
			d: Date{2024, 03, 12},
			in: Interval{
				timeparts: timeparts{
					day:  123,
					prec: 6,
				},
				unit: IntervalDay,
			},
			want: Date{2024, 7, 13},
			ok:   true,
		},
		{
			d: Date{2024, 03, 12},
			in: Interval{
				timeparts: timeparts{
					month: 12,
				},
				unit: IntervalMonth,
			},
			want: Date{2025, 3, 12},
			ok:   true,
		},
		{
			d: Date{2024, 03, 12},
			in: Interval{
				timeparts: timeparts{
					year: -3000,
				},
				unit: IntervalMonth,
			},
			want: Date{2024, 3, 12},
			ok:   false,
		},
		{
			d: Date{2023, 03, 29},
			in: Interval{
				timeparts: timeparts{
					month: -1,
				},
				unit: IntervalMonth,
			},
			want: Date{2023, 2, 28},
			ok:   true,
		},
		{
			d: Date{2024, 02, 29},
			in: Interval{
				timeparts: timeparts{
					year: -1,
				},
				unit: IntervalYear,
			},
			want: Date{2023, 2, 28},
			ok:   true,
		},
		{
			d: Date{2024, 03, 12},
			in: Interval{
				timeparts: timeparts{
					year: 12,
				},
				unit: IntervalYear,
			},
			want: Date{2036, 3, 12},
			ok:   true,
		},
		{
			d: Date{2024, 03, 12},
			in: Interval{
				timeparts: timeparts{
					year: 10001,
				},
				unit: IntervalYear,
			},
			want: Date{2024, 3, 12},
			ok:   false,
		},
	}

	for _, tc := range testCases {
		d, ok := tc.d.AddInterval(&tc.in)

		assert.Equal(t, tc.want, d)
		assert.Equal(t, tc.ok, ok)
	}
}

func TestWeightString(t *testing.T) {
	testCases := []struct {
		dt   DateTime
		want []byte
	}{
		{
			dt: DateTime{
				Date: Date{2024, 3, 15},
				Time: Time{7, 23, 40, 0},
			},
			want: []byte{116, 101, 115, 116, 58, 32, 153, 178, 222, 117, 232, 0, 0, 0},
		},
		{
			dt: DateTime{
				Date: Date{2024, 3, 15},
				Time: Time{1<<15 | 7, 23, 40, 0},
			},
			want: []byte{116, 101, 115, 116, 58, 32, 102, 77, 33, 138, 24, 0, 0, 0},
		},
	}

	dst := []byte("test: ")
	for _, tc := range testCases {
		res := tc.dt.WeightString(dst)
		assert.Equal(t, tc.want, res)
	}
}

func TestDateTimeFormats(t *testing.T) {
	testCases := []struct {
		year                int
		month               int
		day                 int
		hour                int
		minute              int
		second              int
		nanosecond          int
		prec                int
		want                string
		wantDateTimeInt64   int64
		wantDateTimeFloat64 float64
	}{
		{2024, 3, 15, 12, 23, 34, 45, 0, "2024-03-15 12:23:34", 20240315122334, 20240315122334},
		{2024, 3, 15, 0, 0, 0, 0, 6, "2024-03-15 00:00:00.000000", 20240315000000, 20240315000000},
		{0, 0, 0, 12, 23, 34, 45000, 9, "0000-00-00 12:23:34.000045000", 122334, 122334.000045},
		{0, 0, 0, 0, 0, 0, 0, 0, "0000-00-00 00:00:00", 0, 0},
	}

	for _, tc := range testCases {
		dt := DateTime{
			Date: Date{uint16(tc.year), uint8(tc.month), uint8(tc.day)},
			Time: Time{uint16(tc.hour), uint8(tc.minute), uint8(tc.second), uint32(tc.nanosecond)},
		}

		b := dt.Format(uint8(tc.prec))
		assert.Equal(t, tc.want, string(b))

		i := dt.FormatInt64()
		assert.Equal(t, tc.wantDateTimeInt64, i)

		f := dt.FormatFloat64()
		assert.Equal(t, tc.wantDateTimeFloat64, f)
	}
}

func TestDateTimeCompare(t *testing.T) {
	testCases := []struct {
		dt1  DateTime
		dt2  DateTime
		want int
	}{
		{DateTime{Date: Date{2024, 03, 12}}, DateTime{Date: Date{2024, 02, 12}}, 1},
		{DateTime{Time: Time{12, 30, 20, 0}}, DateTime{Time: Time{12, 30, 20, 23}}, -1},
		{DateTime{Date: Date{2024, 03, 12}, Time: Time{12, 30, 20, 0}}, DateTime{Time: Time{12, 30, 20, 23}}, -1},
		{DateTime{Date: Date{2024, 03, 12}, Time: Time{12, 30, 20, 0}}, DateTime{Date: Date{2024, 03, 12}, Time: Time{12, 30, 20, 0}}, 0},
	}

	for _, tc := range testCases {
		got := tc.dt1.Compare(tc.dt2)
		assert.Equal(t, tc.want, got)
	}
}

func TestDateTimeRound(t *testing.T) {
	testCases := []struct {
		dt   DateTime
		p    int
		want DateTime
	}{
		{DateTime{Date: Date{2024, 03, 12}}, 4, DateTime{Date: Date{2024, 03, 12}}},
		{DateTime{Time: Time{12, 30, 20, 123312}}, 6, DateTime{Time: Time{12, 30, 20, 123000}}},
		{DateTime{Date: Date{2024, 03, 12}, Time: Time{12, 30, 20, 123312}}, 9, DateTime{Date: Date{2024, 03, 12}, Time: Time{12, 30, 20, 123312}}},
		{DateTime{Date: Date{2024, 03, 12}, Time: Time{12, 30, 20, 1e9}}, 9, DateTime{Date: Date{2024, 03, 12}, Time: Time{12, 30, 21, 0}}},
		{DateTime{Date: Date{2024, 03, 12}, Time: Time{12, 30, 20, 123}}, 0, DateTime{Date: Date{2024, 03, 12}, Time: Time{12, 30, 20, 0}}},
	}

	for _, tc := range testCases {
		got := tc.dt.Round(tc.p)
		assert.Equal(t, tc.want, got)
	}
}

func TestHash(t *testing.T) {
	time := NewTimeFromStd(testGoTime)
	h := vthash.New()
	time.Hash(&h)

	want := [16]byte{
		0xaa, 0x5c, 0xb4, 0xd3, 0x02, 0x85, 0xb3, 0xf3,
		0xb2, 0x44, 0x7d, 0x7c, 0x00, 0xda, 0x4a, 0xec,
	}
	assert.Equal(t, want, h.Sum128())

	date := Date{2024, 3, 16}
	h = vthash.New()
	date.Hash(&h)

	want = [16]byte{
		0xa8, 0xa0, 0x91, 0xbd, 0x3b, 0x27, 0xfc, 0x8b,
		0xf2, 0xfa, 0xe3, 0x09, 0xba, 0x23, 0x56, 0xe5,
	}
	assert.Equal(t, want, h.Sum128())

	dt := DateTime{Date: date, Time: time}
	h = vthash.New()
	dt.Hash(&h)

	want = [16]byte{
		0x0f, 0xd7, 0x67, 0xa0, 0xd8, 0x6, 0x1c, 0xc,
		0xe7, 0xbd, 0x71, 0x74, 0xfa, 0x74, 0x66, 0x38,
	}
	assert.Equal(t, want, h.Sum128())
}
