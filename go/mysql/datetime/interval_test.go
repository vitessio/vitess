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

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/mysql/decimal"
)

func TestIntervalType(t *testing.T) {
	testCases := []struct {
		in                 IntervalType
		wantPartCount      int
		wantTimeParts      bool
		wantDateParts      bool
		wantDayParts       bool
		wantMonthParts     bool
		wantNeedsPrecision bool
	}{
		{IntervalYear, 1, false, true, false, false, false},
		{IntervalMonth, 1, false, true, false, true, false},
		{IntervalDay, 1, false, true, true, false, false},
		{IntervalHour, 1, true, false, false, false, false},
		{IntervalMinute, 1, true, false, false, false, false},
		{IntervalSecond, 1, true, false, false, false, false},
		{IntervalMicrosecond, 1, true, false, false, false, true},
		{IntervalNone, 0, false, false, false, false, false},
		{IntervalQuarter, 1, false, true, false, true, false},
		{IntervalWeek, 1, false, true, true, false, false},
		{IntervalSecondMicrosecond, 2, true, false, false, false, true},
		{IntervalMinuteMicrosecond, 3, true, false, false, false, true},
		{IntervalMinuteSecond, 2, true, false, false, false, false},
		{IntervalHourMicrosecond, 4, true, false, false, false, true},
		{IntervalHourSecond, 3, true, false, false, false, false},
		{IntervalHourMinute, 2, true, false, false, false, false},
		{IntervalDayMicrosecond, 5, true, true, true, false, true},
		{IntervalDaySecond, 4, true, true, true, false, false},
		{IntervalDayMinute, 3, true, true, true, false, false},
		{IntervalDayHour, 2, true, true, true, false, false},
		{IntervalYearMonth, 2, false, true, false, true, false},
	}

	for _, tc := range testCases {
		got := tc.in.HasTimeParts()
		assert.Equal(t, tc.wantTimeParts, got)

		got = tc.in.HasDateParts()
		assert.Equal(t, tc.wantDateParts, got)

		got = tc.in.HasDayParts()
		assert.Equal(t, tc.wantDayParts, got)

		got = tc.in.HasMonthParts()
		assert.Equal(t, tc.wantMonthParts, got)

		got = tc.in.NeedsPrecision()
		assert.Equal(t, tc.wantNeedsPrecision, got)

		assert.Equal(t, tc.wantPartCount, tc.in.PartCount())
	}
}

func TestParseInterval(t *testing.T) {
	testCases := []struct {
		in   string
		tt   IntervalType
		want *Interval
	}{
		{
			in: "123",
			tt: IntervalSecond,
			want: &Interval{
				timeparts: timeparts{
					sec:  123,
					prec: 6,
				},
				unit: IntervalSecond,
			},
		},
		{
			in: "1",
			tt: IntervalDay,
			want: &Interval{
				timeparts: timeparts{
					day:  1,
					prec: 0,
				},
				unit: IntervalDay,
			},
		},
		{
			in: "1234",
			tt: IntervalMinute,
			want: &Interval{
				timeparts: timeparts{
					min:  1234,
					prec: 0,
				},
				unit: IntervalMinute,
			},
		},
		{
			in: "123.98",
			tt: IntervalSecond,
			want: &Interval{
				timeparts: timeparts{
					sec:  123,
					nsec: 980000000,
					prec: 6,
				},
				unit: IntervalSecond,
			},
		},
	}

	for _, tc := range testCases {
		res := ParseInterval(tc.in, tc.tt, false)
		assert.Equal(t, tc.want, res)
	}

	// Neg interval case
	res := ParseInterval("123", IntervalSecond, true)
	want := &Interval{
		timeparts: timeparts{
			sec:  -123,
			prec: 6,
		},
		unit: IntervalSecond,
	}
	assert.Equal(t, want, res)
}

func TestParseIntervalInt64(t *testing.T) {
	testCases := []struct {
		in   int64
		tt   IntervalType
		want *Interval
	}{
		{
			in: 123,
			tt: IntervalSecond,
			want: &Interval{
				timeparts: timeparts{
					sec:  123,
					prec: 0,
				},
				unit: IntervalSecond,
			},
		},
		{
			in: 1234,
			tt: IntervalMicrosecond,
			want: &Interval{
				timeparts: timeparts{
					nsec: 1234000,
					prec: 6,
				},
				unit: IntervalMicrosecond,
			},
		},
		{
			in: 35454,
			tt: IntervalMinute,
			want: &Interval{
				timeparts: timeparts{
					min:  35454,
					prec: 0,
				},
				unit: IntervalMinute,
			},
		},
	}

	for _, tc := range testCases {
		res := ParseIntervalInt64(tc.in, tc.tt, false)
		assert.Equal(t, tc.want, res)
	}

	// Neg interval case
	res := ParseIntervalInt64(123, IntervalSecond, true)
	want := &Interval{
		timeparts: timeparts{
			sec:  -123,
			prec: 0,
		},
		unit: IntervalSecond,
	}
	assert.Equal(t, want, res)
}

func TestParseIntervalFloat(t *testing.T) {
	testCases := []struct {
		in   float64
		tt   IntervalType
		want *Interval
	}{
		{
			in: 123.45,
			tt: IntervalSecond,
			want: &Interval{
				timeparts: timeparts{
					sec:  123,
					nsec: 450000000,
					prec: 6,
				},
				unit: IntervalSecond,
			},
		},
		{
			in: 12.34,
			tt: IntervalMinute,
			want: &Interval{
				timeparts: timeparts{
					min:  12,
					prec: 0,
				},
				unit: IntervalMinute,
			},
		},
		{
			in: 12.67,
			tt: IntervalHour,
			want: &Interval{
				timeparts: timeparts{
					hour: 13,
					prec: 0,
				},
				unit: IntervalHour,
			},
		},
		{
			in: 12.67,
			tt: IntervalMicrosecond,
			want: &Interval{
				timeparts: timeparts{
					nsec: 13000,
					prec: 6,
				},
				unit: IntervalMicrosecond,
			},
		},
		{
			in: 123,
			tt: IntervalDay,
			want: &Interval{
				timeparts: timeparts{
					day:  123,
					prec: 0,
				},
				unit: IntervalDay,
			},
		},
	}

	for _, tc := range testCases {
		res := ParseIntervalFloat(tc.in, tc.tt, false)
		assert.Equal(t, tc.want, res)

		res = ParseIntervalDecimal(decimal.NewFromFloat(tc.in), 6, tc.tt, false)
		assert.Equal(t, tc.want, res)
	}

	// Neg interval case
	res := ParseIntervalFloat(123.4, IntervalSecond, true)
	want := &Interval{
		timeparts: timeparts{
			sec:  -123,
			nsec: -400000000,
			prec: 6,
		},
		unit: IntervalSecond,
	}
	assert.Equal(t, want, res)
}

func TestInRange(t *testing.T) {
	testCases := []struct {
		in          Interval
		wantInRange bool
	}{
		{
			in: Interval{
				timeparts: timeparts{
					day: 3652425,
				},
			},
			wantInRange: false,
		},
		{
			in: Interval{
				timeparts: timeparts{
					day: 3652424,
				},
			},
			wantInRange: true,
		},
		{
			in: Interval{
				timeparts: timeparts{
					hour: 3652425 * 24,
				},
			},
			wantInRange: false,
		},
		{
			in: Interval{
				timeparts: timeparts{
					hour: 3652424 * 24,
				},
			},
			wantInRange: true,
		},
		{
			in: Interval{
				timeparts: timeparts{
					min: 3652425 * 24 * 60,
				},
			},
			wantInRange: false,
		},
		{
			in: Interval{
				timeparts: timeparts{
					min: 3652424 * 24 * 60,
				},
			},
			wantInRange: true,
		},
		{
			in: Interval{
				timeparts: timeparts{
					sec: 3652425 * 24 * 60 * 60,
				},
			},
			wantInRange: false,
		},
		{
			in: Interval{
				timeparts: timeparts{
					sec: 3652424 * 24 * 60 * 60,
				},
			},
			wantInRange: true,
		},
	}

	for _, tc := range testCases {
		got := tc.in.inRange()

		assert.Equal(t, tc.wantInRange, got)
	}
}
