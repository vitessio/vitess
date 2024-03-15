/*
Copyright 2019 The Vitess Authors.

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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/decimal"
)

func TestParseDate(t *testing.T) {
	type date struct {
		year  int
		month int
		day   int
	}
	tests := []struct {
		input  string
		output date
		err    bool
	}{
		{input: "0000-00-00", output: date{}},
		{input: "2022-10-12", output: date{2022, 10, 12}},
		{input: "22-10-12", output: date{2022, 10, 12}},
		{input: "20221012", output: date{2022, 10, 12}},
		{input: "221012", output: date{2022, 10, 12}},
		{input: "2012-12-31", output: date{2012, 12, 31}},
		{input: "2012-1-1", output: date{2012, 1, 1}},
		{input: "2012-12-1", output: date{2012, 12, 1}},
		{input: "2012-1-11", output: date{2012, 1, 11}},
		{input: "12-12-31", output: date{2012, 12, 31}},
		{input: "12-1-1", output: date{2012, 1, 1}},
		{input: "12-12-1", output: date{2012, 12, 1}},
		{input: "12-1-11", output: date{2012, 1, 11}},
		{input: "2012/12/31", output: date{2012, 12, 31}},
		{input: "2012^12^31", output: date{2012, 12, 31}},
		{input: "2012@12@31", output: date{2012, 12, 31}},
		{input: "20070523", output: date{2007, 5, 23}},
		{input: "20070523111111", output: date{2007, 5, 23}, err: true},
		// Go can't represent a zero month or day.
		{input: "99000000", output: date{9900, 1, 1}, err: true},
		{input: "9900000", output: date{1999, 1, 1}, err: true},
		{input: "990000", output: date{1999, 1, 1}, err: true},
		{input: "070523", output: date{2007, 5, 23}},
		{input: "071332", err: true},
		{input: "2022", err: true},
	}

	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			got, ok := ParseDate(test.input)
			if test.err {
				if !got.IsZero() {
					assert.Equal(t, test.output.year, got.Year())
					assert.Equal(t, test.output.month, got.Month())
					assert.Equal(t, test.output.day, got.Day())
				}
				assert.Falsef(t, ok, "expected '%s' to fail to parse", test.input)
				return
			}

			require.True(t, ok)
			assert.Equal(t, test.output.year, got.Year())
			assert.Equal(t, test.output.month, got.Month())
			assert.Equal(t, test.output.day, got.Day())
		})
	}
}

func TestParseTime(t *testing.T) {
	type testTime struct {
		hour       int
		minute     int
		second     int
		nanosecond int
		negative   bool
	}
	tests := []struct {
		input  string
		output testTime
		norm   string
		l      int
		state  TimeState
	}{
		{input: "00:00:00", norm: "00:00:00.000000", output: testTime{}},
		{input: "-00:00:00", norm: "-00:00:00.000000", output: testTime{negative: true}},
		{input: "00:00:00foo", norm: "00:00:00.000000", output: testTime{}, state: TimePartial},
		{input: "11:12:13", norm: "11:12:13.000000", output: testTime{11, 12, 13, 0, false}},
		{input: "11:12:13foo", norm: "11:12:13.000000", output: testTime{11, 12, 13, 0, false}, state: TimePartial},
		{input: "11:12:13.1", norm: "11:12:13.100000", output: testTime{11, 12, 13, 100000000, false}, l: 1},
		{input: "11:12:13.foo", norm: "11:12:13.000000", output: testTime{11, 12, 13, 0, false}, state: TimePartial},
		{input: "11:12:13.1foo", norm: "11:12:13.100000", output: testTime{11, 12, 13, 100000000, false}, l: 1, state: TimePartial},
		{input: "11:12:13.123456", norm: "11:12:13.123456", output: testTime{11, 12, 13, 123456000, false}, l: 6},
		{input: "11:12:13.000001", norm: "11:12:13.000001", output: testTime{11, 12, 13, 1000, false}, l: 6},
		{input: "11:12:13.000000", norm: "11:12:13.000000", output: testTime{11, 12, 13, 0, false}, l: 6},
		{input: "11:12:13.123456foo", norm: "11:12:13.123456", output: testTime{11, 12, 13, 123456000, false}, l: 6, state: TimePartial},
		{input: "3 11:12:13", norm: "83:12:13.000000", output: testTime{3*24 + 11, 12, 13, 0, false}},
		{input: "3 11:12:13foo", norm: "83:12:13.000000", output: testTime{3*24 + 11, 12, 13, 0, false}, state: TimePartial},
		{input: "3 41:12:13", norm: "113:12:13.000000", output: testTime{3*24 + 41, 12, 13, 0, false}},
		{input: "3 41:12:13foo", norm: "113:12:13.000000", output: testTime{3*24 + 41, 12, 13, 0, false}, state: TimePartial},
		{input: "34 23:12:13", norm: "838:59:59.000000", output: testTime{838, 59, 59, 0, false}, state: TimePartial},
		{input: "35 11:12:13", norm: "838:59:59.000000", output: testTime{838, 59, 59, 0, false}, state: TimePartial},
		{input: "11:12", norm: "11:12:00.000000", output: testTime{11, 12, 0, 0, false}},
		{input: "5 11:12", norm: "131:12:00.000000", output: testTime{5*24 + 11, 12, 0, 0, false}},
		{input: "-2 11:12", norm: "-59:12:00.000000", output: testTime{2*24 + 11, 12, 0, 0, true}},
		{input: "--2 11:12", norm: "00:00:00.000000", state: TimeInvalid},
		{input: "nonsense", norm: "00:00:00.000000", state: TimeInvalid},
		{input: "2 11", norm: "59:00:00.000000", output: testTime{2*24 + 11, 0, 0, 0, false}},
		{input: "2 -11", norm: "00:00:02.000000", output: testTime{0, 0, 2, 0, false}, state: TimePartial},
		{input: "13", norm: "00:00:13.000000", output: testTime{0, 0, 13, 0, false}},
		{input: "111213", norm: "11:12:13.000000", output: testTime{11, 12, 13, 0, false}},
		{input: "111213.123456", norm: "11:12:13.123456", output: testTime{11, 12, 13, 123456000, false}, l: 6},
		{input: "-111213", norm: "-11:12:13.000000", output: testTime{11, 12, 13, 0, true}},
		{input: "1213", norm: "00:12:13.000000", output: testTime{0, 12, 13, 0, false}},
		{input: "25:12:13", norm: "25:12:13.000000", output: testTime{25, 12, 13, 0, false}},
		{input: "32:35", norm: "32:35:00.000000", output: testTime{32, 35, 0, 0, false}},
		{input: "101:34:58", norm: "101:34:58.000000", output: testTime{101, 34, 58, 0, false}},
		{input: "101:64:58", norm: "00:00:00.000000", state: TimeInvalid},
		{input: "101:34:68", norm: "00:00:00.000000", state: TimeInvalid},
		{input: "1", norm: "00:00:01.000000", output: testTime{0, 0, 1, 0, false}},
		{input: "11", norm: "00:00:11.000000", output: testTime{0, 0, 11, 0, false}},
		{input: "111", norm: "00:01:11.000000", output: testTime{0, 1, 11, 0, false}},
		{input: "1111", norm: "00:11:11.000000", output: testTime{0, 11, 11, 0, false}},
		{input: "11111", norm: "01:11:11.000000", output: testTime{1, 11, 11, 0, false}},
		{input: "111111", norm: "11:11:11.000000", output: testTime{11, 11, 11, 0, false}},
		{input: "1foo", norm: "00:00:01.000000", output: testTime{0, 0, 1, 0, false}, state: TimePartial},
		{input: "11foo", norm: "00:00:11.000000", output: testTime{0, 0, 11, 0, false}, state: TimePartial},
		{input: "111foo", norm: "00:01:11.000000", output: testTime{0, 1, 11, 0, false}, state: TimePartial},
		{input: "1111foo", norm: "00:11:11.000000", output: testTime{0, 11, 11, 0, false}, state: TimePartial},
		{input: "11111foo", norm: "01:11:11.000000", output: testTime{1, 11, 11, 0, false}, state: TimePartial},
		{input: "111111foo", norm: "11:11:11.000000", output: testTime{11, 11, 11, 0, false}, state: TimePartial},
		{input: "1111111foo", norm: "111:11:11.000000", output: testTime{111, 11, 11, 0, false}, state: TimePartial},
		{input: "-1", norm: "-00:00:01.000000", output: testTime{0, 0, 1, 0, true}},
		{input: "-11", norm: "-00:00:11.000000", output: testTime{0, 0, 11, 0, true}},
		{input: "-111", norm: "-00:01:11.000000", output: testTime{0, 1, 11, 0, true}},
		{input: "-1111", norm: "-00:11:11.000000", output: testTime{0, 11, 11, 0, true}},
		{input: "-11111", norm: "-01:11:11.000000", output: testTime{1, 11, 11, 0, true}},
		{input: "-111111", norm: "-11:11:11.000000", output: testTime{11, 11, 11, 0, true}},
		{input: "-1111111", norm: "-111:11:11.000000", output: testTime{111, 11, 11, 0, true}},
		{input: "0", norm: "00:00:00.000000", output: testTime{0, 0, 0, 0, false}},
		{input: "1", norm: "00:00:01.000000", output: testTime{0, 0, 1, 0, false}},
		{input: "11", norm: "00:00:11.000000", output: testTime{0, 0, 11, 0, false}},
		{input: "111", norm: "00:01:11.000000", output: testTime{0, 1, 11, 0, false}},
		{input: "1111", norm: "00:11:11.000000", output: testTime{0, 11, 11, 0, false}},
		{input: "11111", norm: "01:11:11.000000", output: testTime{1, 11, 11, 0, false}},
		{input: "111111", norm: "11:11:11.000000", output: testTime{11, 11, 11, 0, false}},
		{input: "1111111", norm: "111:11:11.000000", output: testTime{111, 11, 11, 0, false}},
		{input: "-1.1", norm: "-00:00:01.100000", output: testTime{0, 0, 1, 100000000, true}, l: 1},
		{input: "-11.1", norm: "-00:00:11.100000", output: testTime{0, 0, 11, 100000000, true}, l: 1},
		{input: "-111.1", norm: "-00:01:11.100000", output: testTime{0, 1, 11, 100000000, true}, l: 1},
		{input: "-1111.1", norm: "-00:11:11.100000", output: testTime{0, 11, 11, 100000000, true}, l: 1},
		{input: "-11111.1", norm: "-01:11:11.100000", output: testTime{1, 11, 11, 100000000, true}, l: 1},
		{input: "-111111.1", norm: "-11:11:11.100000", output: testTime{11, 11, 11, 100000000, true}, l: 1},
		{input: "-1111111.1", norm: "-111:11:11.100000", output: testTime{111, 11, 11, 100000000, true}, l: 1},
		{input: "1.1", norm: "00:00:01.100000", output: testTime{0, 0, 1, 100000000, false}, l: 1},
		{input: "11.1", norm: "00:00:11.100000", output: testTime{0, 0, 11, 100000000, false}, l: 1},
		{input: "111.1", norm: "00:01:11.100000", output: testTime{0, 1, 11, 100000000, false}, l: 1},
		{input: "1111.1", norm: "00:11:11.100000", output: testTime{0, 11, 11, 100000000, false}, l: 1},
		{input: "11111.1", norm: "01:11:11.100000", output: testTime{1, 11, 11, 100000000, false}, l: 1},
		{input: "111111.1", norm: "11:11:11.100000", output: testTime{11, 11, 11, 100000000, false}, l: 1},
		{input: "1111111.1", norm: "111:11:11.100000", output: testTime{111, 11, 11, 100000000, false}, l: 1},
		{input: "20000101", norm: "838:59:59.000000", output: testTime{838, 59, 59, 0, false}, state: TimePartial},
		{input: "-20000101", norm: "-838:59:59.000000", output: testTime{838, 59, 59, 0, true}, state: TimePartial},
		{input: "999995959", norm: "838:59:59.000000", output: testTime{838, 59, 59, 0, false}, state: TimePartial},
		{input: "-999995959", norm: "-838:59:59.000000", output: testTime{838, 59, 59, 0, true}, state: TimePartial},
		{input: "4294965959", norm: "838:59:59.000000", output: testTime{838, 59, 59, 0, false}, state: TimePartial},
		{input: "-4294965959", norm: "-838:59:59.000000", output: testTime{838, 59, 59, 0, true}, state: TimePartial},
		{input: "4294975959", norm: "00:00:00.000000", state: TimeInvalid},
		{input: "-4294975959", norm: "00:00:00.000000", state: TimeInvalid},
		{input: "\t34 foo\t", norm: "00:00:34.000000", output: testTime{0, 0, 34, 0, false}, state: TimePartial},
		{input: "\t34 1foo\t", norm: "817:00:00.000000", output: testTime{817, 0, 0, 0, false}, state: TimePartial},
		{input: "\t34 23foo\t", norm: "838:59:59.000000", output: testTime{838, 59, 59, 0, false}, state: TimePartial},
		{input: "\t35 foo\t", norm: "00:00:35.000000", output: testTime{0, 0, 35, 0, false}, state: TimePartial},
		{input: "\t35 1foo\t", norm: "838:59:59.000000", output: testTime{838, 59, 59, 0, false}, state: TimePartial},
	}

	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			got, l, state := ParseTime(test.input, -1)
			assert.Equal(t, test.state, state)
			assert.Equal(t, test.output.hour, got.Hour())
			assert.Equal(t, test.output.minute, got.Minute())
			assert.Equal(t, test.output.second, got.Second())
			assert.Equal(t, test.output.nanosecond, got.Nanosecond())
			assert.Equal(t, test.norm, string(got.AppendFormat(nil, 6)))
			assert.Equal(t, test.l, l)
		})
	}
}

func TestParseDateTime(t *testing.T) {
	type datetime struct {
		year       int
		month      int
		day        int
		hour       int
		minute     int
		second     int
		nanosecond int
	}
	tests := []struct {
		input  string
		output datetime
		l      int
		err    bool
	}{
		{input: "0000-00-00 00:00:00", output: datetime{}},
		{input: "2022-10-12 11:12:13", output: datetime{2022, 10, 12, 11, 12, 13, 0}},
		{input: "2022-10-12 11:12:13.123456", output: datetime{2022, 10, 12, 11, 12, 13, 123456000}, l: 6},
		{input: "20221012111213.123456", output: datetime{2022, 10, 12, 11, 12, 13, 123456000}, l: 6},
		{input: "221012111213.123456", output: datetime{2022, 10, 12, 11, 12, 13, 123456000}, l: 6},
		{input: "2022101211121321321312", output: datetime{2022, 10, 12, 11, 12, 13, 0}, err: true},
		{input: "3284004416225113510", output: datetime{}, err: true},
		{input: "2012-12-31 11:30:45", output: datetime{2012, 12, 31, 11, 30, 45, 0}},
		{input: "2012^12^31 11+30+45", output: datetime{2012, 12, 31, 11, 30, 45, 0}},
		{input: "2012/12/31 11*30*45", output: datetime{2012, 12, 31, 11, 30, 45, 0}},
		{input: "2012@12@31 11^30^45", output: datetime{2012, 12, 31, 11, 30, 45, 0}},
		{input: "2012-12-31T11:30:45", output: datetime{2012, 12, 31, 11, 30, 45, 0}},
		{input: "20070523091528", output: datetime{2007, 5, 23, 9, 15, 28, 0}},
		{input: "070523091528", output: datetime{2007, 5, 23, 9, 15, 28, 0}},

		{input: "2042-07-07 07:07:07", output: datetime{2042, 7, 7, 7, 7, 7, 0}},
		{input: "2042-7-07 07:07:07", output: datetime{2042, 7, 7, 7, 7, 7, 0}},
		{input: "2042-07-7 07:07:07", output: datetime{2042, 7, 7, 7, 7, 7, 0}},
		{input: "2042-07-07 7:07:07", output: datetime{2042, 7, 7, 7, 7, 7, 0}},
		{input: "2042-07-07 07:7:07", output: datetime{2042, 7, 7, 7, 7, 7, 0}},
		{input: "2042-07-07 07:07:7", output: datetime{2042, 7, 7, 7, 7, 7, 0}},
		{input: "2042-7-7 7:7:7", output: datetime{2042, 7, 7, 7, 7, 7, 0}},

		{input: "42-07-07 07:07:07", output: datetime{2042, 7, 7, 7, 7, 7, 0}},
		{input: "42-7-07 07:07:07", output: datetime{2042, 7, 7, 7, 7, 7, 0}},
		{input: "42-07-7 07:07:07", output: datetime{2042, 7, 7, 7, 7, 7, 0}},
		{input: "42-07-07 7:07:07", output: datetime{2042, 7, 7, 7, 7, 7, 0}},
		{input: "42-07-07 07:7:07", output: datetime{2042, 7, 7, 7, 7, 7, 0}},
		{input: "42-07-07 07:07:7", output: datetime{2042, 7, 7, 7, 7, 7, 0}},
		{input: "42-7-7 7:7:7", output: datetime{2042, 7, 7, 7, 7, 7, 0}},
	}

	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			got, l, ok := ParseDateTime(test.input, -1)
			if test.err {
				if !got.IsZero() {
					assert.Equal(t, test.output.year, got.Date.Year())
					assert.Equal(t, test.output.month, got.Date.Month())
					assert.Equal(t, test.output.day, got.Date.Day())
					assert.Equal(t, test.output.hour, got.Time.Hour())
					assert.Equal(t, test.output.minute, got.Time.Minute())
					assert.Equal(t, test.output.second, got.Time.Second())
					assert.Equal(t, test.output.nanosecond, got.Time.Nanosecond())
					assert.Equal(t, test.l, l)
				}
				assert.Falsef(t, ok, "did not fail to parse %s", test.input)
				return
			}

			require.True(t, ok)
			assert.Equal(t, test.output.year, got.Date.Year())
			assert.Equal(t, test.output.month, got.Date.Month())
			assert.Equal(t, test.output.day, got.Date.Day())
			assert.Equal(t, test.output.hour, got.Time.Hour())
			assert.Equal(t, test.output.minute, got.Time.Minute())
			assert.Equal(t, test.output.second, got.Time.Second())
			assert.Equal(t, test.output.nanosecond, got.Time.Nanosecond())
			assert.Equal(t, test.l, l)
		})
	}
}

func TestParseDateTimeInt64(t *testing.T) {
	type datetime struct {
		year       int
		month      int
		day        int
		hour       int
		minute     int
		second     int
		nanosecond int
	}
	tests := []struct {
		input  int64
		output datetime
		l      int
		err    bool
	}{
		{input: 1, output: datetime{}, err: true},
		{input: 20221012000000, output: datetime{2022, 10, 12, 0, 0, 0, 0}},
		{input: 20221012112233, output: datetime{2022, 10, 12, 11, 22, 33, 0}},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%d", test.input), func(t *testing.T) {
			got, ok := ParseDateTimeInt64(test.input)
			if test.err {
				if !got.IsZero() {
					assert.Equal(t, test.output.year, got.Date.Year())
					assert.Equal(t, test.output.month, got.Date.Month())
					assert.Equal(t, test.output.day, got.Date.Day())
					assert.Equal(t, test.output.hour, got.Time.Hour())
					assert.Equal(t, test.output.minute, got.Time.Minute())
					assert.Equal(t, test.output.second, got.Time.Second())
					assert.Equal(t, test.output.nanosecond, got.Time.Nanosecond())
				}
				assert.Falsef(t, ok, "did not fail to parse %s", test.input)
				return
			}

			require.True(t, ok)
			assert.Equal(t, test.output.year, got.Date.Year())
			assert.Equal(t, test.output.month, got.Date.Month())
			assert.Equal(t, test.output.day, got.Date.Day())
			assert.Equal(t, test.output.hour, got.Time.Hour())
			assert.Equal(t, test.output.minute, got.Time.Minute())
			assert.Equal(t, test.output.second, got.Time.Second())
			assert.Equal(t, test.output.nanosecond, got.Time.Nanosecond())
		})
	}
}

func TestParseDateTimeFloat(t *testing.T) {
	type datetime struct {
		year       int
		month      int
		day        int
		hour       int
		minute     int
		second     int
		nanosecond int
	}
	tests := []struct {
		input   float64
		prec    int
		output  datetime
		outPrec int
		l       int
		err     bool
	}{
		{input: 1, prec: 3, outPrec: 3, output: datetime{}, err: true},
		{input: 20221012000000.101562, prec: -2, outPrec: 6, output: datetime{2022, 10, 12, 0, 0, 0, 101562500}},
		{input: 20221012112233.125000, prec: 3, outPrec: 3, output: datetime{2022, 10, 12, 11, 22, 33, 125000000}},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%f", test.input), func(t *testing.T) {
			got, p, ok := ParseDateTimeFloat(test.input, test.prec)
			if test.err {
				if !got.IsZero() {
					assert.Equal(t, test.output.year, got.Date.Year())
					assert.Equal(t, test.output.month, got.Date.Month())
					assert.Equal(t, test.output.day, got.Date.Day())
					assert.Equal(t, test.output.hour, got.Time.Hour())
					assert.Equal(t, test.output.minute, got.Time.Minute())
					assert.Equal(t, test.output.second, got.Time.Second())
					assert.Equal(t, test.output.nanosecond, got.Time.Nanosecond())
				}
				assert.Falsef(t, ok, "did not fail to parse %s", test.input)
				return
			}

			require.True(t, ok)
			assert.Equal(t, test.outPrec, p)
			assert.Equal(t, test.output.year, got.Date.Year())
			assert.Equal(t, test.output.month, got.Date.Month())
			assert.Equal(t, test.output.day, got.Date.Day())
			assert.Equal(t, test.output.hour, got.Time.Hour())
			assert.Equal(t, test.output.minute, got.Time.Minute())
			assert.Equal(t, test.output.second, got.Time.Second())
			assert.Equal(t, test.output.nanosecond, got.Time.Nanosecond())
		})
	}
}

func TestParseDateTimeDecimal(t *testing.T) {
	type datetime struct {
		year       int
		month      int
		day        int
		hour       int
		minute     int
		second     int
		nanosecond int
	}
	tests := []struct {
		input   decimal.Decimal
		prec    int
		output  datetime
		outPrec int
		l       int32
		err     bool
	}{
		{input: decimal.NewFromFloat(1), l: 6, prec: 3, outPrec: 3, output: datetime{}, err: true},
		{input: decimal.NewFromFloat(20221012000000.101562), l: 6, prec: -2, outPrec: 6, output: datetime{2022, 10, 12, 0, 0, 0, 100000000}},
		{input: decimal.NewFromFloat(20221012112233.125000), l: 6, prec: 3, outPrec: 3, output: datetime{2022, 10, 12, 11, 22, 33, 125000000}},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v", test.input), func(t *testing.T) {
			got, p, ok := ParseDateTimeDecimal(test.input, test.l, test.prec)
			if test.err {
				if !got.IsZero() {
					assert.Equal(t, test.output.year, got.Date.Year())
					assert.Equal(t, test.output.month, got.Date.Month())
					assert.Equal(t, test.output.day, got.Date.Day())
					assert.Equal(t, test.output.hour, got.Time.Hour())
					assert.Equal(t, test.output.minute, got.Time.Minute())
					assert.Equal(t, test.output.second, got.Time.Second())
					assert.Equal(t, test.output.nanosecond, got.Time.Nanosecond())
				}
				assert.Falsef(t, ok, "did not fail to parse %s", test.input)
				return
			}

			require.True(t, ok)
			assert.Equal(t, test.outPrec, p)
			assert.Equal(t, test.output.year, got.Date.Year())
			assert.Equal(t, test.output.month, got.Date.Month())
			assert.Equal(t, test.output.day, got.Date.Day())
			assert.Equal(t, test.output.hour, got.Time.Hour())
			assert.Equal(t, test.output.minute, got.Time.Minute())
			assert.Equal(t, test.output.second, got.Time.Second())
			assert.Equal(t, test.output.nanosecond, got.Time.Nanosecond())
		})
	}
}

func TestParseDateFloatAndDecimal(t *testing.T) {
	type date struct {
		year  int
		month int
		day   int
	}
	tests := []struct {
		input   float64
		prec    int
		output  date
		outPrec int
		l       int32
		err     bool
	}{
		{input: 1, output: date{0, 0, 1}, err: true},
		{input: 20221012.102, output: date{2022, 10, 12}},
		{input: 20221212.52, output: date{2022, 12, 12}},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%f", test.input), func(t *testing.T) {
			got, ok := ParseDateFloat(test.input)
			if test.err {
				if !got.IsZero() {
					assert.Equal(t, test.output.year, got.Year())
					assert.Equal(t, test.output.month, got.Month())
					assert.Equal(t, test.output.day, got.Day())
				}
				assert.Falsef(t, ok, "did not fail to parse %s", test.input)
				return
			}

			require.True(t, ok)
			assert.Equal(t, test.output.year, got.Year())
			assert.Equal(t, test.output.month, got.Month())
			assert.Equal(t, test.output.day, got.Day())

			got, ok = ParseDateDecimal(decimal.NewFromFloat(test.input))
			if test.err {
				if !got.IsZero() {
					assert.Equal(t, test.output.year, got.Year())
					assert.Equal(t, test.output.month, got.Month())
					assert.Equal(t, test.output.day, got.Day())
				}
				assert.Falsef(t, ok, "did not fail to parse %s", test.input)
				return
			}

			require.True(t, ok)
			assert.Equal(t, test.output.year, got.Year())
			assert.Equal(t, test.output.month, got.Month())
			assert.Equal(t, test.output.day, got.Day())
		})
	}
}

func TestParseTimeFloat(t *testing.T) {
	type time struct {
		hour       int
		minute     int
		second     int
		nanosecond int
	}
	tests := []struct {
		input   float64
		prec    int
		outPrec int
		output  time
		err     bool
	}{
		{input: 1, prec: 1, outPrec: 1, output: time{0, 0, 1, 0}, err: false},
		{input: 201012.102, prec: -1, outPrec: 6, output: time{20, 10, 12, 102000000}},
		{input: 201212.52, prec: -1, outPrec: 6, output: time{20, 12, 12, 519999999}},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%f", test.input), func(t *testing.T) {
			got, p, ok := ParseTimeFloat(test.input, test.prec)
			if test.err {
				if !got.IsZero() {
					assert.Equal(t, test.outPrec, p)
					assert.Equal(t, test.output.hour, got.Hour())
					assert.Equal(t, test.output.minute, got.Minute())
					assert.Equal(t, test.output.second, got.Second())
					assert.Equal(t, test.output.nanosecond, got.Nanosecond())
				}
				assert.Falsef(t, ok, "did not fail to parse %s", test.input)
				return
			}

			require.True(t, ok)
			assert.Equal(t, test.outPrec, p)
			assert.Equal(t, test.output.hour, got.Hour())
			assert.Equal(t, test.output.minute, got.Minute())
			assert.Equal(t, test.output.second, got.Second())
			assert.Equal(t, test.output.nanosecond, got.Nanosecond())
		})
	}
}

func TestParseTimeDecimal(t *testing.T) {
	type time struct {
		hour       int
		minute     int
		second     int
		nanosecond int
	}
	tests := []struct {
		input   decimal.Decimal
		l       int32
		prec    int
		outPrec int
		output  time
		err     bool
	}{
		{input: decimal.NewFromFloat(1), l: 6, prec: 1, outPrec: 1, output: time{0, 0, 1, 0}, err: false},
		{input: decimal.NewFromFloat(201012.102), l: 6, prec: -1, outPrec: 6, output: time{20, 10, 12, 102000000}},
		{input: decimal.NewFromFloat(201212.52), l: 6, prec: -1, outPrec: 6, output: time{20, 12, 12, 520000000}},
		{input: decimal.NewFromFloat(201212.52), l: 10, prec: -1, outPrec: 9, output: time{20, 12, 12, 520000000}},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%v", test.input), func(t *testing.T) {
			got, p, ok := ParseTimeDecimal(test.input, test.l, test.prec)
			if test.err {
				if !got.IsZero() {
					assert.Equal(t, test.outPrec, p)
					assert.Equal(t, test.output.hour, got.Hour())
					assert.Equal(t, test.output.minute, got.Minute())
					assert.Equal(t, test.output.second, got.Second())
					assert.Equal(t, test.output.nanosecond, got.Nanosecond())
				}
				assert.Falsef(t, ok, "did not fail to parse %s", test.input)
				return
			}

			require.True(t, ok)
			assert.Equal(t, test.outPrec, p)
			assert.Equal(t, test.output.hour, got.Hour())
			assert.Equal(t, test.output.minute, got.Minute())
			assert.Equal(t, test.output.second, got.Second())
			assert.Equal(t, test.output.nanosecond, got.Nanosecond())
		})
	}
}
