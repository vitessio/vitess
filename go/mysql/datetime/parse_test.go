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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
		err    bool
	}{
		{input: "00:00:00", norm: "00:00:00.000000", output: testTime{}},
		{input: "00:00:00foo", norm: "00:00:00.000000", output: testTime{}, err: true},
		{input: "11:12:13", norm: "11:12:13.000000", output: testTime{11, 12, 13, 0, false}},
		{input: "11:12:13foo", norm: "11:12:13.000000", output: testTime{11, 12, 13, 0, false}, err: true},
		{input: "11:12:13.1", norm: "11:12:13.100000", output: testTime{11, 12, 13, 100000000, false}, l: 1},
		{input: "11:12:13.foo", norm: "11:12:13.000000", output: testTime{11, 12, 13, 0, false}, err: true},
		{input: "11:12:13.1foo", norm: "11:12:13.100000", output: testTime{11, 12, 13, 100000000, false}, l: 1, err: true},
		{input: "11:12:13.123456", norm: "11:12:13.123456", output: testTime{11, 12, 13, 123456000, false}, l: 6},
		{input: "11:12:13.000001", norm: "11:12:13.000001", output: testTime{11, 12, 13, 1000, false}, l: 6},
		{input: "11:12:13.000000", norm: "11:12:13.000000", output: testTime{11, 12, 13, 0, false}, l: 6},
		{input: "11:12:13.123456foo", norm: "11:12:13.123456", output: testTime{11, 12, 13, 123456000, false}, l: 6, err: true},
		{input: "3 11:12:13", norm: "83:12:13.000000", output: testTime{3*24 + 11, 12, 13, 0, false}},
		{input: "3 11:12:13foo", norm: "83:12:13.000000", output: testTime{3*24 + 11, 12, 13, 0, false}, err: true},
		{input: "3 41:12:13", norm: "113:12:13.000000", output: testTime{3*24 + 41, 12, 13, 0, false}},
		{input: "3 41:12:13foo", norm: "113:12:13.000000", output: testTime{3*24 + 41, 12, 13, 0, false}, err: true},
		{input: "35 11:12:13", norm: "00:00:00.000000", err: true},
		{input: "11:12", norm: "11:12:00.000000", output: testTime{11, 12, 0, 0, false}},
		{input: "5 11:12", norm: "131:12:00.000000", output: testTime{5*24 + 11, 12, 0, 0, false}},
		{input: "-2 11:12", norm: "-59:12:00.000000", output: testTime{2*24 + 11, 12, 0, 0, true}},
		{input: "--2 11:12", norm: "00:00:00.000000", err: true},
		{input: "nonsense", norm: "00:00:00.000000", err: true},
		{input: "2 11", norm: "59:00:00.000000", output: testTime{2*24 + 11, 0, 0, 0, false}},
		{input: "2 -11", norm: "48:00:00.000000", output: testTime{2 * 24, 0, 0, 0, false}, err: true},
		{input: "13", norm: "00:00:13.000000", output: testTime{0, 0, 13, 0, false}},
		{input: "111213", norm: "11:12:13.000000", output: testTime{11, 12, 13, 0, false}},
		{input: "111213.123456", norm: "11:12:13.123456", output: testTime{11, 12, 13, 123456000, false}, l: 6},
		{input: "-111213", norm: "-11:12:13.000000", output: testTime{11, 12, 13, 0, true}},
		{input: "1213", norm: "00:12:13.000000", output: testTime{0, 12, 13, 0, false}},
		{input: "25:12:13", norm: "25:12:13.000000", output: testTime{25, 12, 13, 0, false}},
		{input: "32:35", norm: "32:35:00.000000", output: testTime{32, 35, 0, 0, false}},
		{input: "101:34:58", norm: "101:34:58.000000", output: testTime{101, 34, 58, 0, false}},
		{input: "1", norm: "00:00:01.000000", output: testTime{0, 0, 1, 0, false}},
		{input: "11", norm: "00:00:11.000000", output: testTime{0, 0, 11, 0, false}},
		{input: "111", norm: "00:01:11.000000", output: testTime{0, 1, 11, 0, false}},
		{input: "1111", norm: "00:11:11.000000", output: testTime{0, 11, 11, 0, false}},
		{input: "11111", norm: "01:11:11.000000", output: testTime{1, 11, 11, 0, false}},
		{input: "111111", norm: "11:11:11.000000", output: testTime{11, 11, 11, 0, false}},
		{input: "1foo", norm: "00:00:01.000000", output: testTime{0, 0, 1, 0, false}, err: true},
		{input: "11foo", norm: "00:00:11.000000", output: testTime{0, 0, 11, 0, false}, err: true},
		{input: "111foo", norm: "00:01:11.000000", output: testTime{0, 1, 11, 0, false}, err: true},
		{input: "1111foo", norm: "00:11:11.000000", output: testTime{0, 11, 11, 0, false}, err: true},
		{input: "11111foo", norm: "01:11:11.000000", output: testTime{1, 11, 11, 0, false}, err: true},
		{input: "111111foo", norm: "11:11:11.000000", output: testTime{11, 11, 11, 0, false}, err: true},
		{input: "1111111foo", norm: "111:11:11.000000", output: testTime{111, 11, 11, 0, false}, err: true},
		{input: "-1", norm: "-00:00:01.000000", output: testTime{0, 0, 1, 0, true}},
		{input: "-11", norm: "-00:00:11.000000", output: testTime{0, 0, 11, 0, true}},
		{input: "-111", norm: "-00:01:11.000000", output: testTime{0, 1, 11, 0, true}},
		{input: "-1111", norm: "-00:11:11.000000", output: testTime{0, 11, 11, 0, true}},
		{input: "-11111", norm: "-01:11:11.000000", output: testTime{1, 11, 11, 0, true}},
		{input: "-111111", norm: "-11:11:11.000000", output: testTime{11, 11, 11, 0, true}},
		{input: "-1111111", norm: "-111:11:11.000000", output: testTime{111, 11, 11, 0, true}},
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
		{input: "2000-01-01 12:34:58.000000", norm: "00:00:00.000000", err: true},
		{input: "20000101", norm: "838:59:59.000000", output: testTime{838, 59, 59, 0, false}, err: true},
		{input: "-20000101", norm: "-838:59:59.000000", output: testTime{838, 59, 59, 0, true}, err: true},
		{input: "999995959", norm: "838:59:59.000000", output: testTime{838, 59, 59, 0, false}, err: true},
		{input: "-999995959", norm: "-838:59:59.000000", output: testTime{838, 59, 59, 0, true}, err: true},
		{input: "4294965959", norm: "838:59:59.000000", output: testTime{838, 59, 59, 0, false}, err: true},
		{input: "-4294965959", norm: "-838:59:59.000000", output: testTime{838, 59, 59, 0, true}, err: true},
		{input: "4294975959", norm: "00:00:00.000000", err: true},
		{input: "-4294975959", norm: "00:00:00.000000", err: true},
	}

	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			got, l, ok := ParseTime(test.input, -1)
			if test.err {
				assert.Equal(t, test.output.hour, got.Hour())
				assert.Equal(t, test.output.minute, got.Minute())
				assert.Equal(t, test.output.second, got.Second())
				assert.Equal(t, test.output.nanosecond, got.Nanosecond())
				assert.Equal(t, test.norm, string(got.AppendFormat(nil, 6)))
				assert.Equal(t, test.l, l)
				assert.Falsef(t, ok, "did not fail to parse %s", test.input)
				return
			}

			require.True(t, ok)
			assert.Equal(t, test.output.hour, got.Hour())
			assert.Equal(t, test.output.minute, got.Minute())
			assert.Equal(t, test.output.second, got.Second())
			assert.Equal(t, test.output.nanosecond, got.Nanosecond())
			assert.Equal(t, test.l, l)
			assert.Equal(t, test.norm, string(got.AppendFormat(nil, 6)))
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
