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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseDate(t *testing.T) {
	type date struct {
		year  int
		month time.Month
		day   int
	}
	tests := []struct {
		input  string
		output date
		err    bool
	}{
		{input: "2022-10-12", output: date{2022, time.October, 12}},
		{input: "22-10-12", output: date{2022, time.October, 12}},
		{input: "20221012", output: date{2022, time.October, 12}},
		{input: "221012", output: date{2022, time.October, 12}},
		{input: "2012-12-31", output: date{2012, time.December, 31}},
		{input: "2012-1-1", output: date{2012, time.January, 1}},
		{input: "2012-12-1", output: date{2012, time.December, 1}},
		{input: "2012-1-11", output: date{2012, time.January, 11}},
		{input: "12-12-31", output: date{2012, time.December, 31}},
		{input: "12-1-1", output: date{2012, time.January, 1}},
		{input: "12-12-1", output: date{2012, time.December, 1}},
		{input: "12-1-11", output: date{2012, time.January, 11}},
		{input: "2012/12/31", output: date{2012, time.December, 31}},
		{input: "2012^12^31", output: date{2012, time.December, 31}},
		{input: "2012@12@31", output: date{2012, time.December, 31}},
		{input: "20070523", output: date{2007, time.May, 23}},
		{input: "070523", output: date{2007, time.May, 23}},
		{input: "071332", err: true},
		{input: "2022", err: true},
	}

	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			got, ok := ParseDate(test.input)
			if test.err {
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
	}
	tests := []struct {
		input  string
		output testTime
		norm   string
		err    bool
	}{
		{input: "11:12:13", norm: "11:12:13", output: testTime{11, 12, 13, 0}},
		{input: "11:12:13.123456", norm: "11:12:13.123456000", output: testTime{11, 12, 13, 123456000}},
		{input: "3 11:12:13", norm: "83:12:13", output: testTime{3*24 + 11, 12, 13, 0}},
		{input: "3 41:12:13", norm: "113:12:13", output: testTime{3*24 + 41, 12, 13, 0}},
		{input: "35 11:12:13", err: true},
		{input: "11:12", norm: "11:12:00", output: testTime{11, 12, 0, 0}},
		{input: "5 11:12", norm: "131:12:00", output: testTime{5*24 + 11, 12, 0, 0}},
		{input: "-2 11:12", norm: "-59:12:00", output: testTime{-2*24 - 11, -12, 0, 0}},
		{input: "--2 11:12", err: true},
		{input: "2 11", norm: "59:00:00", output: testTime{2*24 + 11, 0, 0, 0}},
		{input: "2 -11", err: true},
		{input: "13", norm: "00:00:13", output: testTime{0, 0, 13, 0}},
		{input: "111213", norm: "11:12:13", output: testTime{11, 12, 13, 0}},
		{input: "111213.123456", norm: "11:12:13.123456000", output: testTime{11, 12, 13, 123456000}},
		{input: "-111213", norm: "-11:12:13", output: testTime{-11, -12, -13, 0}},
		{input: "1213", norm: "00:12:13", output: testTime{0, 12, 13, 0}},
		{input: "25:12:13", norm: "25:12:13", output: testTime{25, 12, 13, 0}},
		{input: "32:35", norm: "32:35:00", output: testTime{32, 35, 0, 0}},
		{input: "101:34:58", norm: "101:34:58", output: testTime{101, 34, 58, 0}},
	}

	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			got, ok := ParseTime(test.input)
			if test.err {
				assert.Falsef(t, ok, "did not fail to parse %s", test.input)
				return
			}

			require.True(t, ok)
			now := time.Now()
			startOfToday := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
			expected := startOfToday.Add(time.Duration(test.output.hour)*time.Hour +
				time.Duration(test.output.minute)*time.Minute +
				time.Duration(test.output.second)*time.Second +
				time.Duration(test.output.nanosecond)*time.Nanosecond)

			assert.Equal(t, expected, got.Time)
			assert.Equal(t, test.norm, string(got.AppendFormat(nil, 9)))
		})
	}
}

func TestParseDateTime(t *testing.T) {
	type datetime struct {
		year       int
		month      time.Month
		day        int
		hour       int
		minute     int
		second     int
		nanosecond int
	}
	tests := []struct {
		input  string
		output datetime
		err    bool
	}{
		{input: "2022-10-12 11:12:13", output: datetime{2022, time.October, 12, 11, 12, 13, 0}},
		{input: "2022-10-12 11:12:13.123456", output: datetime{2022, time.October, 12, 11, 12, 13, 123456000}},
		{input: "20221012111213.123456", output: datetime{2022, time.October, 12, 11, 12, 13, 123456000}},
		{input: "2012-12-31 11:30:45", output: datetime{2012, time.December, 31, 11, 30, 45, 0}},
		{input: "2012^12^31 11+30+45", output: datetime{2012, time.December, 31, 11, 30, 45, 0}},
		{input: "2012/12/31 11*30*45", output: datetime{2012, time.December, 31, 11, 30, 45, 0}},
		{input: "2012@12@31 11^30^45", output: datetime{2012, time.December, 31, 11, 30, 45, 0}},
		{input: "2012-12-31T11:30:45", output: datetime{2012, time.December, 31, 11, 30, 45, 0}},
		{input: "20070523091528", output: datetime{2007, time.May, 23, 9, 15, 28, 0}},
		{input: "070523091528", output: datetime{2007, time.May, 23, 9, 15, 28, 0}},

		{input: "2042-07-07 07:07:07", output: datetime{2042, time.July, 7, 7, 7, 7, 0}},
		{input: "2042-7-07 07:07:07", output: datetime{2042, time.July, 7, 7, 7, 7, 0}},
		{input: "2042-07-7 07:07:07", output: datetime{2042, time.July, 7, 7, 7, 7, 0}},
		{input: "2042-07-07 7:07:07", output: datetime{2042, time.July, 7, 7, 7, 7, 0}},
		{input: "2042-07-07 07:7:07", output: datetime{2042, time.July, 7, 7, 7, 7, 0}},
		{input: "2042-07-07 07:07:7", output: datetime{2042, time.July, 7, 7, 7, 7, 0}},
		{input: "2042-7-7 7:7:7", output: datetime{2042, time.July, 7, 7, 7, 7, 0}},

		{input: "42-07-07 07:07:07", output: datetime{2042, time.July, 7, 7, 7, 7, 0}},
		{input: "42-7-07 07:07:07", output: datetime{2042, time.July, 7, 7, 7, 7, 0}},
		{input: "42-07-7 07:07:07", output: datetime{2042, time.July, 7, 7, 7, 7, 0}},
		{input: "42-07-07 7:07:07", output: datetime{2042, time.July, 7, 7, 7, 7, 0}},
		{input: "42-07-07 07:7:07", output: datetime{2042, time.July, 7, 7, 7, 7, 0}},
		{input: "42-07-07 07:07:7", output: datetime{2042, time.July, 7, 7, 7, 7, 0}},
		{input: "42-7-7 7:7:7", output: datetime{2042, time.July, 7, 7, 7, 7, 0}},
	}

	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			got, ok := ParseDateTime(test.input)
			if test.err {
				assert.Falsef(t, ok, "did not fail to parse %s", test.input)
				return
			}

			require.True(t, ok)
			assert.Equal(t, test.output.year, got.Year())
			assert.Equal(t, test.output.month, got.Month())
			assert.Equal(t, test.output.day, got.Day())
			assert.Equal(t, test.output.hour, got.Hour())
			assert.Equal(t, test.output.minute, got.Minute())
			assert.Equal(t, test.output.second, got.Second())
			assert.Equal(t, test.output.nanosecond, got.Nanosecond())
		})
	}
}
