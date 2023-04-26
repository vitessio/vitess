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

package sqlparser

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
	}{{
		input:  "2022-10-12",
		output: date{2022, time.October, 12},
	}, {
		input:  "22-10-12",
		output: date{2022, time.October, 12},
	}, {
		input:  "20221012",
		output: date{2022, time.October, 12},
	}, {
		input:  "221012",
		output: date{2022, time.October, 12},
	}, {
		input: "2022",
		err:   true,
	}}

	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			got, err := ParseDate(test.input)
			if test.err {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
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
		err    bool
	}{{
		input:  "11:12:13",
		output: testTime{11, 12, 13, 0},
	}, {
		input:  "11:12:13.123456",
		output: testTime{11, 12, 13, 123456000},
	}, {
		input:  "3 11:12:13",
		output: testTime{3*24 + 11, 12, 13, 0},
	}, {
		input: "35 11:12:13",
		err:   true,
	}, {
		input:  "11:12",
		output: testTime{11, 12, 0, 0},
	}, {
		input:  "5 11:12",
		output: testTime{5*24 + 11, 12, 0, 0},
	}, {
		input:  "-2 11:12",
		output: testTime{-2*24 - 11, -12, 0, 0},
	}, {
		input: "--2 11:12",
		err:   true,
	}, {
		input:  "2 11",
		output: testTime{2*24 + 11, 0, 0, 0},
	}, {
		input: "2 -11",
		err:   true,
	}, {
		input:  "13",
		output: testTime{0, 0, 13, 0},
	}, {
		input:  "111213",
		output: testTime{11, 12, 13, 0},
	}, {
		input:  "111213.123456",
		output: testTime{11, 12, 13, 123456000},
	}, {
		input:  "-111213",
		output: testTime{-11, -12, -13, 0},
	}, {
		input:  "1213",
		output: testTime{0, 12, 13, 0},
	}, {
		input: "25:12:13",
		err:   true,
	}}

	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			got, err := ParseTime(test.input)
			if test.err {
				assert.Errorf(t, err, "got: %s", got)
				return
			}

			require.NoError(t, err)
			now := time.Now()
			startOfToday := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
			expected := startOfToday.Add(time.Duration(test.output.hour)*time.Hour +
				time.Duration(test.output.minute)*time.Minute +
				time.Duration(test.output.second)*time.Second +
				time.Duration(test.output.nanosecond)*time.Nanosecond)

			assert.Equal(t, expected, got)
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
	}{{
		input:  "2022-10-12 11:12:13",
		output: datetime{2022, time.October, 12, 11, 12, 13, 0},
	}, {
		input:  "2022-10-12 11:12:13.123456",
		output: datetime{2022, time.October, 12, 11, 12, 13, 123456000},
	}, {
		input:  "20221012111213.123456",
		output: datetime{2022, time.October, 12, 11, 12, 13, 123456000},
	}}

	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			got, err := ParseDateTime(test.input)
			if test.err {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
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
