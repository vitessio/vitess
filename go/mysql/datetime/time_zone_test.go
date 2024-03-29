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

func TestDST(t *testing.T) {
	testCases := []struct {
		time     Time
		year     int
		month    time.Month
		day      int
		tz       string
		expected string
	}{
		{
			time: Time{hour: 130, minute: 34, second: 58},
			year: 2023, month: 10, day: 24,
			tz:       "Europe/Madrid",
			expected: "2023-10-29T10:34:58+01:00",
		},
		{
			time: Time{hour: 130, minute: 34, second: 58},
			year: 2023, month: 10, day: 29,
			tz:       "Europe/Madrid",
			expected: "2023-11-03T10:34:58+01:00",
		},
		{
			time: Time{hour: 130 | negMask, minute: 34, second: 58},
			year: 2023, month: 11, day: 03,
			tz:       "Europe/Madrid",
			expected: "2023-10-28T13:25:02+02:00",
		},
	}

	for _, tc := range testCases {
		tz, err := ParseTimeZone(tc.tz)
		require.NoError(t, err)

		got := tc.time.toStdTime(tc.year, tc.month, tc.day, tz)
		assert.Equal(t, tc.expected, got.Format(time.RFC3339))
	}
}

func TestParseTimeZone(t *testing.T) {
	testCases := []struct {
		tz   string
		want string
	}{
		{
			tz:   "Europe/Amsterdam",
			want: "Europe/Amsterdam",
		},
		{
			tz:   "",
			want: "Unknown or incorrect time zone: ''",
		},
		{
			tz:   "+02:00",
			want: "UTC+02:00",
		},
		{
			tz:   "+14:00",
			want: "UTC+14:00",
		},
		{
			tz:   "+14:01",
			want: "Unknown or incorrect time zone: '+14:01'",
		},
		{
			tz:   "-13:59",
			want: "UTC-13:59",
		},
		{
			tz:   "-14:00",
			want: "Unknown or incorrect time zone: '-14:00'",
		},
		{
			tz:   "-15:00",
			want: "Unknown or incorrect time zone: '-15:00'",
		},
		{
			tz:   "foo",
			want: "Unknown or incorrect time zone: 'foo'",
		},
	}

	for _, tc := range testCases {

		zone, err := ParseTimeZone(tc.tz)
		if err != nil {
			assert.Equal(t, tc.want, err.Error())
		} else {
			assert.Equal(t, tc.want, zone.String())
		}
	}
}
