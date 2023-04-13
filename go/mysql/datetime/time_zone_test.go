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
)

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
