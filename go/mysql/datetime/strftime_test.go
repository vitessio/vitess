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
	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	in := "%a-%Y"
	res, err := New(in)
	assert.NoError(t, err)
	assert.Equal(t, res.Pattern(), in)
	assert.Equal(t, res.compiled[0].format([]byte{}, DateTime{Date: Date{2024, 3, 15}}, 1), []byte("Fri"))
	assert.Equal(t, res.compiled[1].format([]byte{}, DateTime{Date: Date{2024, 3, 15}}, 1), []byte("-"))
	assert.Equal(t, res.compiled[2].format([]byte{}, DateTime{Date: Date{2024, 3, 15}}, 1), []byte("2024"))

	in = "%"
	res, err = New(in)
	assert.Nil(t, res)
	assert.Error(t, err)

	in = "-"
	res, err = New(in)
	assert.NoError(t, err)
	assert.Equal(t, res.Pattern(), in)
	assert.Equal(t, res.compiled[0].format([]byte{}, DateTime{Date: Date{2024, 3, 15}}, 1), []byte("-"))
}

func TestStrftimeFormat(t *testing.T) {
	testCases := []struct {
		in              string
		want_YYYY_MM_DD string
		want_YYYY_M_D   string
	}{
		{"1999-12-31 23:59:58.999", "1999-12-31", "1999-12-31"},
		{"2000-01-02 03:04:05", "2000-01-02", "2000-1-2"},
		{"2001-01-01 01:04:05", "2001-01-01", "2001-1-1"},
	}

	for _, tc := range testCases {
		t.Run(tc.in, func(t *testing.T) {
			dt, _, ok := ParseDateTime(tc.in, -1)
			require.True(t, ok)

			got := Date_YYYY_MM_DD.Format(dt, 6)
			assert.Equal(t, []byte(tc.want_YYYY_MM_DD), got)

			got = Date_YYYY_M_D.Format(dt, 6)
			assert.Equal(t, []byte(tc.want_YYYY_M_D), got)

			res := Date_YYYY_MM_DD.FormatString(dt, 6)
			assert.Equal(t, tc.want_YYYY_MM_DD, res)

			res = Date_YYYY_M_D.FormatString(dt, 6)
			assert.Equal(t, tc.want_YYYY_M_D, res)

			dst := []byte("test: ")
			b := Date_YYYY_MM_DD.AppendFormat(dst, dt, 6)
			want := append([]byte("test: "), []byte(tc.want_YYYY_MM_DD)...)
			assert.Equal(t, want, b)
		})
	}
}

func TestFormatNumeric(t *testing.T) {
	in := "%Y%h%H%s%d"
	res, err := New(in)
	require.NoError(t, err)

	testCases := []struct {
		dt   string
		want int64
	}{
		{"1999-12-31 23:59:58.999", 199911235831},
		{"2000-01-02 03:04:05", 200003030502},
		{"2001-01-01 01:04:05", 200101010501},
	}

	for _, tc := range testCases {
		dt, _, ok := ParseDateTime(tc.dt, -1)
		require.True(t, ok)

		n := res.FormatNumeric(dt)
		assert.Equal(t, tc.want, n)
	}
}
