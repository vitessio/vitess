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

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFormattingFromMySQL(t *testing.T) {
	const FormatString = `%a %b %c %D %d %e %f %H %h %I %i %j %k %l %M %m %p %r %S %s %T %U %u %V %v %W %w %X %x %Y %y %%`

	var cases = []struct {
		timestamp string
		output    string
	}{
		{
			`1999-12-31 23:59:58.999`,
			`Fri Dec 12 31st 31 31 999000 23 11 11 59 365 23 11 December 12 PM 11:59:58 PM 58 58 23:59:58 52 52 52 52 Friday 5 1999 1999 1999 99 %`,
		},
		{
			`2000-01-02 03:04:05`,
			`Sun Jan 1 2nd 02 2 000000 03 03 03 04 002 3 3 January 01 AM 03:04:05 AM 05 05 03:04:05 01 00 01 52 Sunday 0 2000 1999 2000 00 %`,
		},
		{
			`2001-01-01 01:04:05`,
			`Mon Jan 1 1st 01 1 000000 01 01 01 04 001 1 1 January 01 AM 01:04:05 AM 05 05 01:04:05 00 01 53 01 Monday 1 2000 2001 2001 01 %`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.timestamp, func(t *testing.T) {
			dt, ok := ParseDateTime(tc.timestamp)
			require.True(t, ok)

			eval, err := Format(FormatString, dt, 6)
			require.NoError(t, err)

			require.Equal(t, tc.output, string(eval))
		})
	}
}
