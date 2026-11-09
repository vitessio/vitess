/*
Copyright 2026 The Vitess Authors.

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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCheckDateAndDateTime checks CheckDate and CheckDateTime against the verdicts
// MySQL 8.4 gives for `SET sql_mode = <mode>; SELECT DATE '<s>'` (or TIMESTAMP '<s>'):
// "ok" when the literal is accepted, "ERR" when it fails with ER_WRONG_VALUE (1525).
func TestCheckDateAndDateTime(t *testing.T) {
	// The columns of want, in order.
	modes := []struct {
		name  string
		flags CheckFlags
	}{
		{"DEFAULT (NO_ZERO_IN_DATE,NO_ZERO_DATE)", CheckNoZeroInDate | CheckNoZeroDate},
		{"''", 0},
		{"NO_ZERO_DATE", CheckNoZeroDate},
		{"NO_ZERO_IN_DATE", CheckNoZeroInDate},
		{"ALLOW_INVALID_DATES", CheckAllowInvalidDates},
		{"ALLOW_INVALID_DATES,NO_ZERO_IN_DATE", CheckAllowInvalidDates | CheckNoZeroInDate},
	}

	dates := []struct {
		in   string
		want string
	}{
		{"0000-00-00", "ERR ok  ERR ok  ok  ok"},
		{"2020-00-15", "ERR ok  ok  ERR ok  ERR"},
		{"2020-01-00", "ERR ok  ok  ERR ok  ERR"},
		{"2020-00-00", "ERR ok  ok  ERR ok  ERR"},
		{"0000-00-15", "ERR ok  ok  ERR ok  ERR"},
		{"0000-01-01", "ok  ok  ok  ok  ok  ok"},
		{"0000-00-01", "ERR ok  ok  ERR ok  ERR"},
		{"2020-02-31", "ERR ERR ERR ERR ok  ok"},
		{"0000-02-31", "ERR ERR ERR ERR ok  ok"},
		{"2020-02-29", "ok  ok  ok  ok  ok  ok"},
		{"2021-02-29", "ERR ERR ERR ERR ok  ok"},
		{"2020-04-31", "ERR ERR ERR ERR ok  ok"},
		{"2020-02-32", "ERR ERR ERR ERR ERR ERR"},
		{"2020-13-01", "ERR ERR ERR ERR ERR ERR"},
		{"2020-00-31", "ERR ok  ok  ERR ok  ERR"},
		{"00-00-00", "ERR ok  ERR ok  ok  ok"},
		{"00-01-01", "ok  ok  ok  ok  ok  ok"},
		{"000000", "ERR ok  ERR ok  ok  ok"},
		{"00000000", "ERR ok  ERR ok  ok  ok"},
		{"20200015", "ERR ok  ok  ERR ok  ERR"},
		{"20200231", "ERR ERR ERR ERR ok  ok"},
		{"2020-1-1", "ok  ok  ok  ok  ok  ok"},
		{"foo", "ERR ERR ERR ERR ERR ERR"},
	}
	datetimes := []struct {
		in   string
		want string
	}{
		{"0000-00-00 00:00:00", "ERR ok  ERR ok  ok  ok"},
		{"0000-00-00 00:00:01", "ERR ok  ok  ERR ok  ERR"},
		{"0000-00-00 00:00:00.000", "ERR ok  ERR ok  ok  ok"},
		{"0000-00-00 00:00:00.001", "ERR ok  ok  ERR ok  ERR"},
		{"2020-00-15 10:00:00", "ERR ok  ok  ERR ok  ERR"},
		{"2020-02-31 10:00:00", "ERR ERR ERR ERR ok  ok"},
		{"2020-02-31 25:00:00", "ERR ERR ERR ERR ERR ERR"},
		{"00000000000000", "ERR ok  ERR ok  ok  ok"},
		{"20200015000000", "ERR ok  ok  ERR ok  ERR"},
		{"00-00-00 00:00:00", "ERR ok  ERR ok  ok  ok"},
		{"2020-01-01 10:00:00", "ok  ok  ok  ok  ok  ok"},
		{"foo", "ERR ERR ERR ERR ERR ERR"},
	}

	run := func(t *testing.T, check func(string, CheckFlags) bool, in, want string) {
		verdicts := strings.Fields(want)
		require.Len(t, verdicts, len(modes))
		for i, mode := range modes {
			assert.Equal(t, verdicts[i] == "ok", check(in, mode.flags), "%q under sql_mode %s", in, mode.name)
		}
	}
	for _, tc := range dates {
		t.Run("DATE "+tc.in, func(t *testing.T) {
			run(t, CheckDate, tc.in, tc.want)
		})
	}
	for _, tc := range datetimes {
		t.Run("TIMESTAMP "+tc.in, func(t *testing.T) {
			run(t, CheckDateTime, tc.in, tc.want)
		})
	}
}
