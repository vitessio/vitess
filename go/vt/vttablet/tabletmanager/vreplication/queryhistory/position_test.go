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

package queryhistory

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/replication"
)

func TestPosBetween(t *testing.T) {
	mustPos := func(s string) replication.Position {
		pos, err := replication.DecodePosition(s)
		require.NoError(t, err)
		return pos
	}
	posQuery := func(gtid string) string {
		return fmt.Sprintf("update _vt.vreplication set pos='%s', time_updated=42, transaction_timestamp=0, rows_copied=0, message='' where id=1", gtid)
	}

	const (
		uuidA = "02cd68df-5dd8-11f1-9a8b-12c5b0c400a7"
		uuidB = "18b5025a-cd79-f2ac-0000-000000000001"
	)

	tests := []struct {
		name        string
		lower       string
		upper       string
		query       string
		wantMatched bool
	}{
		{
			name:        "single source in range",
			lower:       "MySQL56/" + uuidA + ":1-100",
			upper:       "MySQL56/" + uuidA + ":1-110",
			query:       posQuery("MySQL56/" + uuidA + ":1-105"),
			wantMatched: true,
		},
		{
			name:        "single source at lower bound",
			lower:       "MySQL56/" + uuidA + ":1-100",
			upper:       "MySQL56/" + uuidA + ":1-110",
			query:       posQuery("MySQL56/" + uuidA + ":1-100"),
			wantMatched: true,
		},
		{
			name:        "single source at upper bound",
			lower:       "MySQL56/" + uuidA + ":1-100",
			upper:       "MySQL56/" + uuidA + ":1-110",
			query:       posQuery("MySQL56/" + uuidA + ":1-110"),
			wantMatched: true,
		},
		{
			// The exact scenario from issue #20220: a multi-source position
			// where the stored GTID lags the upper bound by one on the first
			// source.
			name:        "multi source in range",
			lower:       "MySQL56/" + uuidA + ":1-1491," + uuidB + ":101",
			upper:       "MySQL56/" + uuidA + ":1-1493," + uuidB + ":101",
			query:       posQuery("MySQL56/" + uuidA + ":1-1492," + uuidB + ":101"),
			wantMatched: true,
		},
		{
			// The vreplication test env can run with a MariaDB GTID flavor,
			// so the matcher must not assume a MySQL56 position prefix.
			name:        "mariadb flavor in range",
			lower:       "MariaDB/0-1-100",
			upper:       "MariaDB/0-1-110",
			query:       posQuery("MariaDB/0-1-105"),
			wantMatched: true,
		},
		{
			name:        "below lower bound",
			lower:       "MySQL56/" + uuidA + ":1-100",
			upper:       "MySQL56/" + uuidA + ":1-110",
			query:       posQuery("MySQL56/" + uuidA + ":1-99"),
			wantMatched: false,
		},
		{
			name:        "above upper bound",
			lower:       "MySQL56/" + uuidA + ":1-100",
			upper:       "MySQL56/" + uuidA + ":1-110",
			query:       posQuery("MySQL56/" + uuidA + ":1-111"),
			wantMatched: false,
		},
		{
			name:        "not a pos update query",
			lower:       "MySQL56/" + uuidA + ":1-100",
			upper:       "MySQL56/" + uuidA + ":1-110",
			query:       "update _vt.vreplication set state='Stopped' where id=1",
			wantMatched: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &posBetweenExpectation{lower: mustPos(tt.lower), upper: mustPos(tt.upper)}
			matched, err := e.MatchQuery(tt.query)
			require.NoError(t, err)
			require.Equal(t, tt.wantMatched, matched)
		})
	}
}
