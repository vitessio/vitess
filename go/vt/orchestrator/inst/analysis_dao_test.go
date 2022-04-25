/*
Copyright 2022 The Vitess Authors.

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

package inst

import (
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/orchestrator/db"
	"vitess.io/vitess/go/vt/orchestrator/external/golib/sqlutils"
	"vitess.io/vitess/go/vt/orchestrator/test"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestGetReplicationAnalysis(t *testing.T) {
	tests := []struct {
		name       string
		info       *test.InfoForRecoveryAnalysis
		codeWanted AnalysisCode
		wantErr    string
	}{
		{
			name: "ClusterHasNoPrimary",
			info: &test.InfoForRecoveryAnalysis{
				TabletInfo: &topodatapb.Tablet{
					Alias:         &topodatapb.TabletAlias{Cell: "zon1", Uid: 100},
					Hostname:      "localhost",
					Keyspace:      "ks",
					Shard:         "0",
					Type:          topodatapb.TabletType_REPLICA,
					MysqlHostname: "localhost",
					MysqlPort:     6709,
				},
				LastCheckValid: 1,
			},
			codeWanted: ClusterHasNoPrimary,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.info.SetValuesFromTabletInfo()
			db.Db = test.NewTestDB([][]sqlutils.RowMap{{
				tt.info.ConvertToRowMap(),
			}})

			got, err := GetReplicationAnalysis("", &ReplicationAnalysisHints{})
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Len(t, got, 1)
			require.Equal(t, tt.codeWanted, got[0].Analysis)
		})
	}
}
