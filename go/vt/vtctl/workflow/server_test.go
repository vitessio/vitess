/*
Copyright 2021 The Vitess Authors.

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

package workflow

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/prototext"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

type fakeTMC struct {
	tmclient.TabletManagerClient
	vrepQueriesByTablet map[string]map[string]*querypb.QueryResult
}

func (fake *fakeTMC) VReplicationExec(ctx context.Context, tablet *topodatapb.Tablet, query string) (*querypb.QueryResult, error) {
	alias := topoproto.TabletAliasString(tablet.Alias)
	tabletQueries, ok := fake.vrepQueriesByTablet[alias]
	if !ok {
		return nil, fmt.Errorf("no query map registered on fake for %s", alias)
	}

	p3qr, ok := tabletQueries[query]
	if !ok {
		return nil, fmt.Errorf("no result on fake for query %q on tablet %s", query, alias)
	}

	return p3qr, nil
}

func TestCheckReshardingJournalExistsOnTablet(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tablet := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "zone1",
			Uid:  100,
		},
	}
	journal := &binlogdatapb.Journal{
		Id:            1,
		MigrationType: binlogdatapb.MigrationType_SHARDS,
		Tables:        []string{"t1", "t2"},
	}
	journalBytes, err := prototext.Marshal(journal)
	require.NoError(t, err, "could not marshal journal %+v into bytes", journal)

	// get some bytes that will fail to unmarshal into a binlogdatapb.Journal
	tabletBytes, err := prototext.Marshal(tablet)
	require.NoError(t, err, "could not marshal tablet %+v into bytes", tablet)

	p3qr := sqltypes.ResultToProto3(sqltypes.MakeTestResult([]*querypb.Field{
		{
			Name: "val",
			Type: querypb.Type_BLOB,
		},
	}, string(journalBytes)))

	tests := []struct {
		name        string
		tablet      *topodatapb.Tablet
		result      *querypb.QueryResult
		journal     *binlogdatapb.Journal
		shouldExist bool
		shouldErr   bool
	}{
		{
			name:        "journal exists",
			tablet:      tablet,
			result:      p3qr,
			shouldExist: true,
			journal:     journal,
		},
		{
			name:        "journal does not exist",
			tablet:      tablet,
			result:      sqltypes.ResultToProto3(sqltypes.MakeTestResult(nil)),
			journal:     &binlogdatapb.Journal{},
			shouldExist: false,
		},
		{
			name:   "cannot unmarshal into journal",
			tablet: tablet,
			result: sqltypes.ResultToProto3(sqltypes.MakeTestResult([]*querypb.Field{
				{
					Name: "val",
					Type: querypb.Type_BLOB,
				},
			}, string(tabletBytes))),
			shouldErr: true,
		},
		{
			name: "VReplicationExec fails on tablet",
			tablet: &topodatapb.Tablet{ // Here we use a different tablet to force the fake to return an error
				Alias: &topodatapb.TabletAlias{
					Cell: "zone2",
					Uid:  200,
				},
			},
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tmc := &fakeTMC{
				vrepQueriesByTablet: map[string]map[string]*querypb.QueryResult{
					topoproto.TabletAliasString(tablet.Alias): { // always use the tablet shared by these tests cases
						"select val from _vt.resharding_journal where id=1": tt.result,
					},
				},
			}

			ws := NewServer(nil, tmc)
			journal, exists, err := ws.CheckReshardingJournalExistsOnTablet(ctx, tt.tablet, 1)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)

			existAssertionMsg := "expected journal to "
			if tt.shouldExist {
				existAssertionMsg += "already exist on tablet"
			} else {
				existAssertionMsg += "not exist"
			}

			assert.Equal(t, tt.shouldExist, exists, existAssertionMsg)
			utils.MustMatch(t, tt.journal, journal, "journal in resharding_journal did not match")
		})
	}
}
