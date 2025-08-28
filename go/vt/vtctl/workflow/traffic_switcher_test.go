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
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type testTrafficSwitcher struct {
	trafficSwitcher
	sourceKeyspaceSchema *vindexes.KeyspaceSchema
}

func (tts *testTrafficSwitcher) SourceKeyspaceSchema() *vindexes.KeyspaceSchema {
	return tts.sourceKeyspaceSchema
}

func TestReverseWorkflowName(t *testing.T) {
	tests := []struct {
		in  string
		out string
	}{
		{
			in:  "aa",
			out: "aa_reverse",
		},
		{
			in:  "aa_reverse",
			out: "aa",
		},
		{
			in:  "aa_reverse_aa",
			out: "aa_reverse_aa_reverse",
		},
	}
	for _, test := range tests {
		got := ReverseWorkflowName(test.in)
		assert.Equal(t, test.out, got)
	}
}

// TestSwitchTrafficPositionHandling confirms that if any writes are somehow
// executed against the source between the stop source writes and wait for
// catchup steps, that we have the correct position and do not lose the write(s).
func TestTrafficSwitchPositionHandling(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	workflowName := "wf1"
	tableName := "t1"
	sourceKeyspaceName := "sourceks"
	targetKeyspaceName := "targetks"

	schema := map[string]*tabletmanagerdatapb.SchemaDefinition{
		tableName: {
			TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
				{
					Name:   tableName,
					Schema: fmt.Sprintf("CREATE TABLE %s (id BIGINT, name VARCHAR(64), PRIMARY KEY (id))", tableName),
				},
			},
		},
	}

	sourceKeyspace := &testKeyspace{
		KeyspaceName: sourceKeyspaceName,
		ShardNames:   []string{"0"},
	}
	targetKeyspace := &testKeyspace{
		KeyspaceName: targetKeyspaceName,
		ShardNames:   []string{"0"},
	}

	env := newTestEnv(t, ctx, defaultCellName, sourceKeyspace, targetKeyspace)
	defer env.close()
	env.tmc.schema = schema

	ts, _, err := env.ws.getWorkflowState(ctx, targetKeyspaceName, workflowName)
	require.NoError(t, err)
	sw := &switcher{ts: ts, s: env.ws}

	lockCtx, sourceUnlock, lockErr := sw.lockKeyspace(ctx, ts.SourceKeyspaceName(), "test")
	require.NoError(t, lockErr)
	ctx = lockCtx
	defer sourceUnlock(&err)
	lockCtx, targetUnlock, lockErr := sw.lockKeyspace(ctx, ts.TargetKeyspaceName(), "test")
	require.NoError(t, lockErr)
	ctx = lockCtx
	defer targetUnlock(&err)

	err = ts.stopSourceWrites(ctx)
	require.NoError(t, err)

	// Now we simulate a write on the source.
	newPosition := position[:strings.LastIndex(position, "-")+1]
	oldSeqNo, err := strconv.Atoi(position[strings.LastIndex(position, "-")+1:])
	require.NoError(t, err)
	newPosition = fmt.Sprintf("%s%d", newPosition, oldSeqNo+1)
	env.tmc.setPrimaryPosition(env.tablets[sourceKeyspaceName][startingSourceTabletUID], newPosition)

	// And confirm that we picked up the new position.
	err = ts.gatherSourcePositions(ctx)
	require.NoError(t, err)
	err = ts.ForAllSources(func(ms *MigrationSource) error {
		require.Equal(t, newPosition, ms.Position)
		return nil
	})
	require.NoError(t, err)
}
