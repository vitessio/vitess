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

package workflow

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

// TestCreateRejectsAllTablesWithIncludeList ensures the table-selection
// invariant is enforced at the RPC boundary, not only in vtctldclient:
// VTAdmin forwards a raw request and direct gRPC callers bypass the CLI
// checks, so all_tables=true combined with an explicit include list would
// otherwise silently move only the explicit subset.
func TestCreateRejectsAllTablesWithIncludeList(t *testing.T) {
	ctx := context.Background()
	s := &Server{}

	t.Run("MoveTablesCreate", func(t *testing.T) {
		_, err := s.MoveTablesCreate(ctx, &vtctldatapb.MoveTablesCreateRequest{
			Workflow:       "wf1",
			SourceKeyspace: "sourceks",
			TargetKeyspace: "targetks",
			AllTables:      true,
			IncludeTables:  []string{"t1"},
		})
		require.ErrorContains(t, err, "cannot specify both all_tables")
	})

	t.Run("MigrateCreate", func(t *testing.T) {
		_, err := s.MigrateCreate(ctx, &vtctldatapb.MigrateCreateRequest{
			Workflow:       "wf1",
			MountName:      "ext1",
			SourceKeyspace: "sourceks",
			TargetKeyspace: "targetks",
			AllTables:      true,
			IncludeTables:  []string{"t1"},
		})
		require.ErrorContains(t, err, "cannot specify both all_tables")
	})
}
