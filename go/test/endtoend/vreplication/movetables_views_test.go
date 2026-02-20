/*
Copyright 2025 The Vitess Authors.

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

package vreplication

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	workflowpkg "vitess.io/vitess/go/vt/vtctl/workflow"
)

const (
	viewsSourceKs = "source"
	viewsTargetKs = "target"
)

var (
	viewsInitialSchema = `
create table customer(id int, name varchar(128), primary key(id));
create view customer_view as select id, name from customer;
`
	viewsInitialSourceVSchema = `
{
  "tables": {
	"customer": {}
  }
}
`
	viewsInitialTargetVSchema = `
{
  "sharded": true,
  "vindexes": {
    "reverse_bits": {
      "type": "reverse_bits"
    }
  },
  "tables": {
    "customer": {
      "column_vindexes": [
        {
          "column": "id",
          "name": "reverse_bits"
        }
      ]
    }
  }
}
`
)

// TestMoveTablesViews tests that MoveTables correctly handles views.
func TestMoveTablesViews(t *testing.T) {
	moveTables, vc := setupViewsMoveTables(t, nil)
	defer vc.TearDown()

	vtgateConn := vc.GetVTGateConn(t)
	defer vtgateConn.Close()

	// Wait for the vtgate to pick up the new view on the target from the schema tracker.
	require.Eventually(t, func() bool {
		_, err := vtgateConn.ExecuteFetch("select * from "+viewsTargetKs+".customer_view", 100, false)
		return err == nil
	}, 5*time.Second, 1*time.Second)

	// Validate that the queries get routed to the source keyspace correctly.
	confirmKeyspacesRoutedTo(t, viewsSourceKs, viewsSourceKs, "customer_view", nil)
	confirmKeyspacesRoutedTo(t, viewsTargetKs, viewsSourceKs, "customer_view", nil)

	// Switch traffic to the target keyspace.
	moveTables.SwitchReadsAndWrites()

	// Validate that the queries get routed to the target keyspace correctly.
	confirmKeyspacesRoutedTo(t, viewsSourceKs, viewsTargetKs, "customer_view", nil)
	confirmKeyspacesRoutedTo(t, viewsTargetKs, viewsTargetKs, "customer_view", nil)

	// Complete the workflow, which will delete the view on the source keyspace.
	moveTables.Complete()

	// View/table should not exist on the source.
	query := fmt.Sprintf("select * from %s.customer_view", viewsSourceKs)
	_, err := vtgateConn.ExecuteFetch(query, 100, false)
	require.Error(t, err)
	require.True(t, workflowpkg.IsTableDidNotExistError(err))

	// Validate that the view does not exist directly on the source tablet.
	sourceTablet := vc.getPrimaryTablet(t, viewsSourceKs, "0")
	qr, err := sourceTablet.QueryTablet("show tables like 'customer_view'", viewsSourceKs, true)
	require.NoError(t, err)
	require.Zero(t, len(qr.Rows), "view should not exist on source tablet")

	// Validate that the view is not in the _vt.views table on the source tablet.
	qr, err = sourceTablet.QueryTablet(
		fmt.Sprintf("select * from _vt.views where TABLE_SCHEMA = 'vt_%s' and TABLE_NAME = 'customer_view'", viewsSourceKs),
		"", true)
	require.NoError(t, err)
	require.Zero(t, len(qr.Rows), "view should not exist in _vt.views on source tablet")
}

// TestMoveTablesViewsRenameTables tests that MoveTables correctly renames
// views on the source when using the --rename-tables flag.
func TestMoveTablesViewsRenameTables(t *testing.T) {
	completeFlags := []string{"--rename-tables"}
	moveTables, vc := setupViewsMoveTables(t, completeFlags)
	defer vc.TearDown()

	vtgateConn := vc.GetVTGateConn(t)
	defer vtgateConn.Close()

	// Wait for the vtgate to pick up the new view on the target from the schema tracker.
	require.Eventually(t, func() bool {
		_, err := vtgateConn.ExecuteFetch("select * from "+viewsTargetKs+".customer_view", 100, false)
		return err == nil
	}, 5*time.Second, 1*time.Second)

	// Switch traffic and complete the workflow with rename-tables.
	moveTables.SwitchReadsAndWrites()

	// Validate that the queries get routed to the target keyspace correctly.
	confirmKeyspacesRoutedTo(t, viewsSourceKs, viewsTargetKs, "customer_view", nil)
	confirmKeyspacesRoutedTo(t, viewsTargetKs, viewsTargetKs, "customer_view", nil)

	moveTables.Complete()

	// Validate that the original view does not exist on the source.
	sourceTablet := vc.getPrimaryTablet(t, viewsSourceKs, "0")
	qr, err := sourceTablet.QueryTablet("show tables like 'customer_view'", viewsSourceKs, true)
	require.NoError(t, err)
	require.Zero(t, len(qr.Rows), "view should not exist on source tablet")

	// Validate that the renamed view exists on the source.
	qr, err = sourceTablet.QueryTablet("show tables like '_customer_view_old'", viewsSourceKs, true)
	require.NoError(t, err)
	require.Equal(t, 1, len(qr.Rows), "renamed view should exist on source tablet")
}

// TestMoveTablesViewsKeepData tests that MoveTables correctly keeps
// views on the source when using the --keep-data flag.
func TestMoveTablesViewsKeepData(t *testing.T) {
	completeFlags := []string{"--keep-data"}
	moveTables, vc := setupViewsMoveTables(t, completeFlags)
	defer vc.TearDown()

	vtgateConn := vc.GetVTGateConn(t)
	defer vtgateConn.Close()

	// Wait for the vtgate to pick up the new view on the target from the schema tracker.
	require.Eventually(t, func() bool {
		_, err := vtgateConn.ExecuteFetch("select * from "+viewsTargetKs+".customer_view", 100, false)
		return err == nil
	}, 5*time.Second, 1*time.Second)

	// Switch traffic and complete the workflow with keep-data.
	moveTables.SwitchReadsAndWrites()

	// Validate that the queries get routed to the target keyspace correctly.
	confirmKeyspacesRoutedTo(t, viewsSourceKs, viewsTargetKs, "customer_view", nil)
	confirmKeyspacesRoutedTo(t, viewsTargetKs, viewsTargetKs, "customer_view", nil)

	moveTables.Complete()

	// Validate that the view still exists on the source tablet.
	sourceTablet := vc.getPrimaryTablet(t, viewsSourceKs, "0")
	qr, err := sourceTablet.QueryTablet("show tables like 'customer_view'", viewsSourceKs, true)
	require.NoError(t, err)
	require.Equal(t, 1, len(qr.Rows), "view should still exist on source tablet")

	// Validate that the view is still in the _vt.views table on the source tablet.
	qr, err = sourceTablet.QueryTablet(
		fmt.Sprintf("select * from _vt.views where TABLE_SCHEMA = 'vt_%s' and TABLE_NAME = 'customer_view'", viewsSourceKs),
		"", true)
	require.NoError(t, err)
	require.Equal(t, 1, len(qr.Rows), "view should still exist in _vt.views on source tablet")
}

// TestMoveTablesViewValidation tests that MoveTables fails when trying to move a view without its
// underlying table.
func TestMoveTablesViewValidation(t *testing.T) {
	// Enable views on the vttablets
	origExtraVTTabletArgs := extraVTTabletArgs
	extraVTTabletArgs = append(extraVTTabletArgs, "--queryserver-enable-views")
	t.Cleanup(func() {
		extraVTTabletArgs = origExtraVTTabletArgs
	})

	setSidecarDBName("_vt")
	vc := NewVitessCluster(t, nil)
	defer vc.TearDown()

	// Create source keyspace with two tables and a view that references only one of them.
	cell := vc.Cells[vc.CellNames[0]]
	schema := `
		create table t1(id int, primary key(id));
		create table t2(id int, primary key(id));
		create view v1 as select id from t1;
	`
	vschema := `{"tables": {"t1": {}, "t2": {}}}`
	vc.AddKeyspace(t, []*Cell{cell}, viewsSourceKs, "0", vschema, schema, 0, 0, 100, nil)
	verifyClusterHealth(t, vc)

	// Create empty target keyspace.
	targetVSchema := `{"tables": {"t2": {}}}`
	vc.AddKeyspace(t, []*Cell{cell}, viewsTargetKs, "0", targetVSchema, "", 0, 0, 200, nil)

	// Try to move only t2 and v1 (but v1 references t1, which is not being moved).
	// This should fail validation.
	workflowName := "wfvalidation"
	output, err := vc.VtctldClient.ExecuteCommandWithOutput(
		"MoveTables",
		"--workflow="+workflowName,
		"--target-keyspace="+viewsTargetKs,
		"Create",
		"--source-keyspace="+viewsSourceKs,
		"--tables=t2",
		"--views=v1",
	)
	require.Error(t, err, "expected MoveTables to fail due to view validation")
	require.Contains(t, output, "view references table not being moved",
		"expected error message about view referencing table not being moved")
}

// TestMoveTablesViewValidationWithTargetTable tests that MoveTables succeeds when moving a view
// that references a table already present on the target (from a previous migration).
func TestMoveTablesViewValidationWithTargetTable(t *testing.T) {
	// Enable views on the vttablets
	origExtraVTTabletArgs := extraVTTabletArgs
	extraVTTabletArgs = append(extraVTTabletArgs, "--queryserver-enable-views")
	t.Cleanup(func() {
		extraVTTabletArgs = origExtraVTTabletArgs
	})

	setSidecarDBName("_vt")
	vc := NewVitessCluster(t, nil)
	defer vc.TearDown()

	// Create source keyspace with two tables and a view that references only one of them.
	cell := vc.Cells[vc.CellNames[0]]
	schema := `
		create table t1(id int, primary key(id));
		create table t2(id int, primary key(id));
		create view v1 as select id from t1;
	`
	vschema := `{"tables": {"t1": {}, "t2": {}}}`
	vc.AddKeyspace(t, []*Cell{cell}, viewsSourceKs, "0", vschema, schema, 0, 0, 100, nil)
	verifyClusterHealth(t, vc)

	// Create target keyspace with t1 already present (simulating a previous migration).
	targetSchema := `create table t1(id int, primary key(id));`
	targetVSchema := `{"tables": {"t1": {}, "t2": {}}}`
	vc.AddKeyspace(t, []*Cell{cell}, viewsTargetKs, "0", targetVSchema, targetSchema, 0, 0, 200, nil)

	// Move t2 and v1. v1 references t1, which exists on target but is not being moved.
	// This should succeed because validation considers tables already on the target.
	workflowName := "wfvalidation"
	_, err := vc.VtctldClient.ExecuteCommandWithOutput(
		"MoveTables",
		"--workflow="+workflowName,
		"--target-keyspace="+viewsTargetKs,
		"Create",
		"--source-keyspace="+viewsSourceKs,
		"--tables=t2",
		"--views=v1",
	)
	require.NoError(t, err, "expected MoveTables to succeed because t1 exists on target")
}

func setupViewsMoveTables(t *testing.T, completeFlags []string) (iMoveTables, *VitessCluster) {
	workflowName := "wfviews"

	// Enable views on the vttablets
	origExtraVTTabletArgs := extraVTTabletArgs
	extraVTTabletArgs = append(extraVTTabletArgs, "--queryserver-enable-views")
	t.Cleanup(func() {
		extraVTTabletArgs = origExtraVTTabletArgs
	})

	setSidecarDBName("_vt")
	vc = NewVitessCluster(t, nil)

	// Create source keyspace with table and view (with data).
	cell := vc.Cells[vc.CellNames[0]]
	vc.AddKeyspace(t, []*Cell{cell}, viewsSourceKs, "0", viewsInitialSourceVSchema, viewsInitialSchema, 0, 0, 100, nil)
	verifyClusterHealth(t, vc)

	vtgateConn := vc.GetVTGateConn(t)
	defer vtgateConn.Close()
	execVtgateQuery(t, vtgateConn, viewsSourceKs, "insert into customer(id, name) values(1, 'a'), (2, 'b'), (3, 'c')")
	waitForRowCount(t, vtgateConn, viewsSourceKs, "customer", 3)
	waitForRowCount(t, vtgateConn, viewsSourceKs, "customer_view", 3)

	// Create empty target keyspace.
	vc.AddKeyspace(t, []*Cell{cell}, viewsTargetKs, "-80,80-", viewsInitialTargetVSchema, "", 0, 0, 200, nil)

	moveTables := newMoveTables(vc, &moveTablesWorkflow{
		workflowInfo: &workflowInfo{
			vc:             vc,
			workflowName:   workflowName,
			targetKeyspace: viewsTargetKs,
		},
		sourceKeyspace: viewsSourceKs,
		tables:         "customer",
		createFlags:    []string{"--all-views"},
		completeFlags:  completeFlags,
	}, workflowFlavorVtctld)
	moveTables.Create()

	ksWorkflow := fmt.Sprintf("%s.%s", viewsTargetKs, workflowName)
	waitForWorkflowState(t, vc, ksWorkflow, binlogdatapb.VReplicationWorkflowState_Running.String())

	return moveTables, vc
}
