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

package vreplication

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"vitess.io/vitess/go/vt/sqlparser"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

type lookupVindex struct {
	typ                string
	name               string
	tableKeyspace      string
	table              string
	columns            []string
	ownerTable         string
	ownerTableKeyspace string
	ignoreNulls        bool

	t *testing.T
}

func (lv *lookupVindex) String() string {
	return lv.typ + " " + lv.name + " on " + lv.tableKeyspace + "." + lv.table + " (" + lv.columns[0] + ")"
}

func (lv *lookupVindex) create() {
	cols := strings.Join(lv.columns, ",")
	args := []string{
		"LookupVindex",
		"--name", lv.name,
		"--table-keyspace=" + lv.ownerTableKeyspace,
		"create",
		"--keyspace=" + lv.tableKeyspace,
		"--type=" + lv.typ,
		"--table-owner=" + lv.ownerTable,
		"--table-owner-columns=" + cols,
		"--tablet-types=PRIMARY",
	}
	if lv.ignoreNulls {
		args = append(args, "--ignore-nulls")
	}

	err := vc.VtctldClient.ExecuteCommand(args...)
	require.NoError(lv.t, err, "error executing LookupVindex create: %v", err)
	waitForWorkflowState(lv.t, vc, fmt.Sprintf("%s.%s", lv.ownerTableKeyspace, lv.name), binlogdatapb.VReplicationWorkflowState_Running.String())
	lv.expectParamsAndOwner(true)
}

func (lv *lookupVindex) cancel() {
	args := []string{
		"LookupVindex",
		"--name", lv.name,
		"--table-keyspace=" + lv.ownerTableKeyspace,
		"cancel",
	}
	err := vc.VtctldClient.ExecuteCommand(args...)
	require.NoError(lv.t, err, "error executing LookupVindex complete: %v", err)
}

func (lv *lookupVindex) externalize() {
	args := []string{
		"LookupVindex",
		"--name", lv.name,
		"--table-keyspace=" + lv.ownerTableKeyspace,
		"externalize",
		"--keyspace=" + lv.tableKeyspace,
	}
	err := vc.VtctldClient.ExecuteCommand(args...)
	require.NoError(lv.t, err, "error executing LookupVindex externalize: %v", err)
	lv.expectParamsAndOwner(false)
}

func (lv *lookupVindex) internalize() {
	args := []string{
		"LookupVindex",
		"--name", lv.name,
		"--table-keyspace=" + lv.ownerTableKeyspace,
		"internalize",
		"--keyspace=" + lv.tableKeyspace,
	}
	err := vc.VtctldClient.ExecuteCommand(args...)
	require.NoError(lv.t, err, "error executing LookupVindex internalize: %v", err)
	lv.expectParamsAndOwner(true)
}

func (lv *lookupVindex) complete() {
	args := []string{
		"LookupVindex",
		"--name", lv.name,
		"--table-keyspace=" + lv.ownerTableKeyspace,
		"complete",
		"--keyspace=" + lv.tableKeyspace,
	}
	err := vc.VtctldClient.ExecuteCommand(args...)
	require.NoError(lv.t, err, "error executing LookupVindex complete: %v", err)
	lv.expectParamsAndOwner(false)
}

func (lv *lookupVindex) show() error {
	return nil
}

func (lv *lookupVindex) expectParamsAndOwner(expectedWriteOnlyParam bool) {
	vschema, err := vc.VtctldClient.ExecuteCommandWithOutput("GetVSchema", lv.ownerTableKeyspace)
	require.NoError(lv.t, err, "error executing GetVSchema: %v", err)
	vdx := gjson.Get(vschema, "vindexes."+lv.name)
	require.NotNil(lv.t, vdx, "lookup vindex %s not found", lv.name)

	expectedOwner, expectedFrom, expectedTo := lv.ownerTable, strings.Join(lv.columns, ","), "keyspace_id"
	require.Equal(lv.t, expectedOwner, vdx.Get("owner").String(), "expected 'owner' parameter to be %s", expectedOwner)
	require.Equal(lv.t, expectedFrom, vdx.Get("params.from").String(), "expected 'from' parameter to be %s", expectedFrom)
	require.Equal(lv.t, expectedTo, vdx.Get("params.to").String(), "expected 'to' parameter to be %s", expectedTo)

	want := ""
	if expectedWriteOnlyParam {
		want = "true"
	}
	require.Equal(lv.t, want, vdx.Get("params.write_only").String(), "expected write_only parameter to be %s", want)
}

func getNumRowsInQuery(t *testing.T, query string) int {
	stmt, err := sqlparser.NewTestParser().Parse(query)
	require.NoError(t, err)
	insertStmt, ok := stmt.(*sqlparser.Insert)
	require.True(t, ok)
	rows, ok := insertStmt.Rows.(sqlparser.Values)
	require.True(t, ok)
	return len(rows)
}
