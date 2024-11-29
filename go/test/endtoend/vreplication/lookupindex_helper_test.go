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

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

type lookupIndex struct {
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

func (li *lookupIndex) String() string {
	return li.typ + " " + li.name + " on " + li.tableKeyspace + "." + li.table + " (" + li.columns[0] + ")"
}

func (li *lookupIndex) create() {
	cols := strings.Join(li.columns, ",")
	args := []string{
		"LookupVindex",
		"--name", li.name,
		"--table-keyspace=" + li.ownerTableKeyspace,
		"create",
		"--keyspace=" + li.tableKeyspace,
		"--type=" + li.typ,
		"--table-owner=" + li.ownerTable,
		"--table-owner-columns=" + cols,
		"--tablet-types=PRIMARY",
	}
	if li.ignoreNulls {
		args = append(args, "--ignore-nulls")
	}

	err := vc.VtctldClient.ExecuteCommand(args...)
	require.NoError(li.t, err, "error executing LookupVindex create: %v", err)
	waitForWorkflowState(li.t, vc, fmt.Sprintf("%s.%s", li.ownerTableKeyspace, li.name), binlogdatapb.VReplicationWorkflowState_Running.String())
}

func (li *lookupIndex) cancel() error {
	return nil
}

func (li *lookupIndex) externalize() error {
	return nil
}

func (li *lookupIndex) show() error {
	return nil
}
