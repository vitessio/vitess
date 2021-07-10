/*
Copyright 2019 The Vitess Authors.

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

package vtgate

import (
	"reflect"
	"sort"
	"testing"
	"time"

	"vitess.io/vitess/go/vt/callerid"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"

	"context"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vtgate/vschemaacl"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
)

func waitForVindex(t *testing.T, ks, name string, watch chan *vschemapb.SrvVSchema, executor *Executor) (*vschemapb.SrvVSchema, *vschemapb.Vindex) {
	t.Helper()

	// Wait up to 10ms until the watch gets notified of the update
	ok := false
	for i := 0; i < 10; i++ {
		select {
		case vschema := <-watch:
			_, ok = vschema.Keyspaces[ks].Vindexes[name]
			if !ok {
				t.Errorf("updated vschema did not contain %s", name)
			}
		default:
			time.Sleep(time.Millisecond)
		}
	}
	if !ok {
		t.Errorf("vschema was not updated as expected")
	}

	// Wait up to 100ms until the vindex manager gets notified of the update
	for i := 0; i < 10; i++ {
		vschema := executor.vm.GetCurrentSrvVschema()
		vindex, ok := vschema.Keyspaces[ks].Vindexes[name]
		if ok {
			return vschema, vindex
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("updated vschema did not contain %s", name)
	return nil, nil
}

func waitForVschemaTables(t *testing.T, ks string, tables []string, executor *Executor) *vschemapb.SrvVSchema {
	t.Helper()

	// Wait up to 100ms until the vindex manager gets notified of the update
	for i := 0; i < 10; i++ {
		vschema := executor.vm.GetCurrentSrvVschema()
		gotTables := []string{}
		for t := range vschema.Keyspaces[ks].Tables {
			gotTables = append(gotTables, t)
		}
		sort.Strings(tables)
		sort.Strings(gotTables)
		if reflect.DeepEqual(tables, gotTables) {
			return vschema
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("updated vschema did not contain tables %v", tables)
	return nil
}

//nolint
func waitForColVindexes(t *testing.T, ks, table string, names []string, executor *Executor) *vschemapb.SrvVSchema {
	t.Helper()

	// Wait up to 10ms until the vindex manager gets notified of the update
	for i := 0; i < 10; i++ {

		vschema := executor.vm.GetCurrentSrvVschema()
		table, ok := vschema.Keyspaces[ks].Tables[table]

		// The table is removed from the vschema when there are no
		// vindexes defined
		if !ok == (len(names) == 0) {
			return vschema
		} else if ok && (len(names) == len(table.ColumnVindexes)) {
			match := true
			for i, name := range names {
				if name != table.ColumnVindexes[i].Name {
					match = false
					break
				}
			}
			if match {
				return vschema
			}
		}

		time.Sleep(time.Millisecond)

	}

	t.Fatalf("updated vschema did not contain vindexes %v on table %s", names, table)
	return nil
}

func TestPlanExecutorAlterVSchemaKeyspace(t *testing.T) {
	*vschemaacl.AuthorizedDDLUsers = "%"
	defer func() {
		*vschemaacl.AuthorizedDDLUsers = ""
	}()
	executor, _, _, _ := createLegacyExecutorEnv()
	session := NewSafeSession(&vtgatepb.Session{TargetString: "@master", Autocommit: true})

	vschemaUpdates := make(chan *vschemapb.SrvVSchema, 2)
	executor.serv.WatchSrvVSchema(context.Background(), "aa", func(vschema *vschemapb.SrvVSchema, err error) {
		vschemaUpdates <- vschema
	})

	vschema := <-vschemaUpdates
	_, ok := vschema.Keyspaces["TestExecutor"].Vindexes["test_vindex"]
	if ok {
		t.Fatalf("test_vindex should not exist in original vschema")
	}

	stmt := "alter vschema create vindex TestExecutor.test_vindex using hash"
	_, err := executor.Execute(context.Background(), "TestExecute", session, stmt, nil)
	require.NoError(t, err)

	_, vindex := waitForVindex(t, "TestExecutor", "test_vindex", vschemaUpdates, executor)
	assert.Equal(t, vindex.Type, "hash")
}

func TestPlanExecutorCreateVindexDDL(t *testing.T) {
	*vschemaacl.AuthorizedDDLUsers = "%"
	defer func() {
		*vschemaacl.AuthorizedDDLUsers = ""
	}()
	executor, _, _, _ := createLegacyExecutorEnv()
	ks := "TestExecutor"

	vschemaUpdates := make(chan *vschemapb.SrvVSchema, 4)
	executor.serv.WatchSrvVSchema(context.Background(), "aa", func(vschema *vschemapb.SrvVSchema, err error) {
		vschemaUpdates <- vschema
	})

	vschema := <-vschemaUpdates
	_, ok := vschema.Keyspaces[ks].Vindexes["test_vindex"]
	if ok {
		t.Fatalf("test_vindex should not exist in original vschema")
	}

	session := NewSafeSession(&vtgatepb.Session{TargetString: ks})
	stmt := "alter vschema create vindex test_vindex using hash"
	_, err := executor.Execute(context.Background(), "TestExecute", session, stmt, nil)
	require.NoError(t, err)

	_, vindex := waitForVindex(t, ks, "test_vindex", vschemaUpdates, executor)
	if vindex == nil || vindex.Type != "hash" {
		t.Errorf("updated vschema did not contain test_vindex")
	}

	_, err = executor.Execute(context.Background(), "TestExecute", session, stmt, nil)
	wantErr := "vindex test_vindex already exists in keyspace TestExecutor"
	if err == nil || err.Error() != wantErr {
		t.Errorf("create duplicate vindex: %v, want %s", err, wantErr)
	}
	select {
	case <-vschemaUpdates:
		t.Error("vschema should not be updated on error")
	default:
	}
}

func TestPlanExecutorDropVindexDDL(t *testing.T) {
	*vschemaacl.AuthorizedDDLUsers = "%"
	defer func() {
		*vschemaacl.AuthorizedDDLUsers = ""
	}()
	executor, _, _, _ := createLegacyExecutorEnv()
	ks := "TestExecutor"

	vschemaUpdates := make(chan *vschemapb.SrvVSchema, 4)
	executor.serv.WatchSrvVSchema(context.Background(), "aa", func(vschema *vschemapb.SrvVSchema, err error) {
		vschemaUpdates <- vschema
	})

	vschema := <-vschemaUpdates
	_, ok := vschema.Keyspaces[ks].Vindexes["test_vindex"]
	if ok {
		t.Fatalf("test_vindex should not exist in original vschema")
	}

	session := NewSafeSession(&vtgatepb.Session{TargetString: ks})
	stmt := "alter vschema drop vindex test_vindex"
	_, err := executor.Execute(context.Background(), "TestExecute", session, stmt, nil)
	wantErr := "vindex test_vindex does not exists in keyspace TestExecutor"
	if err == nil || err.Error() != wantErr {
		t.Errorf("want error %v got %v", wantErr, err)
	}

	stmt = "alter vschema drop vindex TestExecutor.test_vindex"
	_, err = executor.Execute(context.Background(), "TestExecute", session, stmt, nil)
	wantErr = "vindex test_vindex does not exists in keyspace TestExecutor"
	if err == nil || err.Error() != wantErr {
		t.Errorf("want error %v got %v", wantErr, err)
	}

	//add one vindex that has never been used by the tables
	stmt = "alter vschema create vindex test_vindex using hash"
	_, err = executor.Execute(context.Background(), "TestExecute", session, stmt, nil)
	require.NoError(t, err)

	_, vindex := waitForVindex(t, ks, "test_vindex", vschemaUpdates, executor)
	if vindex == nil || vindex.Type != "hash" {
		t.Errorf("updated vschema did not contain test_vindex")
	}

	//drop an existing vindex that has never been used by the tables
	stmt = "alter vschema drop vindex TestExecutor.test_vindex"
	_, err = executor.Execute(context.Background(), "TestExecute", session, stmt, nil)
	require.NoError(t, err)
	vschema = <-vschemaUpdates
	_, ok = vschema.Keyspaces[ks].Vindexes["test_vindex"]
	if ok {
		t.Fatalf("test_vindex should not exist after droping it")
	}

	//drop an existing vindex that is used by at least one table
	stmt = "alter vschema drop vindex TestExecutor.keyspace_id"
	_, err = executor.Execute(context.Background(), "TestExecute", session, stmt, nil)
	wantErr = "can not drop vindex cause keyspace_id still defined on table ksid_table"
	if err == nil || err.Error() != wantErr {
		t.Errorf("drop vindex still defined: %v, want %s", err, wantErr)
	}
	select {
	case <-vschemaUpdates:
		t.Error("vschema should not be updated on error")
	default:
	}
}

func TestPlanExecutorAddDropVschemaTableDDL(t *testing.T) {
	*vschemaacl.AuthorizedDDLUsers = "%"
	defer func() {
		*vschemaacl.AuthorizedDDLUsers = ""
	}()
	executor, sbc1, sbc2, sbclookup := createLegacyExecutorEnv()
	ks := KsTestUnsharded

	vschemaUpdates := make(chan *vschemapb.SrvVSchema, 4)
	executor.serv.WatchSrvVSchema(context.Background(), "aa", func(vschema *vschemapb.SrvVSchema, err error) {
		vschemaUpdates <- vschema
	})

	vschema := <-vschemaUpdates
	_, ok := vschema.Keyspaces[ks].Tables["test_table"]
	if ok {
		t.Fatalf("test_table should not exist in original vschema")
	}

	vschemaTables := []string{}
	for t := range vschema.Keyspaces[ks].Tables {
		vschemaTables = append(vschemaTables, t)
	}

	session := NewSafeSession(&vtgatepb.Session{TargetString: ks})
	stmt := "alter vschema add table test_table"
	_, err := executor.Execute(context.Background(), "TestExecute", session, stmt, nil)
	require.NoError(t, err)
	_ = waitForVschemaTables(t, ks, append(vschemaTables, "test_table"), executor)

	stmt = "alter vschema add table test_table2"
	_, err = executor.Execute(context.Background(), "TestExecute", session, stmt, nil)
	require.NoError(t, err)
	_ = waitForVschemaTables(t, ks, append(vschemaTables, []string{"test_table", "test_table2"}...), executor)

	// Should fail adding a table on a sharded keyspace
	session = NewSafeSession(&vtgatepb.Session{TargetString: "TestExecutor"})
	stmt = "alter vschema add table test_table"
	_, err = executor.Execute(context.Background(), "TestExecute", session, stmt, nil)
	wantErr := "add vschema table: unsupported on sharded keyspace TestExecutor"
	if err == nil || err.Error() != wantErr {
		t.Errorf("want error %v got %v", wantErr, err)
	}

	// No queries should have gone to any tablets
	wantCount := []int64{0, 0, 0}
	gotCount := []int64{
		sbc1.ExecCount.Get(),
		sbc2.ExecCount.Get(),
		sbclookup.ExecCount.Get(),
	}
	if !reflect.DeepEqual(gotCount, wantCount) {
		t.Errorf("Exec %s: %v, want %v", stmt, gotCount, wantCount)
	}
}

func TestExecutorAddSequenceDDL(t *testing.T) {
	*vschemaacl.AuthorizedDDLUsers = "%"
	defer func() {
		*vschemaacl.AuthorizedDDLUsers = ""
	}()
	executor, _, _, _ := createLegacyExecutorEnv()
	ks := KsTestUnsharded

	vschema := executor.vm.GetCurrentSrvVschema()

	var vschemaTables []string
	for t := range vschema.Keyspaces[ks].Tables {
		vschemaTables = append(vschemaTables, t)
	}

	session := NewSafeSession(&vtgatepb.Session{TargetString: ks})
	stmt := "alter vschema add sequence test_seq"
	_, err := executor.Execute(context.Background(), "TestExecute", session, stmt, nil)
	require.NoError(t, err)
	_ = waitForVschemaTables(t, ks, append(vschemaTables, []string{"test_seq"}...), executor)
	vschema = executor.vm.GetCurrentSrvVschema()
	table := vschema.Keyspaces[ks].Tables["test_seq"]
	wantType := "sequence"
	if table.Type != wantType {
		t.Errorf("want table type sequence got %v", table)
	}

	// Should fail adding a table on a sharded keyspace
	ksSharded := "TestExecutor"
	session = NewSafeSession(&vtgatepb.Session{TargetString: ksSharded})

	stmt = "alter vschema add sequence sequence_table"
	_, err = executor.Execute(context.Background(), "TestExecute", session, stmt, nil)

	wantErr := "add sequence table: unsupported on sharded keyspace TestExecutor"
	if err == nil || err.Error() != wantErr {
		t.Errorf("want error %v got %v", wantErr, err)
	}

	// Should be able to add autoincrement to table in sharded keyspace
	stmt = "alter vschema on test_table add vindex hash_index (id)"
	if _, err = executor.Execute(context.Background(), "TestExecute", session, stmt, nil); err != nil {
		t.Error(err)
	}
	time.Sleep(10 * time.Millisecond)

	stmt = "alter vschema on test_table add auto_increment id using test_seq"
	if _, err = executor.Execute(context.Background(), "TestExecute", session, stmt, nil); err != nil {
		t.Error(err)
	}
	time.Sleep(10 * time.Millisecond)

	wantAutoInc := &vschemapb.AutoIncrement{Column: "id", Sequence: "test_seq"}
	gotAutoInc := executor.vm.GetCurrentSrvVschema().Keyspaces[ksSharded].Tables["test_table"].AutoIncrement

	if !reflect.DeepEqual(wantAutoInc, gotAutoInc) {
		t.Errorf("want autoinc %v, got autoinc %v", wantAutoInc, gotAutoInc)
	}
}

func TestExecutorAddDropVindexDDL(t *testing.T) {
	*vschemaacl.AuthorizedDDLUsers = "%"
	defer func() {
		*vschemaacl.AuthorizedDDLUsers = ""
	}()
	executor, sbc1, sbc2, sbclookup := createLegacyExecutorEnv() //nolint
	ks := "TestExecutor"
	session := NewSafeSession(&vtgatepb.Session{TargetString: ks})
	vschemaUpdates := make(chan *vschemapb.SrvVSchema, 4)
	executor.serv.WatchSrvVSchema(context.Background(), "aa", func(vschema *vschemapb.SrvVSchema, err error) {
		vschemaUpdates <- vschema
	})

	vschema := <-vschemaUpdates
	_, ok := vschema.Keyspaces[ks].Vindexes["test_hash"]
	if ok {
		t.Fatalf("test_hash should not exist in original vschema")
	}

	// Create a new vindex implicitly with the statement
	stmt := "alter vschema on test add vindex test_hash (id) using hash "
	_, err := executor.Execute(context.Background(), "TestExecute", session, stmt, nil)
	if err != nil {
		t.Fatalf("error in %s: %v", stmt, err)
	}

	_, vindex := waitForVindex(t, ks, "test_hash", vschemaUpdates, executor)
	if vindex.Type != "hash" {
		t.Errorf("vindex type %s not hash", vindex.Type)
	}

	t.Skip("not yet planned")

	_ = waitForColVindexes(t, ks, "test", []string{"test_hash"}, executor)
	qr, err := executor.Execute(context.Background(), "TestExecute", session, "show vschema vindexes on TestExecutor.test", nil)
	if err != nil {
		t.Fatalf("error in show vschema vindexes on TestExecutor.test: %v", err)
	}
	wantqr := &sqltypes.Result{
		Fields: buildVarCharFields("Columns", "Name", "Type", "Params", "Owner"),
		Rows: [][]sqltypes.Value{
			buildVarCharRow("id", "test_hash", "hash", "", ""),
		},
		RowsAffected: 1,
	}
	if !reflect.DeepEqual(qr, wantqr) {
		t.Errorf("show vschema vindexes on TestExecutor.test:\n%+v, want\n%+v", qr, wantqr)
	}

	// Drop it
	stmt = "alter vschema on test drop vindex test_hash"
	_, err = executor.Execute(context.Background(), "TestExecute", session, stmt, nil)
	if err != nil {
		t.Fatalf("error in %s: %v", stmt, err)
	}

	_, _ = waitForVindex(t, ks, "test_hash", vschemaUpdates, executor)
	_ = waitForColVindexes(t, ks, "test", []string{}, executor)
	_, err = executor.Execute(context.Background(), "TestExecute", session, "show vschema vindexes on TestExecutor.test", nil)
	wantErr := "table `test` does not exist in keyspace `TestExecutor`"
	if err == nil || err.Error() != wantErr {
		t.Fatalf("expected error in show vschema vindexes on TestExecutor.test %v: got %v", wantErr, err)
	}

	// add it again using the same syntax
	stmt = "alter vschema on test add vindex test_hash (id) using hash "
	_, err = executor.Execute(context.Background(), "TestExecute", session, stmt, nil)
	if err != nil {
		t.Fatalf("error in %s: %v", stmt, err)
	}

	_, vindex = waitForVindex(t, ks, "test_hash", vschemaUpdates, executor)
	if vindex.Type != "hash" {
		t.Errorf("vindex type %s not hash", vindex.Type)
	}

	_ = waitForColVindexes(t, ks, "test", []string{"test_hash"}, executor)

	qr, err = executor.Execute(context.Background(), "TestExecute", session, "show vschema vindexes on TestExecutor.test", nil)
	if err != nil {
		t.Fatalf("error in show vschema vindexes on TestExecutor.test: %v", err)
	}
	wantqr = &sqltypes.Result{
		Fields: buildVarCharFields("Columns", "Name", "Type", "Params", "Owner"),
		Rows: [][]sqltypes.Value{
			buildVarCharRow("id", "test_hash", "hash", "", ""),
		},
		RowsAffected: 1,
	}
	if !reflect.DeepEqual(qr, wantqr) {
		t.Errorf("show vschema vindexes on TestExecutor.test:\n%+v, want\n%+v", qr, wantqr)
	}

	// add another
	stmt = "alter vschema on test add vindex test_lookup (c1,c2) using lookup with owner=`test`, from=`c1,c2`, table=test_lookup, to=keyspace_id"
	_, err = executor.Execute(context.Background(), "TestExecute", session, stmt, nil)
	if err != nil {
		t.Fatalf("error in %s: %v", stmt, err)
	}

	vschema, vindex = waitForVindex(t, ks, "test_lookup", vschemaUpdates, executor)
	if vindex.Type != "lookup" {
		t.Errorf("vindex type %s not hash", vindex.Type)
	}

	if table, ok := vschema.Keyspaces[ks].Tables["test"]; ok {
		if len(table.ColumnVindexes) != 2 {
			t.Fatalf("table vindexes want 1 got %d", len(table.ColumnVindexes))
		}
		if table.ColumnVindexes[1].Name != "test_lookup" {
			t.Fatalf("table vindexes didn't contain test_lookup")
		}
	} else {
		t.Fatalf("table test not defined in vschema")
	}

	qr, err = executor.Execute(context.Background(), "TestExecute", session, "show vschema vindexes on TestExecutor.test", nil)
	if err != nil {
		t.Fatalf("error in show vschema vindexes on TestExecutor.test: %v", err)
	}
	wantqr = &sqltypes.Result{
		Fields: buildVarCharFields("Columns", "Name", "Type", "Params", "Owner"),
		Rows: [][]sqltypes.Value{
			buildVarCharRow("id", "test_hash", "hash", "", ""),
			buildVarCharRow("c1, c2", "test_lookup", "lookup", "from=c1,c2; table=test_lookup; to=keyspace_id", "test"),
		},
		RowsAffected: 2,
	}
	if !reflect.DeepEqual(qr, wantqr) {
		t.Errorf("show vschema vindexes on TestExecutor.test:\n%+v, want\n%+v", qr, wantqr)
	}

	stmt = "alter vschema on test add vindex test_hash_id2 (id2) using hash"
	_, err = executor.Execute(context.Background(), "TestExecute", session, stmt, nil)
	if err != nil {
		t.Fatalf("error in %s: %v", stmt, err)
	}

	vschema, vindex = waitForVindex(t, ks, "test_hash_id2", vschemaUpdates, executor)
	if vindex.Type != "hash" {
		t.Errorf("vindex type %s not hash", vindex.Type)
	}

	if table, ok := vschema.Keyspaces[ks].Tables["test"]; ok {
		if len(table.ColumnVindexes) != 3 {
			t.Fatalf("table vindexes want 1 got %d", len(table.ColumnVindexes))
		}
		if table.ColumnVindexes[2].Name != "test_hash_id2" {
			t.Fatalf("table vindexes didn't contain test_hash_id2")
		}
	} else {
		t.Fatalf("table test not defined in vschema")
	}

	qr, err = executor.Execute(context.Background(), "TestExecute", session, "show vschema vindexes on TestExecutor.test", nil)
	if err != nil {
		t.Fatalf("error in show vschema vindexes on TestExecutor.test: %v", err)
	}
	wantqr = &sqltypes.Result{
		Fields: buildVarCharFields("Columns", "Name", "Type", "Params", "Owner"),
		Rows: [][]sqltypes.Value{
			buildVarCharRow("id", "test_hash", "hash", "", ""),
			buildVarCharRow("c1, c2", "test_lookup", "lookup", "from=c1,c2; table=test_lookup; to=keyspace_id", "test"),
			buildVarCharRow("id2", "test_hash_id2", "hash", "", ""),
		},
		RowsAffected: 3,
	}
	if !reflect.DeepEqual(qr, wantqr) {
		t.Errorf("show vschema vindexes on TestExecutor.test:\n%+v, want\n%+v", qr, wantqr)
	}

	// drop one
	stmt = "alter vschema on test drop vindex test_lookup"
	_, err = executor.Execute(context.Background(), "TestExecute", session, stmt, nil)
	if err != nil {
		t.Fatalf("error in %s: %v", stmt, err)
	}

	// wait for up to 50ms for it to disappear
	deadline := time.Now().Add(50 * time.Millisecond)
	for {
		qr, err = executor.Execute(context.Background(), "TestExecute", session, "show vschema vindexes on TestExecutor.test", nil)
		if err != nil {
			t.Fatalf("error in show vschema vindexes on TestExecutor.test: %v", err)
		}
		wantqr = &sqltypes.Result{
			Fields: buildVarCharFields("Columns", "Name", "Type", "Params", "Owner"),
			Rows: [][]sqltypes.Value{
				buildVarCharRow("id", "test_hash", "hash", "", ""),
				buildVarCharRow("id2", "test_hash_id2", "hash", "", ""),
			},
			RowsAffected: 2,
		}
		if reflect.DeepEqual(qr, wantqr) {
			break
		}

		if time.Now().After(deadline) {
			t.Errorf("timed out waiting for test_lookup vindex to be removed")
		}
		time.Sleep(1 * time.Millisecond)
	}

	// use the newly created vindex on a new table
	stmt = "alter vschema on test2 add vindex test_hash (id)"
	_, err = executor.Execute(context.Background(), "TestExecute", session, stmt, nil)
	if err != nil {
		t.Fatalf("error in %s: %v", stmt, err)
	}

	vschema, vindex = waitForVindex(t, ks, "test_hash", vschemaUpdates, executor)
	if vindex.Type != "hash" {
		t.Errorf("vindex type %s not hash", vindex.Type)
	}

	if table, ok := vschema.Keyspaces[ks].Tables["test2"]; ok {
		if len(table.ColumnVindexes) != 1 {
			t.Fatalf("table vindexes want 1 got %d", len(table.ColumnVindexes))
		}
		if table.ColumnVindexes[0].Name != "test_hash" {
			t.Fatalf("table vindexes didn't contain test_hash")
		}
	} else {
		t.Fatalf("table test2 not defined in vschema")
	}

	// create an identical vindex definition on a different table
	stmt = "alter vschema on test2 add vindex test_lookup (c1,c2) using lookup with owner=`test`, from=`c1,c2`, table=test_lookup, to=keyspace_id"
	_, err = executor.Execute(context.Background(), "TestExecute", session, stmt, nil)
	if err != nil {
		t.Fatalf("error in %s: %v", stmt, err)
	}

	vschema, vindex = waitForVindex(t, ks, "test_lookup", vschemaUpdates, executor)
	if vindex.Type != "lookup" {
		t.Errorf("vindex type %s not hash", vindex.Type)
	}

	if table, ok := vschema.Keyspaces[ks].Tables["test2"]; ok {
		if len(table.ColumnVindexes) != 2 {
			t.Fatalf("table vindexes want 1 got %d", len(table.ColumnVindexes))
		}
		if table.ColumnVindexes[1].Name != "test_lookup" {
			t.Fatalf("table vindexes didn't contain test_lookup")
		}
	} else {
		t.Fatalf("table test2 not defined in vschema")
	}

	qr, err = executor.Execute(context.Background(), "TestExecute", session, "show vschema vindexes on TestExecutor.test2", nil)
	if err != nil {
		t.Fatalf("error in show vschema vindexes on TestExecutor.test2: %v", err)
	}
	wantqr = &sqltypes.Result{
		Fields: buildVarCharFields("Columns", "Name", "Type", "Params", "Owner"),
		Rows: [][]sqltypes.Value{
			buildVarCharRow("id", "test_hash", "hash", "", ""),
			buildVarCharRow("c1, c2", "test_lookup", "lookup", "from=c1,c2; table=test_lookup; to=keyspace_id", "test"),
		},
		RowsAffected: 2,
	}
	if !reflect.DeepEqual(qr, wantqr) {
		t.Errorf("show vschema vindexes on TestExecutor.test:\n%+v, want\n%+v", qr, wantqr)
	}

	stmt = "alter vschema on test2 add vindex nonexistent (c1,c2)"
	_, err = executor.Execute(context.Background(), "TestExecute", session, stmt, nil)
	wantErr = "vindex nonexistent does not exist in keyspace TestExecutor"
	if err == nil || err.Error() != wantErr {
		t.Errorf("got %v want err %s", err, wantErr)
	}

	stmt = "alter vschema on test2 add vindex test_hash (c1,c2) using lookup"
	_, err = executor.Execute(context.Background(), "TestExecute", session, stmt, nil)
	wantErr = "vindex test_hash defined with type hash not lookup"
	if err == nil || err.Error() != wantErr {
		t.Errorf("got %v want err %s", err, wantErr)
	}

	stmt = "alter vschema on test2 add vindex test_lookup (c1,c2) using lookup with owner=xyz"
	_, err = executor.Execute(context.Background(), "TestExecute", session, stmt, nil)
	wantErr = "vindex test_lookup defined with owner test not xyz"
	if err == nil || err.Error() != wantErr {
		t.Errorf("got %v want err %s", err, wantErr)
	}

	stmt = "alter vschema on test2 add vindex test_lookup (c1,c2) using lookup with owner=`test`, foo=bar"
	_, err = executor.Execute(context.Background(), "TestExecute", session, stmt, nil)
	wantErr = "vindex test_lookup defined with different parameters"
	if err == nil || err.Error() != wantErr {
		t.Errorf("got %v want err %s", err, wantErr)
	}

	stmt = "alter vschema on nonexistent drop vindex test_lookup"
	_, err = executor.Execute(context.Background(), "TestExecute", session, stmt, nil)
	wantErr = "table TestExecutor.nonexistent not defined in vschema"
	if err == nil || err.Error() != wantErr {
		t.Errorf("got %v want err %s", err, wantErr)
	}

	stmt = "alter vschema on nonexistent drop vindex test_lookup"
	_, err = executor.Execute(context.Background(), "TestExecute", NewSafeSession(&vtgatepb.Session{TargetString: "InvalidKeyspace"}), stmt, nil)
	wantErr = "table InvalidKeyspace.nonexistent not defined in vschema"
	if err == nil || err.Error() != wantErr {
		t.Errorf("got %v want err %s", err, wantErr)
	}

	stmt = "alter vschema on nowhere.nohow drop vindex test_lookup"
	_, err = executor.Execute(context.Background(), "TestExecute", session, stmt, nil)
	wantErr = "table nowhere.nohow not defined in vschema"
	if err == nil || err.Error() != wantErr {
		t.Errorf("got %v want err %s", err, wantErr)
	}

	stmt = "alter vschema on test drop vindex test_lookup"
	_, err = executor.Execute(context.Background(), "TestExecute", session, stmt, nil)
	wantErr = "vindex test_lookup not defined in table TestExecutor.test"
	if err == nil || err.Error() != wantErr {
		t.Errorf("got %v want err %s", err, wantErr)
	}

	// no queries should have gone to any tablets
	wantCount := []int64{0, 0, 0}
	gotCount := []int64{
		sbc1.ExecCount.Get(),
		sbc2.ExecCount.Get(),
		sbclookup.ExecCount.Get(),
	}
	if !reflect.DeepEqual(gotCount, wantCount) {
		t.Errorf("Exec %s: %v, want %v", "", gotCount, wantCount)
	}
}

func TestPlanExecutorVindexDDLACL(t *testing.T) {
	//t.Skip("not yet planned")
	executor, _, _, _ := createLegacyExecutorEnv()
	ks := "TestExecutor"
	session := NewSafeSession(&vtgatepb.Session{TargetString: ks})

	ctxRedUser := callerid.NewContext(context.Background(), &vtrpcpb.CallerID{}, &querypb.VTGateCallerID{Username: "redUser"})
	ctxBlueUser := callerid.NewContext(context.Background(), &vtrpcpb.CallerID{}, &querypb.VTGateCallerID{Username: "blueUser"})

	// test that by default no users can perform the operation
	stmt := "alter vschema create vindex test_hash using hash"
	_, err := executor.Execute(ctxRedUser, "TestExecute", session, stmt, nil)
	require.EqualError(t, err, `User 'redUser' is not allowed to perform vschema operations`)

	_, err = executor.Execute(ctxBlueUser, "TestExecute", session, stmt, nil)
	require.EqualError(t, err, `User 'blueUser' is not allowed to perform vschema operations`)

	// test when all users are enabled
	*vschemaacl.AuthorizedDDLUsers = "%"
	vschemaacl.Init()
	_, err = executor.Execute(ctxRedUser, "TestExecute", session, stmt, nil)
	if err != nil {
		t.Errorf("unexpected error '%v'", err)
	}
	stmt = "alter vschema create vindex test_hash2 using hash"
	_, err = executor.Execute(ctxBlueUser, "TestExecute", session, stmt, nil)
	if err != nil {
		t.Errorf("unexpected error '%v'", err)
	}

	// test when only one user is enabled
	*vschemaacl.AuthorizedDDLUsers = "orangeUser, blueUser, greenUser"
	vschemaacl.Init()
	_, err = executor.Execute(ctxRedUser, "TestExecute", session, stmt, nil)
	require.EqualError(t, err, `User 'redUser' is not allowed to perform vschema operations`)

	stmt = "alter vschema create vindex test_hash3 using hash"
	_, err = executor.Execute(ctxBlueUser, "TestExecute", session, stmt, nil)
	if err != nil {
		t.Errorf("unexpected error '%v'", err)
	}

	// restore the disallowed state
	*vschemaacl.AuthorizedDDLUsers = ""
}
