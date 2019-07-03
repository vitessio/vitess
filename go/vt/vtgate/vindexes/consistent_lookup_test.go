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

package vindexes

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	"vitess.io/vitess/go/vt/sqlparser"
)

func TestConsistentLookupInit(t *testing.T) {
	lookup := createConsistentLookup(t, "consistent_lookup")
	cols := []sqlparser.ColIdent{
		sqlparser.NewColIdent("fc"),
	}
	err := lookup.(WantOwnerInfo).SetOwnerInfo("ks", "t1", cols)
	want := "does not match"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("SetOwnerInfo: %v, want %v", err, want)
	}
}

func TestConsistentLookupInfo(t *testing.T) {
	lookup := createConsistentLookup(t, "consistent_lookup")
	if lookup.Cost() != 20 {
		t.Errorf("Cost(): %d, want 20", lookup.Cost())
	}
	if strings.Compare("consistent_lookup", lookup.String()) != 0 {
		t.Errorf("String(): %s, want consistent_lookup", lookup.String())
	}
	if lookup.IsUnique() {
		t.Errorf("IsUnique(): %v, want false", lookup.IsUnique())
	}
	if lookup.IsFunctional() {
		t.Errorf("IsFunctional(): %v, want false", lookup.IsFunctional())
	}
}

func TestConsistentLookupUniqueInfo(t *testing.T) {
	lookup := createConsistentLookup(t, "consistent_lookup_unique")
	if lookup.Cost() != 10 {
		t.Errorf("Cost(): %d, want 10", lookup.Cost())
	}
	if strings.Compare("consistent_lookup_unique", lookup.String()) != 0 {
		t.Errorf("String(): %s, want consistent_lookup_unique", lookup.String())
	}
	if !lookup.IsUnique() {
		t.Errorf("IsUnique(): %v, want true", lookup.IsUnique())
	}
	if lookup.IsFunctional() {
		t.Errorf("IsFunctional(): %v, want false", lookup.IsFunctional())
	}
}

func TestConsistentLookupMap(t *testing.T) {
	lookup := createConsistentLookup(t, "consistent_lookup")
	vc := &loggingVCursor{}
	vc.AddResult(makeTestResult(2), nil)
	vc.AddResult(makeTestResult(2), nil)

	got, err := lookup.Map(vc, []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)})
	if err != nil {
		t.Error(err)
	}
	want := []key.Destination{
		key.DestinationKeyspaceIDs([][]byte{
			[]byte("1"),
			[]byte("2"),
		}),
		key.DestinationKeyspaceIDs([][]byte{
			[]byte("1"),
			[]byte("2"),
		}),
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Map(): %#v, want %+v", got, want)
	}
	vc.verifyLog(t, []string{
		"Execute select toc from t where fromc1 = :fromc1 [{fromc1 1}] false",
		"Execute select toc from t where fromc1 = :fromc1 [{fromc1 2}] false",
	})

	// Test query fail.
	vc.AddResult(nil, fmt.Errorf("execute failed"))
	_, err = lookup.Map(vc, []sqltypes.Value{sqltypes.NewInt64(1)})
	wantErr := "lookup.Map: execute failed"
	if err == nil || err.Error() != wantErr {
		t.Errorf("lookup(query fail) err: %v, want %s", err, wantErr)
	}
}

func TestConsistentLookupUniqueMap(t *testing.T) {
	lookup := createConsistentLookup(t, "consistent_lookup_unique")
	vc := &loggingVCursor{}
	vc.AddResult(makeTestResult(0), nil)
	vc.AddResult(makeTestResult(1), nil)

	got, err := lookup.Map(vc, []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)})
	if err != nil {
		t.Error(err)
	}
	want := []key.Destination{
		key.DestinationNone{},
		key.DestinationKeyspaceID([]byte("1")),
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Map(): %#v, want %+v", got, want)
	}
	vc.verifyLog(t, []string{
		"Execute select toc from t where fromc1 = :fromc1 [{fromc1 1}] false",
		"Execute select toc from t where fromc1 = :fromc1 [{fromc1 2}] false",
	})

	// More than one result is invalid
	vc.AddResult(makeTestResult(2), nil)
	_, err = lookup.Map(vc, []sqltypes.Value{sqltypes.NewInt64(1)})
	wanterr := "Lookup.Map: unexpected multiple results from vindex t: INT64(1)"
	if err == nil || err.Error() != wanterr {
		t.Errorf("lookup(query fail) err: %v, want %s", err, wanterr)
	}
}

func TestConsistentLookupMapAbsent(t *testing.T) {
	lookup := createConsistentLookup(t, "consistent_lookup")
	vc := &loggingVCursor{}
	vc.AddResult(makeTestResult(0), nil)
	vc.AddResult(makeTestResult(0), nil)

	got, err := lookup.Map(vc, []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)})
	if err != nil {
		t.Error(err)
	}
	want := []key.Destination{
		key.DestinationNone{},
		key.DestinationNone{},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Map(): %#v, want %+v", got, want)
	}
	vc.verifyLog(t, []string{
		"Execute select toc from t where fromc1 = :fromc1 [{fromc1 1}] false",
		"Execute select toc from t where fromc1 = :fromc1 [{fromc1 2}] false",
	})
}

func TestConsistentLookupVerify(t *testing.T) {
	lookup := createConsistentLookup(t, "consistent_lookup")
	vc := &loggingVCursor{}
	vc.AddResult(makeTestResult(1), nil)
	vc.AddResult(makeTestResult(1), nil)

	_, err := lookup.Verify(vc, []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)}, [][]byte{[]byte("test1"), []byte("test2")})
	if err != nil {
		t.Error(err)
	}
	vc.verifyLog(t, []string{
		"ExecutePre select fromc1 from t where fromc1 = :fromc1 and toc = :toc [{fromc1 1} {toc test1}] false",
		"ExecutePre select fromc1 from t where fromc1 = :fromc1 and toc = :toc [{fromc1 2} {toc test2}] false",
	})

	// Test query fail.
	vc.AddResult(nil, fmt.Errorf("execute failed"))
	_, err = lookup.Verify(vc, []sqltypes.Value{sqltypes.NewInt64(1)}, [][]byte{[]byte("\x16k@\xb4J\xbaK\xd6")})
	want := "lookup.Verify: execute failed"
	if err == nil || err.Error() != want {
		t.Errorf("lookup(query fail) err: %v, want %s", err, want)
	}
}

func TestConsistentLookupCreateSimple(t *testing.T) {
	lookup := createConsistentLookup(t, "consistent_lookup")
	vc := &loggingVCursor{}
	vc.AddResult(&sqltypes.Result{}, nil)

	if err := lookup.(Lookup).Create(vc,
		[][]sqltypes.Value{{
			sqltypes.NewInt64(1),
			sqltypes.NewInt64(2),
		}, {
			sqltypes.NewInt64(3),
			sqltypes.NewInt64(4),
		}},
		[][]byte{[]byte("test1"), []byte("test2")},
		false /* ignoreMode */); err != nil {
		t.Error(err)
	}
	vc.verifyLog(t, []string{
		"ExecutePre insert into t(fromc1, fromc2, toc) values(:fromc10, :fromc20, :toc0), (:fromc11, :fromc21, :toc1) [{fromc10 1} {fromc11 3} {fromc20 2} {fromc21 4} {toc0 test1} {toc1 test2}] true",
	})
}

func TestConsistentLookupCreateThenRecreate(t *testing.T) {
	lookup := createConsistentLookup(t, "consistent_lookup")
	vc := &loggingVCursor{}
	vc.AddResult(nil, errors.New("Duplicate entry"))
	vc.AddResult(&sqltypes.Result{}, nil)
	vc.AddResult(&sqltypes.Result{}, nil)

	if err := lookup.(Lookup).Create(vc,
		[][]sqltypes.Value{{
			sqltypes.NewInt64(1),
			sqltypes.NewInt64(2),
		}},
		[][]byte{[]byte("test1")},
		false /* ignoreMode */); err != nil {
		t.Error(err)
	}
	vc.verifyLog(t, []string{
		"ExecutePre insert into t(fromc1, fromc2, toc) values(:fromc10, :fromc20, :toc0) [{fromc10 1} {fromc20 2} {toc0 test1}] true",
		"ExecutePre select toc from t where fromc1 = :fromc1 and fromc2 = :fromc2 for update [{fromc1 1} {fromc2 2} {toc test1}] false",
		"ExecutePre insert into t(fromc1, fromc2, toc) values(:fromc1, :fromc2, :toc) [{fromc1 1} {fromc2 2} {toc test1}] true",
	})
}

func TestConsistentLookupCreateThenUpdate(t *testing.T) {
	lookup := createConsistentLookup(t, "consistent_lookup")
	vc := &loggingVCursor{}
	vc.AddResult(nil, errors.New("Duplicate entry"))
	vc.AddResult(makeTestResult(1), nil)
	vc.AddResult(&sqltypes.Result{}, nil)
	vc.AddResult(&sqltypes.Result{}, nil)

	if err := lookup.(Lookup).Create(vc,
		[][]sqltypes.Value{{
			sqltypes.NewInt64(1),
			sqltypes.NewInt64(2),
		}},
		[][]byte{[]byte("test1")},
		false /* ignoreMode */); err != nil {
		t.Error(err)
	}
	vc.verifyLog(t, []string{
		"ExecutePre insert into t(fromc1, fromc2, toc) values(:fromc10, :fromc20, :toc0) [{fromc10 1} {fromc20 2} {toc0 test1}] true",
		"ExecutePre select toc from t where fromc1 = :fromc1 and fromc2 = :fromc2 for update [{fromc1 1} {fromc2 2} {toc test1}] false",
		"ExecuteKeyspaceID select fc1 from t1 where fc1 = :fromc1 and fc2 = :fromc2 lock in share mode [{fromc1 1} {fromc2 2} {toc test1}] false",
		"ExecutePre update t set toc=:toc where fromc1 = :fromc1 and fromc2 = :fromc2 [{fromc1 1} {fromc2 2} {toc test1}] true",
	})
}

func TestConsistentLookupCreateThenSkipUpdate(t *testing.T) {
	lookup := createConsistentLookup(t, "consistent_lookup")
	vc := &loggingVCursor{}
	vc.AddResult(nil, errors.New("Duplicate entry"))
	vc.AddResult(makeTestResult(1), nil)
	vc.AddResult(&sqltypes.Result{}, nil)
	vc.AddResult(&sqltypes.Result{}, nil)

	if err := lookup.(Lookup).Create(vc,
		[][]sqltypes.Value{{
			sqltypes.NewInt64(1),
			sqltypes.NewInt64(2),
		}},
		[][]byte{[]byte("1")},
		false /* ignoreMode */); err != nil {
		t.Error(err)
	}
	vc.verifyLog(t, []string{
		"ExecutePre insert into t(fromc1, fromc2, toc) values(:fromc10, :fromc20, :toc0) [{fromc10 1} {fromc20 2} {toc0 1}] true",
		"ExecutePre select toc from t where fromc1 = :fromc1 and fromc2 = :fromc2 for update [{fromc1 1} {fromc2 2} {toc 1}] false",
		"ExecuteKeyspaceID select fc1 from t1 where fc1 = :fromc1 and fc2 = :fromc2 lock in share mode [{fromc1 1} {fromc2 2} {toc 1}] false",
	})
}

func TestConsistentLookupCreateThenDupkey(t *testing.T) {
	lookup := createConsistentLookup(t, "consistent_lookup")
	vc := &loggingVCursor{}
	vc.AddResult(nil, errors.New("Duplicate entry"))
	vc.AddResult(makeTestResult(1), nil)
	vc.AddResult(makeTestResult(1), nil)
	vc.AddResult(&sqltypes.Result{}, nil)

	err := lookup.(Lookup).Create(vc,
		[][]sqltypes.Value{{
			sqltypes.NewInt64(1),
			sqltypes.NewInt64(2),
		}},
		[][]byte{[]byte("test1")},
		false /* ignoreMode */)
	want := "duplicate entry"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("lookup(query fail) err: %v, must contain %s", err, want)
	}
	vc.verifyLog(t, []string{
		"ExecutePre insert into t(fromc1, fromc2, toc) values(:fromc10, :fromc20, :toc0) [{fromc10 1} {fromc20 2} {toc0 test1}] true",
		"ExecutePre select toc from t where fromc1 = :fromc1 and fromc2 = :fromc2 for update [{fromc1 1} {fromc2 2} {toc test1}] false",
		"ExecuteKeyspaceID select fc1 from t1 where fc1 = :fromc1 and fc2 = :fromc2 lock in share mode [{fromc1 1} {fromc2 2} {toc test1}] false",
	})
}

func TestConsistentLookupCreateNonDupError(t *testing.T) {
	lookup := createConsistentLookup(t, "consistent_lookup")
	vc := &loggingVCursor{}
	vc.AddResult(nil, errors.New("general error"))

	err := lookup.(Lookup).Create(vc,
		[][]sqltypes.Value{{
			sqltypes.NewInt64(1),
			sqltypes.NewInt64(2),
		}},
		[][]byte{[]byte("test1")},
		false /* ignoreMode */)
	want := "general error"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("lookup(query fail) err: %v, must contain %s", err, want)
	}
	vc.verifyLog(t, []string{
		"ExecutePre insert into t(fromc1, fromc2, toc) values(:fromc10, :fromc20, :toc0) [{fromc10 1} {fromc20 2} {toc0 test1}] true",
	})
}

func TestConsistentLookupCreateThenBadRows(t *testing.T) {
	lookup := createConsistentLookup(t, "consistent_lookup")
	vc := &loggingVCursor{}
	vc.AddResult(nil, errors.New("Duplicate entry"))
	vc.AddResult(makeTestResult(2), nil)

	err := lookup.(Lookup).Create(vc,
		[][]sqltypes.Value{{
			sqltypes.NewInt64(1),
			sqltypes.NewInt64(2),
		}},
		[][]byte{[]byte("test1")},
		false /* ignoreMode */)
	want := "unexpected rows"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("lookup(query fail) err: %v, must contain %s", err, want)
	}
	vc.verifyLog(t, []string{
		"ExecutePre insert into t(fromc1, fromc2, toc) values(:fromc10, :fromc20, :toc0) [{fromc10 1} {fromc20 2} {toc0 test1}] true",
		"ExecutePre select toc from t where fromc1 = :fromc1 and fromc2 = :fromc2 for update [{fromc1 1} {fromc2 2} {toc test1}] false",
	})
}

func TestConsistentLookupDelete(t *testing.T) {
	lookup := createConsistentLookup(t, "consistent_lookup")
	vc := &loggingVCursor{}
	vc.AddResult(&sqltypes.Result{}, nil)

	if err := lookup.(Lookup).Delete(vc,
		[][]sqltypes.Value{{
			sqltypes.NewInt64(1),
			sqltypes.NewInt64(2),
		}},
		[]byte("test")); err != nil {
		t.Error(err)
	}
	vc.verifyLog(t, []string{
		"ExecutePost delete from t where fromc1 = :fromc1 and fromc2 = :fromc2 and toc = :toc [{fromc1 1} {fromc2 2} {toc test}] true",
	})
}

func TestConsistentLookupUpdate(t *testing.T) {
	lookup := createConsistentLookup(t, "consistent_lookup")
	vc := &loggingVCursor{}
	vc.AddResult(&sqltypes.Result{}, nil)
	vc.AddResult(&sqltypes.Result{}, nil)

	if err := lookup.(Lookup).Update(vc,
		[]sqltypes.Value{
			sqltypes.NewInt64(1),
			sqltypes.NewInt64(2),
		},
		[]byte("test"),
		[]sqltypes.Value{
			sqltypes.NewInt64(3),
			sqltypes.NewInt64(4),
		}); err != nil {
		t.Error(err)
	}
	vc.verifyLog(t, []string{
		"ExecutePost delete from t where fromc1 = :fromc1 and fromc2 = :fromc2 and toc = :toc [{fromc1 1} {fromc2 2} {toc test}] true",
		"ExecutePre insert into t(fromc1, fromc2, toc) values(:fromc10, :fromc20, :toc0) [{fromc10 3} {fromc20 4} {toc0 test}] true",
	})
}

func TestConsistentLookupNoUpdate(t *testing.T) {
	lookup := createConsistentLookup(t, "consistent_lookup")
	vc := &loggingVCursor{}
	vc.AddResult(&sqltypes.Result{}, nil)
	vc.AddResult(&sqltypes.Result{}, nil)

	if err := lookup.(Lookup).Update(vc,
		[]sqltypes.Value{
			sqltypes.NewInt64(1),
			sqltypes.NewInt64(2),
		},
		[]byte("test"),
		[]sqltypes.Value{
			sqltypes.NewInt64(1),
			sqltypes.NewInt64(2),
		}); err != nil {
		t.Error(err)
	}
	vc.verifyLog(t, []string{})
}

func createConsistentLookup(t *testing.T, name string) Vindex {
	t.Helper()
	l, err := CreateVindex(name, name, map[string]string{
		"table": "t",
		"from":  "fromc1,fromc2",
		"to":    "toc",
	})
	if err != nil {
		t.Fatal(err)
	}
	cols := []sqlparser.ColIdent{
		sqlparser.NewColIdent("fc1"),
		sqlparser.NewColIdent("fc2"),
	}
	if err := l.(WantOwnerInfo).SetOwnerInfo("ks", "t1", cols); err != nil {
		t.Fatal(err)
	}
	return l
}

type loggingVCursor struct {
	results []*sqltypes.Result
	errors  []error
	index   int
	log     []string
}

type bv struct {
	Name string
	Bv   string
}

func (vc *loggingVCursor) AddResult(qr *sqltypes.Result, err error) {
	vc.results = append(vc.results, qr)
	vc.errors = append(vc.errors, err)
}

func (vc *loggingVCursor) Execute(method string, query string, bindvars map[string]*querypb.BindVariable, isDML bool, co vtgatepb.CommitOrder) (*sqltypes.Result, error) {
	name := "Unknown"
	switch co {
	case vtgatepb.CommitOrder_NORMAL:
		name = "Execute"
	case vtgatepb.CommitOrder_PRE:
		name = "ExecutePre"
	case vtgatepb.CommitOrder_POST:
		name = "ExecutePost"
	case vtgatepb.CommitOrder_AUTOCOMMIT:
		name = "ExecuteAutocommit"
	}
	return vc.execute(name, query, bindvars, isDML)
}

func (vc *loggingVCursor) ExecuteKeyspaceID(keyspace string, ksid []byte, query string, bindVars map[string]*querypb.BindVariable, isDML, autocommit bool) (*sqltypes.Result, error) {
	return vc.execute("ExecuteKeyspaceID", query, bindVars, isDML)
}

func (vc *loggingVCursor) execute(method string, query string, bindvars map[string]*querypb.BindVariable, isDML bool) (*sqltypes.Result, error) {
	if vc.index >= len(vc.results) {
		return nil, fmt.Errorf("ran out of results to return: %s", query)
	}
	bvl := make([]bv, 0, len(bindvars))
	for k, v := range bindvars {
		bvl = append(bvl, bv{Name: k, Bv: string(v.Value)})
	}
	sort.Slice(bvl, func(i, j int) bool { return bvl[i].Name < bvl[j].Name })
	vc.log = append(vc.log, fmt.Sprintf("%s %s %v %v", method, query, bvl, isDML))
	idx := vc.index
	vc.index++
	if vc.errors[idx] != nil {
		return nil, vc.errors[idx]
	}
	return vc.results[idx], nil
}

func (vc *loggingVCursor) verifyLog(t *testing.T, want []string) {
	t.Helper()
	for i, got := range vc.log {
		if i >= len(want) {
			t.Fatalf("index exceeded: %v", vc.log[i:])
		}
		if got != want[i] {
			t.Errorf("log(%d):\n%s, want\n%s", i, got, want[i])
		}
	}
	if len(want) > len(vc.log) {
		t.Errorf("expecting queries: %v", want[len(vc.log):])
	}
}

func makeTestResult(numRows int) *sqltypes.Result {
	result := &sqltypes.Result{
		Fields:       sqltypes.MakeTestFields("keyspace_id", "varbinary"),
		RowsAffected: uint64(numRows),
	}
	for i := 0; i < numRows; i++ {
		result.Rows = append(result.Rows, []sqltypes.Value{
			sqltypes.NewVarBinary(strconv.Itoa(i + 1)),
		})
	}
	return result
}
