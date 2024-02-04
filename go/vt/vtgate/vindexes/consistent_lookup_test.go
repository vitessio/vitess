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
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/sqlerror"

	"vitess.io/vitess/go/mysql/collations"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vterrors"
)

func consistentLookupCreateVindexTestCase(
	testName string,
	vindexParams map[string]string,
	expectErr error,
	expectUnknownParams []string,
) createVindexTestCase {
	return createVindexTestCase{
		testName: testName,

		vindexType:   "consistent_lookup",
		vindexName:   "consistent_lookup",
		vindexParams: vindexParams,

		expectCost:          20,
		expectErr:           expectErr,
		expectIsUnique:      false,
		expectNeedsVCursor:  true,
		expectString:        "consistent_lookup",
		expectUnknownParams: expectUnknownParams,
	}
}

func consistentLookupUniqueCreateVindexTestCase(
	testName string,
	vindexParams map[string]string,
	expectErr error,
	expectUnknownParams []string,
) createVindexTestCase {
	return createVindexTestCase{
		testName: testName,

		vindexType:   "consistent_lookup_unique",
		vindexName:   "consistent_lookup_unique",
		vindexParams: vindexParams,

		expectCost:          10,
		expectErr:           expectErr,
		expectIsUnique:      true,
		expectNeedsVCursor:  true,
		expectString:        "consistent_lookup_unique",
		expectUnknownParams: expectUnknownParams,
	}
}

func TestConsistentLookupCreateVindex(t *testing.T) {
	testCaseFs := []func(string, map[string]string, error, []string) createVindexTestCase{
		consistentLookupCreateVindexTestCase,
		consistentLookupUniqueCreateVindexTestCase,
	}
	for _, testCaseF := range testCaseFs {
		testLookupCreateVindexInternalCases(t, testCaseF)
	}
}

func TestConsistentLookupInit(t *testing.T) {
	lookup := createConsistentLookup(t, "consistent_lookup", true)
	cols := []sqlparser.IdentifierCI{
		sqlparser.NewIdentifierCI("fc"),
	}
	err := lookup.(WantOwnerInfo).SetOwnerInfo("ks", "t1", cols)
	want := "does not match"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("SetOwnerInfo: %v, want %v", err, want)
	}
	if got := lookup.(*ConsistentLookup).writeOnly; !got {
		t.Errorf("lookup.writeOnly: false, want true")
	}
}

func TestConsistentLookupMap(t *testing.T) {
	lookup := createConsistentLookup(t, "consistent_lookup", false)
	vc := &loggingVCursor{}
	vc.AddResult(makeTestResultLookup([]int{2, 2}), nil)
	ctx := newTestContext()

	got, err := lookup.Map(ctx, vc, []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)})
	require.NoError(t, err)
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
		"ExecutePre select fromc1, toc from t where fromc1 in ::fromc1 [{fromc1 }] false",
	})
	vc.verifyContext(t, ctx)

	// Test query fail.
	vc.AddResult(nil, fmt.Errorf("execute failed"))
	_, err = lookup.Map(ctx, vc, []sqltypes.Value{sqltypes.NewInt64(1)})
	wantErr := "lookup.Map: execute failed"
	if err == nil || err.Error() != wantErr {
		t.Errorf("lookup(query fail) err: %v, want %s", err, wantErr)
	}
}

func TestConsistentLookupMapWriteOnly(t *testing.T) {
	lookup := createConsistentLookup(t, "consistent_lookup", true)

	got, err := lookup.Map(context.Background(), nil, []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)})
	require.NoError(t, err)
	want := []key.Destination{
		key.DestinationKeyRange{
			KeyRange: &topodatapb.KeyRange{},
		},
		key.DestinationKeyRange{
			KeyRange: &topodatapb.KeyRange{},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Map(): %#v, want %+v", got, want)
	}
}

func TestConsistentLookupUniqueMap(t *testing.T) {
	lookup := createConsistentLookup(t, "consistent_lookup_unique", false)
	vc := &loggingVCursor{}
	vc.AddResult(makeTestResultLookup([]int{0, 1}), nil)
	ctx := newTestContext()

	got, err := lookup.Map(ctx, vc, []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)})
	require.NoError(t, err)
	want := []key.Destination{
		key.DestinationNone{},
		key.DestinationKeyspaceID([]byte("1")),
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Map(): %#v, want %+v", got, want)
	}
	vc.verifyLog(t, []string{
		"ExecutePre select fromc1, toc from t where fromc1 in ::fromc1 [{fromc1 }] false",
	})
	vc.verifyContext(t, ctx)

	// More than one result is invalid
	vc.AddResult(makeTestResultLookup([]int{2}), nil)
	_, err = lookup.Map(ctx, vc, []sqltypes.Value{sqltypes.NewInt64(1)})
	wanterr := "Lookup.Map: unexpected multiple results from vindex t: INT64(1)"
	if err == nil || err.Error() != wanterr {
		t.Errorf("lookup(query fail) err: %v, want %s", err, wanterr)
	}
}

func TestConsistentLookupUniqueMapWriteOnly(t *testing.T) {
	lookup := createConsistentLookup(t, "consistent_lookup_unique", true)

	got, err := lookup.Map(context.Background(), nil, []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)})
	require.NoError(t, err)
	want := []key.Destination{
		key.DestinationKeyRange{
			KeyRange: &topodatapb.KeyRange{},
		},
		key.DestinationKeyRange{
			KeyRange: &topodatapb.KeyRange{},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Map(): %#v, want %+v", got, want)
	}
}

func TestConsistentLookupMapAbsent(t *testing.T) {
	lookup := createConsistentLookup(t, "consistent_lookup", false)
	vc := &loggingVCursor{}
	vc.AddResult(makeTestResultLookup([]int{0, 0}), nil)
	ctx := newTestContext()

	got, err := lookup.Map(ctx, vc, []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)})
	require.NoError(t, err)
	want := []key.Destination{
		key.DestinationNone{},
		key.DestinationNone{},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Map(): %#v, want %+v", got, want)
	}
	vc.verifyLog(t, []string{
		"ExecutePre select fromc1, toc from t where fromc1 in ::fromc1 [{fromc1 }] false",
	})
	vc.verifyContext(t, ctx)
}

func TestConsistentLookupVerify(t *testing.T) {
	lookup := createConsistentLookup(t, "consistent_lookup", false)
	vc := &loggingVCursor{}
	vc.AddResult(makeTestResult(1), nil)
	vc.AddResult(makeTestResult(1), nil)
	ctx := newTestContext()

	_, err := lookup.Verify(ctx, vc, []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)}, [][]byte{[]byte("test1"), []byte("test2")})
	require.NoError(t, err)
	vc.verifyLog(t, []string{
		"ExecutePre select fromc1 from t where fromc1 = :fromc1 and toc = :toc [{fromc1 1} {toc test1}] false",
		"ExecutePre select fromc1 from t where fromc1 = :fromc1 and toc = :toc [{fromc1 2} {toc test2}] false",
	})
	vc.verifyContext(t, ctx)

	// Test query fail.
	vc.AddResult(nil, fmt.Errorf("execute failed"))
	_, err = lookup.Verify(ctx, vc, []sqltypes.Value{sqltypes.NewInt64(1)}, [][]byte{[]byte("\x16k@\xb4J\xbaK\xd6")})
	want := "lookup.Verify: execute failed"
	if err == nil || err.Error() != want {
		t.Errorf("lookup(query fail) err: %v, want %s", err, want)
	}

	// Test write_only.
	lookup = createConsistentLookup(t, "consistent_lookup", true)
	got, err := lookup.Verify(ctx, nil, []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)}, [][]byte{[]byte(""), []byte("")})
	require.NoError(t, err)
	wantBools := []bool{true, true}
	if !reflect.DeepEqual(got, wantBools) {
		t.Errorf("lookup.Verify(writeOnly): %v, want %v", got, wantBools)
	}
}

func TestConsistentLookupCreateSimple(t *testing.T) {
	lookup := createConsistentLookup(t, "consistent_lookup", false)
	vc := &loggingVCursor{}
	vc.AddResult(&sqltypes.Result{}, nil)
	ctx := newTestContext()

	if err := lookup.(Lookup).Create(ctx, vc, [][]sqltypes.Value{{
		sqltypes.NewInt64(1),
		sqltypes.NewInt64(2),
	}, {
		sqltypes.NewInt64(3),
		sqltypes.NewInt64(4),
	}}, [][]byte{[]byte("test1"), []byte("test2")}, false); err != nil {
		t.Error(err)
	}
	vc.verifyLog(t, []string{
		"ExecutePre insert into t(fromc1, fromc2, toc) values(:fromc1_0, :fromc2_0, :toc_0), (:fromc1_1, :fromc2_1, :toc_1) [{fromc1_0 1} {fromc1_1 3} {fromc2_0 2} {fromc2_1 4} {toc_0 test1} {toc_1 test2}] true",
	})
	vc.verifyContext(t, ctx)
}

func TestConsistentLookupCreateThenRecreate(t *testing.T) {
	lookup := createConsistentLookup(t, "consistent_lookup", false)
	vc := &loggingVCursor{}
	vc.AddResult(nil, sqlerror.NewSQLError(sqlerror.ERDupEntry, sqlerror.SSConstraintViolation, "Duplicate entry"))
	vc.AddResult(&sqltypes.Result{}, nil)
	vc.AddResult(&sqltypes.Result{}, nil)
	ctx := newTestContext()

	if err := lookup.(Lookup).Create(ctx, vc, [][]sqltypes.Value{{
		sqltypes.NewInt64(1),
		sqltypes.NewInt64(2),
	}}, [][]byte{[]byte("test1")}, false); err != nil {
		t.Error(err)
	}
	vc.verifyLog(t, []string{
		"ExecutePre insert into t(fromc1, fromc2, toc) values(:fromc1_0, :fromc2_0, :toc_0) [{fromc1_0 1} {fromc2_0 2} {toc_0 test1}] true",
		"ExecutePre select toc from t where fromc1 = :fromc1 and fromc2 = :fromc2 for update [{fromc1 1} {fromc2 2} {toc test1}] false",
		"ExecutePre insert into t(fromc1, fromc2, toc) values(:fromc1, :fromc2, :toc) [{fromc1 1} {fromc2 2} {toc test1}] true",
	})
	vc.verifyContext(t, ctx)
}

func TestConsistentLookupCreateThenUpdate(t *testing.T) {
	lookup := createConsistentLookup(t, "consistent_lookup", false)
	vc := &loggingVCursor{}
	vc.AddResult(nil, vterrors.New(vtrpcpb.Code_ALREADY_EXISTS, "(errno 1062) (sqlstate 23000) Duplicate entry"))
	vc.AddResult(makeTestResult(1), nil)
	vc.AddResult(&sqltypes.Result{}, nil)
	vc.AddResult(&sqltypes.Result{}, nil)
	ctx := newTestContext()

	if err := lookup.(Lookup).Create(ctx, vc, [][]sqltypes.Value{{
		sqltypes.NewInt64(1),
		sqltypes.NewInt64(2),
	}}, [][]byte{[]byte("test1")}, false); err != nil {
		t.Error(err)
	}
	vc.verifyLog(t, []string{
		"ExecutePre insert into t(fromc1, fromc2, toc) values(:fromc1_0, :fromc2_0, :toc_0) [{fromc1_0 1} {fromc2_0 2} {toc_0 test1}] true",
		"ExecutePre select toc from t where fromc1 = :fromc1 and fromc2 = :fromc2 for update [{fromc1 1} {fromc2 2} {toc test1}] false",
		"ExecuteKeyspaceID select fc1 from `dot.t1` where fc1 = :fromc1 and fc2 = :fromc2 lock in share mode [{fromc1 1} {fromc2 2} {toc test1}] false",
		"ExecutePre update t set toc=:toc where fromc1 = :fromc1 and fromc2 = :fromc2 [{fromc1 1} {fromc2 2} {toc test1}] true",
	})
	vc.verifyContext(t, ctx)
}

func TestConsistentLookupCreateThenSkipUpdate(t *testing.T) {
	lookup := createConsistentLookup(t, "consistent_lookup", false)
	vc := &loggingVCursor{}
	vc.AddResult(nil, vterrors.New(vtrpcpb.Code_ALREADY_EXISTS, "(errno 1062) (sqlstate 23000) Duplicate entry"))
	vc.AddResult(makeTestResult(1), nil)
	vc.AddResult(&sqltypes.Result{}, nil)
	vc.AddResult(&sqltypes.Result{}, nil)
	ctx := newTestContext()

	if err := lookup.(Lookup).Create(ctx, vc, [][]sqltypes.Value{{
		sqltypes.NewInt64(1),
		sqltypes.NewInt64(2),
	}}, [][]byte{[]byte("1")}, false); err != nil {
		t.Error(err)
	}
	vc.verifyLog(t, []string{
		"ExecutePre insert into t(fromc1, fromc2, toc) values(:fromc1_0, :fromc2_0, :toc_0) [{fromc1_0 1} {fromc2_0 2} {toc_0 1}] true",
		"ExecutePre select toc from t where fromc1 = :fromc1 and fromc2 = :fromc2 for update [{fromc1 1} {fromc2 2} {toc 1}] false",
		"ExecuteKeyspaceID select fc1 from `dot.t1` where fc1 = :fromc1 and fc2 = :fromc2 lock in share mode [{fromc1 1} {fromc2 2} {toc 1}] false",
	})
	vc.verifyContext(t, ctx)
}

func TestConsistentLookupCreateThenDupkey(t *testing.T) {
	lookup := createConsistentLookup(t, "consistent_lookup", false)
	vc := &loggingVCursor{}
	vc.AddResult(nil, vterrors.New(vtrpcpb.Code_ALREADY_EXISTS, "(errno 1062) (sqlstate 23000) Duplicate entry, pass mysql error as it is"))
	vc.AddResult(makeTestResult(1), nil)
	vc.AddResult(makeTestResult(1), nil)
	vc.AddResult(&sqltypes.Result{}, nil)
	ctx := newTestContext()

	err := lookup.(Lookup).Create(ctx, vc, [][]sqltypes.Value{{
		sqltypes.NewInt64(1),
		sqltypes.NewInt64(2),
	}}, [][]byte{[]byte("test1")}, false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Duplicate entry, pass mysql error as it is")
	vc.verifyLog(t, []string{
		"ExecutePre insert into t(fromc1, fromc2, toc) values(:fromc1_0, :fromc2_0, :toc_0) [{fromc1_0 1} {fromc2_0 2} {toc_0 test1}] true",
		"ExecutePre select toc from t where fromc1 = :fromc1 and fromc2 = :fromc2 for update [{fromc1 1} {fromc2 2} {toc test1}] false",
		"ExecuteKeyspaceID select fc1 from `dot.t1` where fc1 = :fromc1 and fc2 = :fromc2 lock in share mode [{fromc1 1} {fromc2 2} {toc test1}] false",
	})
	vc.verifyContext(t, ctx)
}

func TestConsistentLookupCreateNonDupError(t *testing.T) {
	lookup := createConsistentLookup(t, "consistent_lookup", false)
	vc := &loggingVCursor{}
	vc.AddResult(nil, errors.New("general error"))
	ctx := newTestContext()

	err := lookup.(Lookup).Create(ctx, vc, [][]sqltypes.Value{{
		sqltypes.NewInt64(1),
		sqltypes.NewInt64(2),
	}}, [][]byte{[]byte("test1")}, false)
	want := "general error"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("lookup(query fail) err: %v, must contain %s", err, want)
	}
	vc.verifyLog(t, []string{
		"ExecutePre insert into t(fromc1, fromc2, toc) values(:fromc1_0, :fromc2_0, :toc_0) [{fromc1_0 1} {fromc2_0 2} {toc_0 test1}] true",
	})
	vc.verifyContext(t, ctx)
}

func TestConsistentLookupCreateThenBadRows(t *testing.T) {
	lookup := createConsistentLookup(t, "consistent_lookup", false)
	vc := &loggingVCursor{}
	vc.AddResult(nil, vterrors.New(vtrpcpb.Code_ALREADY_EXISTS, "(errno 1062) (sqlstate 23000) Duplicate entry"))
	vc.AddResult(makeTestResult(2), nil)
	ctx := newTestContext()

	err := lookup.(Lookup).Create(ctx, vc, [][]sqltypes.Value{{
		sqltypes.NewInt64(1),
		sqltypes.NewInt64(2),
	}}, [][]byte{[]byte("test1")}, false)
	want := "unexpected rows"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("lookup(query fail) err: %v, must contain %s", err, want)
	}
	vc.verifyLog(t, []string{
		"ExecutePre insert into t(fromc1, fromc2, toc) values(:fromc1_0, :fromc2_0, :toc_0) [{fromc1_0 1} {fromc2_0 2} {toc_0 test1}] true",
		"ExecutePre select toc from t where fromc1 = :fromc1 and fromc2 = :fromc2 for update [{fromc1 1} {fromc2 2} {toc test1}] false",
	})
	vc.verifyContext(t, ctx)
}

func TestConsistentLookupDelete(t *testing.T) {
	lookup := createConsistentLookup(t, "consistent_lookup", false)
	vc := &loggingVCursor{}
	vc.AddResult(&sqltypes.Result{}, nil)
	ctx := newTestContext()

	if err := lookup.(Lookup).Delete(ctx, vc, [][]sqltypes.Value{{
		sqltypes.NewInt64(1),
		sqltypes.NewInt64(2),
	}}, []byte("test")); err != nil {
		t.Error(err)
	}
	vc.verifyLog(t, []string{
		"ExecutePost delete from t where fromc1 = :fromc1 and fromc2 = :fromc2 and toc = :toc [{fromc1 1} {fromc2 2} {toc test}] true",
	})
	vc.verifyContext(t, ctx)
}

func TestConsistentLookupUpdate(t *testing.T) {
	lookup := createConsistentLookup(t, "consistent_lookup", false)
	vc := &loggingVCursor{}
	vc.AddResult(&sqltypes.Result{}, nil)
	vc.AddResult(&sqltypes.Result{}, nil)
	ctx := newTestContext()

	if err := lookup.(Lookup).Update(ctx, vc, []sqltypes.Value{
		sqltypes.NewInt64(1),
		sqltypes.NewInt64(2),
	}, []byte("test"), []sqltypes.Value{
		sqltypes.NewInt64(3),
		sqltypes.NewInt64(4),
	}); err != nil {
		t.Error(err)
	}
	vc.verifyLog(t, []string{
		"ExecutePost delete from t where fromc1 = :fromc1 and fromc2 = :fromc2 and toc = :toc [{fromc1 1} {fromc2 2} {toc test}] true",
		"ExecutePre insert into t(fromc1, fromc2, toc) values(:fromc1_0, :fromc2_0, :toc_0) [{fromc1_0 3} {fromc2_0 4} {toc_0 test}] true",
	})
	vc.verifyContext(t, ctx)
}

func TestConsistentLookupNoUpdate(t *testing.T) {
	lookup := createConsistentLookup(t, "consistent_lookup", false)
	vc := &loggingVCursor{}
	vc.AddResult(&sqltypes.Result{}, nil)
	vc.AddResult(&sqltypes.Result{}, nil)

	if err := lookup.(Lookup).Update(context.Background(), vc, []sqltypes.Value{
		sqltypes.NewInt64(1),
		sqltypes.NewInt64(2),
	}, []byte("test"), []sqltypes.Value{
		sqltypes.NewInt64(1),
		sqltypes.NewInt64(2),
	}); err != nil {
		t.Error(err)
	}
	vc.verifyLog(t, []string{})
}

func TestConsistentLookupUpdateBecauseComparableTypes(t *testing.T) {
	lookup := createConsistentLookup(t, "consistent_lookup", false)
	vc := &loggingVCursor{}

	type test struct {
		typ querypb.Type
		val string
	}

	tests := []test{
		{querypb.Type_TEXT, "some string"},
		{querypb.Type_VARCHAR, "some string"},
		{querypb.Type_CHAR, "some string"},
	}

	for _, val := range tests {
		t.Run(val.typ.String(), func(t *testing.T) {
			vc.AddResult(&sqltypes.Result{}, nil)
			vc.AddResult(&sqltypes.Result{}, nil)
			literal, err := sqltypes.NewValue(val.typ, []byte(val.val))
			require.NoError(t, err)

			err = lookup.(Lookup).Update(context.Background(), vc, []sqltypes.Value{literal, literal}, []byte("test"), []sqltypes.Value{literal, literal})
			require.NoError(t, err)
			vc.verifyLog(t, []string{})
			vc.log = nil
		})
	}
}

func createConsistentLookup(t *testing.T, name string, writeOnly bool) SingleColumn {
	t.Helper()
	write := "false"
	if writeOnly {
		write = "true"
	}
	l, err := CreateVindex(name, name, map[string]string{
		"table":      "t",
		"from":       "fromc1,fromc2",
		"to":         "toc",
		"write_only": write,
	})
	if err != nil {
		t.Fatal(err)
	}
	require.Empty(t, l.(ParamValidating).UnknownParams())
	cols := []sqlparser.IdentifierCI{
		sqlparser.NewIdentifierCI("fc1"),
		sqlparser.NewIdentifierCI("fc2"),
	}
	if err := l.(WantOwnerInfo).SetOwnerInfo("ks", "dot.t1", cols); err != nil {
		t.Fatal(err)
	}
	return l.(SingleColumn)
}

func newTestContext() context.Context {
	type testContextKey string // keep static checks from complaining about built-in types as context keys
	return context.WithValue(context.Background(), (testContextKey)("test"), "foo")
}

var _ VCursor = (*loggingVCursor)(nil)

type loggingVCursor struct {
	results  []*sqltypes.Result
	errors   []error
	index    int
	log      []string
	contexts []context.Context
}

func (vc *loggingVCursor) LookupRowLockShardSession() vtgatepb.CommitOrder {
	return vtgatepb.CommitOrder_PRE
}

func (vc *loggingVCursor) InTransactionAndIsDML() bool {
	return false
}

func (vc *loggingVCursor) ConnCollation() collations.ID {
	return vc.Environment().CollationEnv().DefaultConnectionCharset()
}

func (vc *loggingVCursor) Environment() *vtenv.Environment {
	return vtenv.NewTestEnv()
}

type bv struct {
	Name string
	Bv   string
}

func (vc *loggingVCursor) AddResult(qr *sqltypes.Result, err error) {
	vc.results = append(vc.results, qr)
	vc.errors = append(vc.errors, err)
}

func (vc *loggingVCursor) Execute(ctx context.Context, method string, query string, bindvars map[string]*querypb.BindVariable, rollbackOnError bool, co vtgatepb.CommitOrder) (*sqltypes.Result, error) {
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
	return vc.execute(ctx, name, query, bindvars, rollbackOnError)
}

func (vc *loggingVCursor) ExecuteKeyspaceID(ctx context.Context, keyspace string, ksid []byte, query string, bindVars map[string]*querypb.BindVariable, rollbackOnError, autocommit bool) (*sqltypes.Result, error) {
	return vc.execute(ctx, "ExecuteKeyspaceID", query, bindVars, rollbackOnError)
}

func (vc *loggingVCursor) execute(ctx context.Context, method string, query string, bindvars map[string]*querypb.BindVariable, rollbackOnError bool) (*sqltypes.Result, error) {
	if vc.index >= len(vc.results) {
		return nil, fmt.Errorf("ran out of results to return: %s", query)
	}
	bvl := make([]bv, 0, len(bindvars))
	for k, v := range bindvars {
		bvl = append(bvl, bv{Name: k, Bv: string(v.Value)})
	}
	sort.Slice(bvl, func(i, j int) bool { return bvl[i].Name < bvl[j].Name })
	vc.log = append(vc.log, fmt.Sprintf("%s %s %v %v", method, query, bvl, rollbackOnError))
	vc.contexts = append(vc.contexts, ctx)
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
			t.Errorf("log(%d):\n%q, want\n%q", i, got, want[i])
		}
	}
	if len(want) > len(vc.log) {
		t.Errorf("expecting queries: %v", want[len(vc.log):])
	}
}

func (vc *loggingVCursor) verifyContext(t *testing.T, want context.Context) {
	t.Helper()
	for i, got := range vc.contexts {
		if got != want {
			t.Errorf("context(%d):\ngot: %v\nwant: %v", i, got, want)
		}
	}
}

// create lookup result with one to one mapping
func makeTestResult(numRows int) *sqltypes.Result {
	result := &sqltypes.Result{
		Fields:       sqltypes.MakeTestFields("id|keyspace_id", "bigint|varbinary"),
		RowsAffected: uint64(numRows),
	}
	for i := 0; i < numRows; i++ {
		result.Rows = append(result.Rows, []sqltypes.Value{
			sqltypes.NewInt64(int64(i + 1)),
			sqltypes.NewVarBinary(strconv.Itoa(i + 1)),
		})
	}
	return result
}

// create lookup result with many to many mapping
func makeTestResultLookup(numRows []int) *sqltypes.Result {
	total := 0
	for _, t := range numRows {
		total += t
	}
	result := &sqltypes.Result{
		Fields:       sqltypes.MakeTestFields("id|keyspace_id", "bigint|varbinary"),
		RowsAffected: uint64(total),
	}
	for i, row := range numRows {
		for j := 0; j < row; j++ {
			result.Rows = append(result.Rows, []sqltypes.Value{
				sqltypes.NewInt64(int64(i + 1)),
				sqltypes.NewVarBinary(strconv.Itoa(j + 1)),
			})
		}
	}
	return result
}
