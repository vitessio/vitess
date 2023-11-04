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
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func lookupHashUniqueCreateVindexTestCase(
	testName string,
	vindexParams map[string]string,
	expectErr error,
	expectUnknownParams []string,
) createVindexTestCase {
	return createVindexTestCase{
		testName: testName,

		vindexType:   "lookup_hash_unique",
		vindexName:   "lookup_hash_unique",
		vindexParams: vindexParams,

		expectCost:          10,
		expectErr:           expectErr,
		expectIsUnique:      true,
		expectNeedsVCursor:  true,
		expectString:        "lookup_hash_unique",
		expectUnknownParams: expectUnknownParams,
	}
}

func TestLookupHashUniqueCreateVindex(t *testing.T) {
	testLookupCreateVindexCommonCases(t, lookupHashUniqueCreateVindexTestCase)
}

func TestLookupHashUniqueNew(t *testing.T) {
	l := createLookup(t, "lookup_hash_unique", false /* writeOnly */)
	if want, got := l.(*LookupHashUnique).writeOnly, false; got != want {
		t.Errorf("Create(lookup, false): %v, want %v", got, want)
	}

	vindex, err := CreateVindex("lookup_hash_unique", "lookup_hash_unique", map[string]string{
		"table":      "t",
		"from":       "fromc",
		"to":         "toc",
		"write_only": "true",
	})
	unknownParams := vindex.(ParamValidating).UnknownParams()
	require.Empty(t, unknownParams)
	require.NoError(t, err)

	l = vindex.(SingleColumn)
	if want, got := l.(*LookupHashUnique).writeOnly, true; got != want {
		t.Errorf("Create(lookup, false): %v, want %v", got, want)
	}

	vdx, err := CreateVindex("lookup_hash_unique", "lookup_hash_unique", map[string]string{
		"table":      "t",
		"from":       "fromc",
		"to":         "toc",
		"write_only": "invalid",
	})
	want := "write_only value must be 'true' or 'false': 'invalid'"
	if err == nil || err.Error() != want {
		t.Errorf("Create(bad_scatter): %v, want %s", err, want)
	}
	if err == nil {
		unknownParams = vdx.(ParamValidating).UnknownParams()
		require.Empty(t, unknownParams)
	}
}

func TestLookupHashUniqueMap(t *testing.T) {
	lhu := createLookup(t, "lookup_hash_unique", false /* writeOnly */)
	vc := &vcursor{numRows: 1}

	got, err := lhu.Map(context.Background(), vc, []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)})
	require.NoError(t, err)
	want := []key.Destination{
		key.DestinationKeyspaceID([]byte("\x16k@\xb4J\xbaK\xd6")),
		key.DestinationNone{},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Map(): %#v, want %+v", got, want)
	}

	vc.numRows = 0
	got, err = lhu.Map(context.Background(), vc, []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)})
	require.NoError(t, err)
	want = []key.Destination{
		key.DestinationNone{},
		key.DestinationNone{},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Map(): %#v, want %+v", got, want)
	}

	vc.numRows = 2
	_, err = lhu.Map(context.Background(), vc, []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)})
	wantErr := "LookupHash.Map: unexpected multiple results from vindex t: INT64(1)"
	if err == nil || err.Error() != wantErr {
		t.Errorf("lhu(query fail) err: %v, want %s", err, wantErr)
	}

	// Test conversion fail.
	vc.result = sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("b|a", "bigint|varbinary"),
		"1|notint",
	)
	got, err = lhu.Map(context.Background(), vc, []sqltypes.Value{sqltypes.NewInt64(1)})
	require.NoError(t, err)
	want = []key.Destination{
		key.DestinationNone{},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Map(): %#v, want %+v", got, want)
	}

	// Test query fail.
	vc.mustFail = true
	_, err = lhu.Map(context.Background(), vc, []sqltypes.Value{sqltypes.NewInt64(1)})
	wantErr = "lookup.Map: execute failed"
	if err == nil || err.Error() != wantErr {
		t.Errorf("lhu(query fail) err: %v, want %s", err, wantErr)
	}
	vc.mustFail = false
}

func TestLookupHashUniqueMapWriteOnly(t *testing.T) {
	lhu := createLookup(t, "lookup_hash_unique", true)
	vc := &vcursor{numRows: 0}

	got, err := lhu.Map(context.Background(), vc, []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)})
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

func TestLookupHashUniqueVerify(t *testing.T) {
	lhu := createLookup(t, "lookup_hash_unique", false /* writeOnly */)
	vc := &vcursor{numRows: 1}

	// The check doesn't actually happen. But we give correct values
	// to avoid confusion.
	got, err := lhu.Verify(context.Background(), vc, []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)}, [][]byte{[]byte("\x16k@\xb4J\xbaK\xd6"), []byte("\x06\xe7\xea\"Βp\x8f")})
	require.NoError(t, err)
	want := []bool{true, true}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("lhu.Verify(match): %v, want %v", got, want)
	}

	vc.numRows = 0
	got, err = lhu.Verify(context.Background(), vc, []sqltypes.Value{sqltypes.NewInt64(1)}, [][]byte{[]byte("\x16k@\xb4J\xbaK\xd6")})
	require.NoError(t, err)
	want = []bool{false}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("lhu.Verify(mismatch): %v, want %v", got, want)
	}

	_, err = lhu.Verify(context.Background(), vc, []sqltypes.Value{sqltypes.NewInt64(1)}, [][]byte{[]byte("bogus")})
	wantErr := "lookup.Verify.vunhash: invalid keyspace id: 626f677573"
	if err == nil || err.Error() != wantErr {
		t.Errorf("lhu.Verify(bogus) err: %v, want %s", err, wantErr)
	}
}

func TestLookupHashUniqueVerifyWriteOnly(t *testing.T) {
	lhu := createLookup(t, "lookup_hash_unique", true)
	vc := &vcursor{numRows: 0}

	got, err := lhu.Verify(context.Background(), vc, []sqltypes.Value{sqltypes.NewInt64(1)}, [][]byte{[]byte("test")})
	require.NoError(t, err)
	want := []bool{true}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("lhu.Verify: %v, want %v", got, want)
	}
	if got, want := len(vc.queries), 0; got != want {
		t.Errorf("vc.queries length: %v, want %v", got, want)
	}
}

func TestLookupHashUniqueCreate(t *testing.T) {
	lhu := createLookup(t, "lookup_hash_unique", false /* writeOnly */)
	vc := &vcursor{}

	err := lhu.(Lookup).Create(context.Background(), vc, [][]sqltypes.Value{{sqltypes.NewInt64(1)}}, [][]byte{[]byte("\x16k@\xb4J\xbaK\xd6")}, false /* ignoreMode */)
	require.NoError(t, err)
	if got, want := len(vc.queries), 1; got != want {
		t.Errorf("vc.queries length: %v, want %v", got, want)
	}

	err = lhu.(Lookup).Create(context.Background(), vc, [][]sqltypes.Value{{sqltypes.NewInt64(1)}}, [][]byte{[]byte("bogus")}, false /* ignoreMode */)
	want := "lookup.Create.vunhash: invalid keyspace id: 626f677573"
	if err == nil || err.Error() != want {
		t.Errorf("lhu.Create(bogus) err: %v, want %s", err, want)
	}
}

func TestLookupHashUniqueDelete(t *testing.T) {
	lhu := createLookup(t, "lookup_hash_unique", false /* writeOnly */)
	vc := &vcursor{}

	err := lhu.(Lookup).Delete(context.Background(), vc, [][]sqltypes.Value{{sqltypes.NewInt64(1)}}, []byte("\x16k@\xb4J\xbaK\xd6"))
	require.NoError(t, err)
	if got, want := len(vc.queries), 1; got != want {
		t.Errorf("vc.queries length: %v, want %v", got, want)
	}

	err = lhu.(Lookup).Delete(context.Background(), vc, [][]sqltypes.Value{{sqltypes.NewInt64(1)}}, []byte("bogus"))
	want := "lookup.Delete.vunhash: invalid keyspace id: 626f677573"
	if err == nil || err.Error() != want {
		t.Errorf("lhu.Delete(bogus) err: %v, want %s", err, want)
	}
}

func TestLookupHashUniqueUpdate(t *testing.T) {
	lhu := createLookup(t, "lookup_hash_unique", false /* writeOnly */)
	vc := &vcursor{}

	err := lhu.(Lookup).Update(context.Background(), vc, []sqltypes.Value{sqltypes.NewInt64(1)}, []byte("\x16k@\xb4J\xbaK\xd6"), []sqltypes.Value{sqltypes.NewInt64(2)})
	require.NoError(t, err)
	if got, want := len(vc.queries), 2; got != want {
		t.Errorf("vc.queries length: %v, want %v", got, want)
	}
}
