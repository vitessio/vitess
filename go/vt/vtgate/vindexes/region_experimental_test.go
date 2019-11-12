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
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
)

func TestRegionExperimentalMapMulti1(t *testing.T) {
	ge, err := createRegionVindex(t, "region_experimental", "f1,f2", 1)
	assert.NoError(t, err)
	got, err := ge.(MultiColumn).MapMulti(nil, [][]sqltypes.Value{{
		sqltypes.NewInt64(1), sqltypes.NewInt64(1),
	}, {
		sqltypes.NewInt64(1), sqltypes.NewInt64(255),
	}, {
		sqltypes.NewInt64(1), sqltypes.NewInt64(256),
	}, {
		// Invalid length.
		sqltypes.NewInt64(1),
	}, {
		// Invalid id.
		sqltypes.NewVarBinary("abcd"), sqltypes.NewInt64(256),
	}, {
		// Invalid region.
		sqltypes.NewInt64(1), sqltypes.NewVarBinary("abcd"),
	}})
	assert.NoError(t, err)

	want := []key.Destination{
		key.DestinationKeyspaceID([]byte("\x01\x16k@\xb4J\xbaK\xd6")),
		key.DestinationKeyspaceID([]byte("\xff\x16k@\xb4J\xbaK\xd6")),
		key.DestinationKeyspaceID([]byte("\x00\x16k@\xb4J\xbaK\xd6")),
		key.DestinationNone{},
		key.DestinationNone{},
		key.DestinationNone{},
	}
	assert.Equal(t, want, got)
}

func TestRegionExperimentalMapMulti2(t *testing.T) {
	ge, err := createRegionVindex(t, "region_experimental", "f1,f2", 2)
	assert.NoError(t, err)
	got, err := ge.(MultiColumn).MapMulti(nil, [][]sqltypes.Value{{
		sqltypes.NewInt64(1), sqltypes.NewInt64(1),
	}, {
		sqltypes.NewInt64(1), sqltypes.NewInt64(255),
	}, {
		sqltypes.NewInt64(1), sqltypes.NewInt64(256),
	}, {
		sqltypes.NewInt64(1), sqltypes.NewInt64(0x10000),
	}})
	assert.NoError(t, err)

	want := []key.Destination{
		key.DestinationKeyspaceID([]byte("\x00\x01\x16k@\xb4J\xbaK\xd6")),
		key.DestinationKeyspaceID([]byte("\x00\xff\x16k@\xb4J\xbaK\xd6")),
		key.DestinationKeyspaceID([]byte("\x01\x00\x16k@\xb4J\xbaK\xd6")),
		key.DestinationKeyspaceID([]byte("\x00\x00\x16k@\xb4J\xbaK\xd6")),
	}
	assert.Equal(t, want, got)
}

func TestRegionExperimentalVerifyMulti(t *testing.T) {

	ge, err := createRegionVindex(t, "region_experimental", "f1,f2", 1)
	assert.NoError(t, err)
	vals := [][]sqltypes.Value{{
		// One for match
		sqltypes.NewInt64(1), sqltypes.NewInt64(1),
	}, {
		// One for mismatch by lookup
		sqltypes.NewInt64(1), sqltypes.NewInt64(1),
	}, {
		// One for mismatch
		sqltypes.NewInt64(1), sqltypes.NewInt64(1),
	}, {
		// One invalid value
		sqltypes.NewInt64(1),
	}}
	ksids := [][]byte{
		[]byte("\x01\x16k@\xb4J\xbaK\xd6"),
		[]byte("\x01\x16k@\xb4J\xbaK\xd6"),
		[]byte("no match"),
		[]byte(""),
	}
	vc := &loggingVCursor{}
	vc.AddResult(makeTestResult(1), nil)
	// The second value should return a mismatch.
	vc.AddResult(&sqltypes.Result{}, nil)
	vc.AddResult(makeTestResult(1), nil)
	vc.AddResult(makeTestResult(1), nil)

	want := []bool{true, false, false, false}
	got, err := ge.(MultiColumn).VerifyMulti(vc, vals, ksids)
	assert.NoError(t, err)
	vc.verifyLog(t, []string{
		"ExecutePre select f1 from t where f1 = :f1 and toc = :toc [{f1 1} {toc \x01\x16k@\xb4J\xbaK\xd6}] false",
		"ExecutePre select f1 from t where f1 = :f1 and toc = :toc [{f1 1} {toc \x01\x16k@\xb4J\xbaK\xd6}] false",
		"ExecutePre select f1 from t where f1 = :f1 and toc = :toc [{f1 1} {toc no match}] false",
		"ExecutePre select f1 from t where f1 = :f1 and toc = :toc [{f1 1} {toc }] false",
	})
	assert.Equal(t, want, got)
}

func TestRegionExperimentalCreateErrors(t *testing.T) {
	_, err := createRegionVindex(t, "region_experimental", "f1,f2", 3)
	assert.EqualError(t, err, "region_bits must be 1 or 2: 3")
	_, err = CreateVindex("region_experimental", "region_experimental", nil)
	assert.EqualError(t, err, "region_experimental missing region_bytes param")
	_, err = createRegionVindex(t, "region_experimental", "f1", 2)
	assert.EqualError(t, err, "two columns are required for region_experimental: [f1]")
}

func createRegionVindex(t *testing.T, name, from string, rb int) (Vindex, error) {
	return CreateVindex(name, name, map[string]string{
		"region_bytes": strconv.Itoa(rb),
		"table":        "t",
		"from":         from,
		"to":           "toc",
	})
}
