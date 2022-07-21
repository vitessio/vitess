//go:build gofuzz
// +build gofuzz

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

package vindexes

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"sync"
	"testing"

	fuzz "github.com/AdaLogics/go-fuzz-headers"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

var initter sync.Once

func onceInit() {
	testing.Init()
}

// All querypbTypes
var querypbTypes = []querypb.Type{querypb.Type_NULL_TYPE,
	querypb.Type_INT8,
	querypb.Type_UINT8,
	querypb.Type_INT16,
	querypb.Type_UINT16,
	querypb.Type_INT24,
	querypb.Type_UINT24,
	querypb.Type_INT32,
	querypb.Type_UINT32,
	querypb.Type_INT64,
	querypb.Type_UINT64,
	querypb.Type_FLOAT32,
	querypb.Type_FLOAT64,
	querypb.Type_TIMESTAMP,
	querypb.Type_DATE,
	querypb.Type_TIME,
	querypb.Type_DATETIME,
	querypb.Type_YEAR,
	querypb.Type_DECIMAL,
	querypb.Type_TEXT,
	querypb.Type_BLOB,
	querypb.Type_VARCHAR,
	querypb.Type_VARBINARY,
	querypb.Type_CHAR,
	querypb.Type_BINARY,
	querypb.Type_BIT,
	querypb.Type_ENUM,
	querypb.Type_SET,
	querypb.Type_GEOMETRY,
	querypb.Type_JSON,
	querypb.Type_EXPRESSION}

// All valid vindexes
var availableVindexes = []string{"binary",
	"unicode_loose_md5",
	"binary_md5",
	"lookup_hash",
	"lookup_hash_unique",
	"lookup",
	"lookup_unique",
	"lookup_unicodeloosemd5_hash",
	"lookup_unicodeloosemd5_hash_unique",
	"hash",
	"region_experimental",
	"consistent_lookup",
	"consistent_lookup_unique",
	"cfc",
	"numeric",
	"numeric_static_map",
	"xxhash",
	"unicode_loose_xxhash",
	"reverse_bits",
	"region_json",
	"null"}

// FuzzVindex implements the vindexes fuzzer
func FuzzVindex(data []byte) int {
	initter.Do(onceInit)
	f := fuzz.NewConsumer(data)

	// Write region map file
	createdFile, err := writeRegionMapFile(f)
	if err != nil {
		return 0
	}
	defer os.Remove(createdFile)

	// Choose type of vindex
	index, err := f.GetInt()
	if err != nil {
		return 0
	}
	targetVindex := availableVindexes[index%len(availableVindexes)]

	// Create params map
	params := make(map[string]string)
	err = f.FuzzMap(&params)
	if err != nil {
		return 0
	}
	params["region_map"] = createdFile

	// Create the vindex
	l, err := CreateVindex(targetVindex, targetVindex, params)
	if err != nil {
		return 0
	}

	// Create values to be passed to our targets
	allValues, err := createValues(f)
	if err != nil {
		return 0
	}

	vc := &loggingVCursor{}

	// Time to call the actual targets. THere are two:
	// 1) Map()
	// 2) Create()

	// Target 1:
	_, _ = Map(ctx, l, vc, allValues)

	// Target 2:
	s1 := reflect.TypeOf(l).String()
	switch s1 {
	case "*vindexes.ConsistentLookup", "*vindexes.LookupHash",
		"*vindexes.LookupHashUnique", "*vindexes.LookupNonUnique",
		"*vindexes.LookupUnique", "*vindexes.LookupUnicodeLooseMD5Hash",
		"*vindexes.LookupUnicodeLooseMD5HashUnique", "*vindexes.clCommon",
		"*vindexes.lookupInternal":
		ksids, err := createKsids(f)
		if err != nil {
			return 0
		}
		_ = l.(Lookup).Create(ctx, vc, allValues, ksids, false /* ignoreMode */)
	}
	return 1
}

// Creates a slice of byte slices to use as ksids
func createKsids(f *fuzz.ConsumeFuzzer) ([][]byte, error) {
	// create ksids
	noOfTotalKsids, err := f.GetInt()
	if err != nil {
		return nil, err
	}
	ksids := make([][]byte, noOfTotalKsids%100)
	for i := 0; i < noOfTotalKsids%100; i++ {
		newBytes, err := f.GetBytes()
		if err != nil {
			return nil, err
		}
		ksids = append(ksids, newBytes)
	}
	return ksids, nil
}

// Checks if a byte slice is valid json
func IsJSON(data []byte) bool {
	var js json.RawMessage
	return json.Unmarshal(data, &js) == nil
}

// Create and write the RegionMap file
func writeRegionMapFile(f *fuzz.ConsumeFuzzer) (string, error) {
	fileBytes, err := f.GetBytes()
	if err != nil || !IsJSON(fileBytes) {
		return "", fmt.Errorf("Could not get valid bytes")
	}

	createdFile, err := os.CreateTemp("", "tmpfile-")
	if err != nil {
		return "", err
	}
	defer createdFile.Close()

	_, err = createdFile.Write(fileBytes)
	if err != nil {
		return "", err
	}
	return createdFile.Name(), nil

}

// Creates a slice of slices of sqltypes.Value
func createValues(f *fuzz.ConsumeFuzzer) ([][]sqltypes.Value, error) {
	noOfTotalValues, err := f.GetInt()
	if err != nil {
		return nil, err
	}
	allValues := make([][]sqltypes.Value, 0)
	for i := 0; i < noOfTotalValues%100; i++ {
		noOfValues, err := f.GetInt()
		if err != nil {
			return nil, err
		}
		values := make([]sqltypes.Value, 0)
		for i2 := 0; i2 < noOfValues%100; i2++ {
			v, err := createValue(f)
			if err != nil {
				return nil, err
			}
			values = append(values, v)
		}
		allValues = append(allValues, values)
	}
	return allValues, nil
}

// Creates a single sqltypes.Value
func createValue(f *fuzz.ConsumeFuzzer) (sqltypes.Value, error) {
	typeIndex, err := f.GetInt()
	if err != nil {
		return sqltypes.Value{}, err
	}

	inputBytes, err := f.GetBytes()
	if err != nil {
		return sqltypes.Value{}, err
	}

	v := querypbTypes[typeIndex%len(querypbTypes)]
	return sqltypes.NewValue(v, inputBytes)
}
