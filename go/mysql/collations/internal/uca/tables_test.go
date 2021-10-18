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

package uca_test

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/collations/internal/encoding"
	"vitess.io/vitess/go/mysql/collations/internal/uca"
)

func verifyAllCodepoints(t *testing.T, expected map[string][]uint16, weights uca.WeightTable, layout uca.TableLayout) {
	t.Helper()

	maxCodepoint := int(layout.MaxCodepoint())
	for cp := 0; cp <= maxCodepoint; cp++ {
		vitessWeights := layout.DebugWeights(weights, rune(cp))
		codepoint := fmt.Sprintf("U+%04X", cp)
		mysqlWeights, mysqlFound := expected[codepoint]

		if len(vitessWeights) == 0 {
			if mysqlFound {
				t.Errorf("missing MySQL weight in Vitess' tables: U+%04X", cp)
				continue
			}
		} else {
			if !mysqlFound {
				t.Errorf("missing Vitess weight in MySQL's tables: U+%04X", cp)
				continue
			}

			if len(mysqlWeights) != len(vitessWeights) {
				t.Errorf("wrong number of collation entities for U+%04X: mysql=%v vs vitess=%v", cp, mysqlWeights, vitessWeights)
				continue
			}

			for i := range vitessWeights {
				a, b := mysqlWeights[i], vitessWeights[i]
				if a != b {
					t.Errorf("weight mismatch for U+%04X (collation entity %d): mysql=%v vitess=%v", cp, i+1, a, b)
				}
			}
		}
	}
}

func loadExpectedWeights(t *testing.T, weights string) map[string][]uint16 {
	fullpath := fmt.Sprintf("../../testdata/mysqldata/%s.json", weights)
	weightsMysqlFile, err := os.Open(fullpath)
	if err != nil {
		t.Skipf("failed to load %q (did you run 'colldump' locally?)", fullpath)
	}

	var meta struct {
		Weights map[string][]uint16
	}
	dec := json.NewDecoder(weightsMysqlFile)
	require.NoError(t, dec.Decode(&meta))
	return meta.Weights
}

func TestWeightsForAllCodepoints(t *testing.T) {
	testWeightsFromMysql := loadExpectedWeights(t, "utf8mb4_0900_ai_ci")
	verifyAllCodepoints(t, testWeightsFromMysql, uca.WeightTable_uca900, uca.TableLayout_uca900{})
}

func TestWeightTablesAreDeduplicated(t *testing.T) {
	sliceptr := func(table uca.WeightTable) uintptr {
		hdr := (*reflect.SliceHeader)(unsafe.Pointer(&table))
		return hdr.Data
	}

	uniqueTables := make(map[uintptr]int)
	for _, col := range collations.All() {
		if uca, ok := col.(collations.CollationUCA); ok {
			weights, _ := uca.UnicodeWeightsTable()
			uniqueTables[sliceptr(weights)]++
		}
	}

	var total int
	for _, count := range uniqueTables {
		total += count
	}
	average := float64(total) / float64(len(uniqueTables))
	if average < 10.0/3.0 {
		t.Fatalf("weight tables are not deduplicated, average table reuse: %f", average)
	}
}

func TestTailoringPatchApplication(t *testing.T) {
	for _, col := range collations.All() {
		uca, ok := col.(collations.CollationUCA)
		if !ok {
			continue
		}
		switch uca.Encoding().(type) {
		case encoding.Encoding_utf8mb4:
		default:
			continue
		}

		weightTable, tableLayout := uca.UnicodeWeightsTable()
		t.Run(col.Name(), func(t *testing.T) {
			expected := loadExpectedWeights(t, col.Name())
			verifyAllCodepoints(t, expected, weightTable, tableLayout)
		})
	}
}
