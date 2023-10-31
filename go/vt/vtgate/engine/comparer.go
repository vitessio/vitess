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

package engine

import (
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

// comparer is the struct that has the logic for comparing two rows in the result set
type comparer struct {
	orderBy, weightString, starColFixedIndex int
	collationID                              collations.ID
	desc                                     bool
}

// compare compares two rows given the comparer and returns which one should be earlier in the result set
// -1 if the first row should be earlier
// 1 is the second row should be earlier
// 0 if both the rows have equal ordering
func (c *comparer) compare(r1, r2 []sqltypes.Value) int {
	var colIndex int
	if c.starColFixedIndex > c.orderBy && c.starColFixedIndex < len(r1) {
		colIndex = c.starColFixedIndex
	} else {
		colIndex = c.orderBy
	}

	v1 := r1[colIndex]
	v2 := r2[colIndex]
	cmp := v1.TinyWeightCmp(v2)

	if cmp == 0 {
		var err error
		cmp, err = evalengine.NullsafeCompare(v1, v2, c.collationID)
		if err != nil {
			_, isCollationErr := err.(evalengine.UnsupportedCollationError)
			if !isCollationErr || c.weightString == -1 {
				panic(err)
			}
			// in case of a comparison or collation error switch to using the weight string column for ordering
			c.orderBy = c.weightString
			c.weightString = -1
			cmp, err = evalengine.NullsafeCompare(r1[c.orderBy], r2[c.orderBy], c.collationID)
			if err != nil {
				panic(err)
			}
		}
	}
	// change the result if descending ordering is required
	if c.desc {
		cmp = -cmp
	}
	return cmp
}

// extractSlices extracts the three fields of OrderByParams into a slice of comparers
func extractSlices(input []OrderByParams) []*comparer {
	var result []*comparer
	for _, order := range input {
		result = append(result, &comparer{
			orderBy:           order.Col,
			weightString:      order.WeightStringCol,
			desc:              order.Desc,
			starColFixedIndex: order.StarColFixedIndex,
			collationID:       order.Type.Coll,
		})
	}
	return result
}
