/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/
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

package vrepl

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	columns1   = ParseColumnList("c1")
	columns12  = ParseColumnList("c1,c2")
	columns123 = ParseColumnList("c1,c2,c3")
	columns2   = ParseColumnList("c2")
	columns21  = ParseColumnList("c2,c1")
	columns12A = ParseColumnList("c1,c2,ca")
)

func TestGetSharedUniqueKeys(t *testing.T) {
	tt := []struct {
		name                           string
		sourceUKs, targetUKs           [](*UniqueKey)
		renameMap                      map[string]string
		expectSourceUK, expectTargetUK *UniqueKey
	}{
		{
			name:           "empty",
			sourceUKs:      []*UniqueKey{},
			targetUKs:      []*UniqueKey{},
			renameMap:      map[string]string{},
			expectSourceUK: nil,
			expectTargetUK: nil,
		},
		{
			name: "half empty",
			sourceUKs: []*UniqueKey{
				{Name: "PRIMARY", Columns: *columns1},
			},
			targetUKs:      []*UniqueKey{},
			renameMap:      map[string]string{},
			expectSourceUK: nil,
			expectTargetUK: nil,
		},
		{
			name: "single identical",
			sourceUKs: []*UniqueKey{
				{Name: "PRIMARY", Columns: *columns1},
			},
			targetUKs: []*UniqueKey{
				{Name: "PRIMARY", Columns: *columns1},
			},
			renameMap:      map[string]string{},
			expectSourceUK: &UniqueKey{Name: "PRIMARY", Columns: *columns1},
			expectTargetUK: &UniqueKey{Name: "PRIMARY", Columns: *columns1},
		},
		{
			name: "single identical non pk",
			sourceUKs: []*UniqueKey{
				{Name: "uidx", Columns: *columns1},
			},
			targetUKs: []*UniqueKey{
				{Name: "uidx", Columns: *columns1},
			},
			renameMap:      map[string]string{},
			expectSourceUK: &UniqueKey{Name: "uidx", Columns: *columns1},
			expectTargetUK: &UniqueKey{Name: "uidx", Columns: *columns1},
		},
		{
			name: "single identical, source is nullable",
			sourceUKs: []*UniqueKey{
				{Name: "uidx", Columns: *columns1, HasNullable: true},
			},
			targetUKs: []*UniqueKey{
				{Name: "uidx", Columns: *columns1},
			},
			renameMap:      map[string]string{},
			expectSourceUK: nil,
			expectTargetUK: nil,
		},
		{
			name: "single identical, target is nullable",
			sourceUKs: []*UniqueKey{
				{Name: "uidx", Columns: *columns1},
			},
			targetUKs: []*UniqueKey{
				{Name: "uidx", Columns: *columns1, HasNullable: true},
			},
			renameMap:      map[string]string{},
			expectSourceUK: nil,
			expectTargetUK: nil,
		},
		{
			name: "single no shared",
			sourceUKs: []*UniqueKey{
				{Name: "uidx", Columns: *columns1},
			},
			targetUKs: []*UniqueKey{
				{Name: "uidx", Columns: *columns12},
			},
			renameMap:      map[string]string{},
			expectSourceUK: nil,
			expectTargetUK: nil,
		},
		{
			name: "single no shared different order",
			sourceUKs: []*UniqueKey{
				{Name: "uidx", Columns: *columns12},
			},
			targetUKs: []*UniqueKey{
				{Name: "uidx", Columns: *columns21},
			},
			renameMap:      map[string]string{},
			expectSourceUK: nil,
			expectTargetUK: nil,
		},
		{
			name: "single identical, source has FLOAT",
			sourceUKs: []*UniqueKey{
				{Name: "uidx", Columns: *columns1, HasFloat: true},
			},
			targetUKs: []*UniqueKey{
				{Name: "uidx", Columns: *columns1},
			},
			renameMap:      map[string]string{},
			expectSourceUK: nil,
			expectTargetUK: nil,
		},
		{
			name: "exact match",
			sourceUKs: []*UniqueKey{
				{Name: "uidx", Columns: *columns1},
				{Name: "uidx123", Columns: *columns123},
			},
			targetUKs: []*UniqueKey{
				{Name: "uidx", Columns: *columns12},
				{Name: "uidx123", Columns: *columns123},
			},
			renameMap:      map[string]string{},
			expectSourceUK: &UniqueKey{Name: "uidx123", Columns: *columns123},
			expectTargetUK: &UniqueKey{Name: "uidx123", Columns: *columns123},
		},
		{
			name: "exact match from multiple options",
			sourceUKs: []*UniqueKey{
				{Name: "uidx", Columns: *columns1},
				{Name: "uidx123", Columns: *columns123},
				{Name: "uidx12", Columns: *columns12},
			},
			targetUKs: []*UniqueKey{
				{Name: "uidx12", Columns: *columns12},
				{Name: "uidx", Columns: *columns12},
				{Name: "uidx123", Columns: *columns123},
			},
			renameMap:      map[string]string{},
			expectSourceUK: &UniqueKey{Name: "uidx123", Columns: *columns123},
			expectTargetUK: &UniqueKey{Name: "uidx123", Columns: *columns123},
		},
		{
			name: "exact match from multiple options reorder",
			sourceUKs: []*UniqueKey{
				{Name: "uidx12", Columns: *columns12},
				{Name: "uidx", Columns: *columns1},
				{Name: "uidx123", Columns: *columns123},
			},
			targetUKs: []*UniqueKey{
				{Name: "uidx", Columns: *columns21},
				{Name: "uidx123", Columns: *columns123},
				{Name: "uidx12", Columns: *columns12},
			},
			renameMap:      map[string]string{},
			expectSourceUK: &UniqueKey{Name: "uidx12", Columns: *columns12},
			expectTargetUK: &UniqueKey{Name: "uidx12", Columns: *columns12},
		},
		{
			name: "match different names",
			sourceUKs: []*UniqueKey{
				{Name: "uidx1", Columns: *columns1},
				{Name: "uidx12", Columns: *columns12},
				{Name: "uidx123", Columns: *columns123},
			},
			targetUKs: []*UniqueKey{
				{Name: "uidx21", Columns: *columns21},
				{Name: "uidx123", Columns: *columns123},
				{Name: "uidxother", Columns: *columns12},
			},
			renameMap:      map[string]string{},
			expectSourceUK: &UniqueKey{Name: "uidx12", Columns: *columns12},
			expectTargetUK: &UniqueKey{Name: "uidxother", Columns: *columns12},
		},
		{
			name: "match different names, nullable",
			sourceUKs: []*UniqueKey{
				{Name: "uidx1", Columns: *columns1},
				{Name: "uidx12", Columns: *columns12},
				{Name: "uidx123", Columns: *columns123},
			},
			targetUKs: []*UniqueKey{
				{Name: "uidx21", Columns: *columns21},
				{Name: "uidx123other", Columns: *columns123},
				{Name: "uidx12", Columns: *columns12, HasNullable: true},
			},
			renameMap:      map[string]string{},
			expectSourceUK: &UniqueKey{Name: "uidx123", Columns: *columns123},
			expectTargetUK: &UniqueKey{Name: "uidx123other", Columns: *columns123},
		},
		{
			name: "match different column names",
			sourceUKs: []*UniqueKey{
				{Name: "uidx1", Columns: *columns1},
				{Name: "uidx12", Columns: *columns12},
				{Name: "uidx123", Columns: *columns123},
			},
			targetUKs: []*UniqueKey{
				{Name: "uidx21", Columns: *columns21},
				{Name: "uidx12A", Columns: *columns12A},
			},
			renameMap:      map[string]string{"c3": "ca"},
			expectSourceUK: &UniqueKey{Name: "uidx123", Columns: *columns123},
			expectTargetUK: &UniqueKey{Name: "uidx12A", Columns: *columns12A},
		},
		{
			// enforce mapping from c3 to ca; will not match c3<->c3
			name: "no match identical column names",
			sourceUKs: []*UniqueKey{
				{Name: "uidx1", Columns: *columns1},
				{Name: "uidx12", Columns: *columns12},
				{Name: "uidx123", Columns: *columns123},
			},
			targetUKs: []*UniqueKey{
				{Name: "uidx21", Columns: *columns21},
				{Name: "uidx123", Columns: *columns123},
			},
			renameMap:      map[string]string{"c3": "ca"},
			expectSourceUK: nil,
			expectTargetUK: nil,
		},
		{
			name: "no match different column names",
			sourceUKs: []*UniqueKey{
				{Name: "uidx1", Columns: *columns1},
				{Name: "uidx12", Columns: *columns12},
				{Name: "uidx123", Columns: *columns123},
			},
			targetUKs: []*UniqueKey{
				{Name: "uidx21", Columns: *columns21},
				{Name: "uidx12A", Columns: *columns12A},
			},
			renameMap:      map[string]string{"c3": "cx"},
			expectSourceUK: nil,
			expectTargetUK: nil,
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			sourceUK, targetUK := GetSharedUniqueKeys(tc.sourceUKs, tc.targetUKs, tc.renameMap)
			assert.Equal(t, tc.expectSourceUK, sourceUK)
			assert.Equal(t, tc.expectTargetUK, targetUK)
		})
	}
}

func TestAddedUniqueKeys(t *testing.T) {
	emptyUniqueKeys := []*UniqueKey{}
	tt := []struct {
		name                 string
		sourceUKs, targetUKs [](*UniqueKey)
		renameMap            map[string]string
		expectAddedUKs       [](*UniqueKey)
		expectRemovedUKs     [](*UniqueKey)
	}{
		{
			name:             "empty",
			sourceUKs:        emptyUniqueKeys,
			targetUKs:        emptyUniqueKeys,
			renameMap:        map[string]string{},
			expectAddedUKs:   emptyUniqueKeys,
			expectRemovedUKs: emptyUniqueKeys,
		},
		{
			name: "UK removed",
			sourceUKs: []*UniqueKey{
				{Name: "PRIMARY", Columns: *columns1},
			},
			targetUKs:      emptyUniqueKeys,
			renameMap:      map[string]string{},
			expectAddedUKs: emptyUniqueKeys,
			expectRemovedUKs: []*UniqueKey{
				{Name: "PRIMARY", Columns: *columns1},
			},
		},
		{
			name: "NULLable UK removed",
			sourceUKs: []*UniqueKey{
				{Name: "PRIMARY", Columns: *columns1, HasNullable: true},
			},
			targetUKs:      emptyUniqueKeys,
			renameMap:      map[string]string{},
			expectAddedUKs: emptyUniqueKeys,
			expectRemovedUKs: []*UniqueKey{
				{Name: "PRIMARY", Columns: *columns1, HasNullable: true},
			},
		},
		{
			name:      "UK added",
			sourceUKs: emptyUniqueKeys,
			targetUKs: []*UniqueKey{
				{Name: "PRIMARY", Columns: *columns1},
			},
			renameMap: map[string]string{},
			expectAddedUKs: []*UniqueKey{
				{Name: "PRIMARY", Columns: *columns1},
			},
			expectRemovedUKs: emptyUniqueKeys,
		},
		{
			name: "single identical",
			sourceUKs: []*UniqueKey{
				{Name: "PRIMARY", Columns: *columns1},
			},
			targetUKs: []*UniqueKey{
				{Name: "PRIMARY", Columns: *columns1},
			},
			renameMap:        map[string]string{},
			expectAddedUKs:   emptyUniqueKeys,
			expectRemovedUKs: emptyUniqueKeys,
		},
		{
			name: "single identical non pk",
			sourceUKs: []*UniqueKey{
				{Name: "uidx", Columns: *columns1},
			},
			targetUKs: []*UniqueKey{
				{Name: "uidx", Columns: *columns1},
			},
			renameMap:        map[string]string{},
			expectAddedUKs:   emptyUniqueKeys,
			expectRemovedUKs: emptyUniqueKeys,
		},
		{
			name: "single identical, source is nullable",
			sourceUKs: []*UniqueKey{
				{Name: "uidx", Columns: *columns1, HasNullable: true},
			},
			targetUKs: []*UniqueKey{
				{Name: "uidx", Columns: *columns1},
			},
			renameMap:        map[string]string{},
			expectAddedUKs:   emptyUniqueKeys,
			expectRemovedUKs: emptyUniqueKeys,
		},
		{
			name: "single identical, target is nullable",
			sourceUKs: []*UniqueKey{
				{Name: "uidx", Columns: *columns1},
			},
			targetUKs: []*UniqueKey{
				{Name: "uidx", Columns: *columns1, HasNullable: true},
			},
			renameMap:        map[string]string{},
			expectAddedUKs:   emptyUniqueKeys,
			expectRemovedUKs: emptyUniqueKeys,
		},
		{
			name: "expand columns: not considered added",
			sourceUKs: []*UniqueKey{
				{Name: "uidx", Columns: *columns1},
			},
			targetUKs: []*UniqueKey{
				{Name: "uidx", Columns: *columns12},
			},
			renameMap:      map[string]string{},
			expectAddedUKs: emptyUniqueKeys,
			expectRemovedUKs: []*UniqueKey{
				{Name: "uidx", Columns: *columns1},
			},
		},
		{
			name: "expand columns, different order: not considered added",
			sourceUKs: []*UniqueKey{
				{Name: "uidx", Columns: *columns1},
			},
			targetUKs: []*UniqueKey{
				{Name: "uidx", Columns: *columns21},
			},
			renameMap:      map[string]string{},
			expectAddedUKs: emptyUniqueKeys,
			expectRemovedUKs: []*UniqueKey{
				{Name: "uidx", Columns: *columns1},
			},
		},
		{
			name: "reduced columns: considered added",
			sourceUKs: []*UniqueKey{
				{Name: "uidx", Columns: *columns12},
			},
			targetUKs: []*UniqueKey{
				{Name: "uidx", Columns: *columns1},
			},
			renameMap: map[string]string{},
			expectAddedUKs: []*UniqueKey{
				{Name: "uidx", Columns: *columns1},
			},
			expectRemovedUKs: emptyUniqueKeys,
		},
		{
			name: "reduced columns, multiple: considered added",
			sourceUKs: []*UniqueKey{
				{Name: "uidx12", Columns: *columns12},
				{Name: "uidx123", Columns: *columns123},
				{Name: "uidx2", Columns: *columns2},
			},
			targetUKs: []*UniqueKey{
				{Name: "uidx", Columns: *columns1},
			},
			renameMap: map[string]string{},
			expectAddedUKs: []*UniqueKey{
				{Name: "uidx", Columns: *columns1},
			},
			expectRemovedUKs: []*UniqueKey{
				{Name: "uidx2", Columns: *columns2},
			},
		},
		{
			name: "different order: not considered added",
			sourceUKs: []*UniqueKey{
				{Name: "uidx", Columns: *columns12},
			},
			targetUKs: []*UniqueKey{
				{Name: "uidx", Columns: *columns21},
			},
			renameMap:        map[string]string{},
			expectAddedUKs:   emptyUniqueKeys,
			expectRemovedUKs: emptyUniqueKeys,
		},
		{
			name: "no match, different columns",
			sourceUKs: []*UniqueKey{
				{Name: "uidx1", Columns: *columns1},
			},
			targetUKs: []*UniqueKey{
				{Name: "uidx2", Columns: *columns2},
			},
			renameMap: map[string]string{},
			expectAddedUKs: []*UniqueKey{
				{Name: "uidx2", Columns: *columns2},
			},
			expectRemovedUKs: []*UniqueKey{
				{Name: "uidx1", Columns: *columns1},
			},
		},
		{
			name: "one match, one expand",
			sourceUKs: []*UniqueKey{
				{Name: "uidx", Columns: *columns1},
				{Name: "uidx123", Columns: *columns123},
			},
			targetUKs: []*UniqueKey{
				{Name: "uidx", Columns: *columns12},
				{Name: "uidx123", Columns: *columns123},
			},
			renameMap:      map[string]string{},
			expectAddedUKs: emptyUniqueKeys,
			expectRemovedUKs: []*UniqueKey{
				{Name: "uidx", Columns: *columns1},
			},
		},
		{
			name: "exact match from multiple options",
			sourceUKs: []*UniqueKey{
				{Name: "uidx", Columns: *columns1},
				{Name: "uidx123", Columns: *columns123},
				{Name: "uidx12", Columns: *columns12},
			},
			targetUKs: []*UniqueKey{
				{Name: "uidx12", Columns: *columns12},
				{Name: "uidx", Columns: *columns12},
				{Name: "uidx123", Columns: *columns123},
			},
			renameMap:      map[string]string{},
			expectAddedUKs: emptyUniqueKeys,
			expectRemovedUKs: []*UniqueKey{
				{Name: "uidx", Columns: *columns1},
			},
		},
		{
			name: "exact match from multiple options reorder",
			sourceUKs: []*UniqueKey{
				{Name: "uidx12", Columns: *columns12},
				{Name: "uidx", Columns: *columns1},
				{Name: "uidx123", Columns: *columns123},
			},
			targetUKs: []*UniqueKey{
				{Name: "uidx", Columns: *columns21},
				{Name: "uidx123", Columns: *columns123},
				{Name: "uidx12", Columns: *columns12},
			},
			renameMap:      map[string]string{},
			expectAddedUKs: emptyUniqueKeys,
			expectRemovedUKs: []*UniqueKey{
				{Name: "uidx", Columns: *columns1},
			},
		},
		{
			name: "match different names",
			sourceUKs: []*UniqueKey{
				{Name: "uidx1", Columns: *columns1},
				{Name: "uidx12", Columns: *columns12},
				{Name: "uidx123", Columns: *columns123},
			},
			targetUKs: []*UniqueKey{
				{Name: "uidx21", Columns: *columns21},
				{Name: "uidx123", Columns: *columns123},
				{Name: "uidxother", Columns: *columns12},
			},
			renameMap:      map[string]string{},
			expectAddedUKs: emptyUniqueKeys,
			expectRemovedUKs: []*UniqueKey{
				{Name: "uidx1", Columns: *columns1},
			},
		},
		{
			name: "match different names, nullable",
			sourceUKs: []*UniqueKey{
				{Name: "uidx1", Columns: *columns1},
				{Name: "uidx12", Columns: *columns12},
				{Name: "uidx123", Columns: *columns123},
			},
			targetUKs: []*UniqueKey{
				{Name: "uidx21", Columns: *columns21},
				{Name: "uidx123other", Columns: *columns123},
				{Name: "uidx12", Columns: *columns12, HasNullable: true},
			},
			renameMap:      map[string]string{},
			expectAddedUKs: emptyUniqueKeys,
			expectRemovedUKs: []*UniqueKey{
				{Name: "uidx1", Columns: *columns1},
			},
		},
		{
			name: "match different column names, expand",
			sourceUKs: []*UniqueKey{
				{Name: "uidx1", Columns: *columns1},
				{Name: "uidx12", Columns: *columns12},
				{Name: "uidx123", Columns: *columns123},
			},
			targetUKs: []*UniqueKey{
				{Name: "uidx21", Columns: *columns21},
				{Name: "uidx12A", Columns: *columns12A},
			},
			renameMap:      map[string]string{"c3": "ca"},
			expectAddedUKs: emptyUniqueKeys,
			expectRemovedUKs: []*UniqueKey{
				{Name: "uidx1", Columns: *columns1},
			},
		},
		{
			name: "match different column names, no expand",
			sourceUKs: []*UniqueKey{
				{Name: "uidx123", Columns: *columns123},
			},
			targetUKs: []*UniqueKey{
				{Name: "uidx12A", Columns: *columns12A},
			},
			renameMap:        map[string]string{"c3": "ca"},
			expectAddedUKs:   emptyUniqueKeys,
			expectRemovedUKs: emptyUniqueKeys,
		},
		{
			// enforce mapping from c3 to ca; will not match c3<->c3
			name: "no match identical column names, expand",
			sourceUKs: []*UniqueKey{
				{Name: "uidx1", Columns: *columns1},
				{Name: "uidx12", Columns: *columns12},
				{Name: "uidx123", Columns: *columns123},
			},
			targetUKs: []*UniqueKey{
				{Name: "uidx21", Columns: *columns21},
				{Name: "uidx123", Columns: *columns123},
			},
			renameMap: map[string]string{"c3": "ca"},
			// 123 expands 12, so even though 3 is mapped to A, 123 is still not more constrained.
			expectAddedUKs: emptyUniqueKeys,
			expectRemovedUKs: []*UniqueKey{
				{Name: "uidx1", Columns: *columns1},
			},
		},
		{
			// enforce mapping from c3 to ca; will not match c3<->c3
			name: "no match identical column names, no expand",
			sourceUKs: []*UniqueKey{
				{Name: "uidx123", Columns: *columns123},
			},
			targetUKs: []*UniqueKey{
				{Name: "uidx123", Columns: *columns123},
			},
			renameMap: map[string]string{"c3": "ca"},
			expectAddedUKs: []*UniqueKey{
				{Name: "uidx123", Columns: *columns123},
			},
			expectRemovedUKs: emptyUniqueKeys,
		},
		{
			name: "no match for different column names, expand",
			sourceUKs: []*UniqueKey{
				{Name: "uidx1", Columns: *columns1},
				{Name: "uidx12", Columns: *columns12},
				{Name: "uidx123", Columns: *columns123},
			},
			targetUKs: []*UniqueKey{
				{Name: "uidx21", Columns: *columns21},
				{Name: "uidx12A", Columns: *columns12A},
			},
			renameMap: map[string]string{"c3": "cx"},
			// 123 expands 12, so even though 3 is mapped to x, 123 is still not more constrained.
			expectAddedUKs: emptyUniqueKeys,
			expectRemovedUKs: []*UniqueKey{
				{Name: "uidx1", Columns: *columns1},
			},
		},
		{
			name: "no match for different column names, no expand",
			sourceUKs: []*UniqueKey{
				{Name: "uidx123", Columns: *columns123},
			},
			targetUKs: []*UniqueKey{
				{Name: "uidx12A", Columns: *columns12A},
			},
			renameMap: map[string]string{"c3": "cx"},
			expectAddedUKs: []*UniqueKey{
				{Name: "uidx12A", Columns: *columns12A},
			},
			expectRemovedUKs: []*UniqueKey{
				{Name: "uidx123", Columns: *columns123},
			},
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			addedUKs := AddedUniqueKeys(tc.sourceUKs, tc.targetUKs, tc.renameMap)
			assert.Equal(t, tc.expectAddedUKs, addedUKs)
			removedUKs := RemovedUniqueKeys(tc.sourceUKs, tc.targetUKs, tc.renameMap)
			assert.Equal(t, tc.expectRemovedUKs, removedUKs)
		})
	}
}
