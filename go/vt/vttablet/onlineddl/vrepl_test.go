/*
   Copyright 2016 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package onlineddl

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/vt/vttablet/onlineddl/vrepl"
)

var (
	columns1   = vrepl.ParseColumnList("c1")
	columns12  = vrepl.ParseColumnList("c1,c2")
	columns123 = vrepl.ParseColumnList("c1,c2,c3")
	columns2   = vrepl.ParseColumnList("c2")
	columns21  = vrepl.ParseColumnList("c2,c1")
	columns12A = vrepl.ParseColumnList("c1,c2,ca")
)

func TestGetSharedUniqueKeys(t *testing.T) {
	tt := []struct {
		name                           string
		sourceUKs, targetUKs           [](*vrepl.UniqueKey)
		renameMap                      map[string]string
		expectSourceUK, expectTargetUK *vrepl.UniqueKey
	}{
		{
			name:           "empty",
			sourceUKs:      []*vrepl.UniqueKey{},
			targetUKs:      []*vrepl.UniqueKey{},
			renameMap:      map[string]string{},
			expectSourceUK: nil,
			expectTargetUK: nil,
		},
		{
			name: "half empty",
			sourceUKs: []*vrepl.UniqueKey{
				{Name: "PRIMARY", Columns: *columns1},
			},
			targetUKs:      []*vrepl.UniqueKey{},
			renameMap:      map[string]string{},
			expectSourceUK: nil,
			expectTargetUK: nil,
		},
		{
			name: "single identical",
			sourceUKs: []*vrepl.UniqueKey{
				{Name: "PRIMARY", Columns: *columns1},
			},
			targetUKs: []*vrepl.UniqueKey{
				{Name: "PRIMARY", Columns: *columns1},
			},
			renameMap:      map[string]string{},
			expectSourceUK: &vrepl.UniqueKey{Name: "PRIMARY", Columns: *columns1},
			expectTargetUK: &vrepl.UniqueKey{Name: "PRIMARY", Columns: *columns1},
		},
		{
			name: "single identical non pk",
			sourceUKs: []*vrepl.UniqueKey{
				{Name: "uidx", Columns: *columns1},
			},
			targetUKs: []*vrepl.UniqueKey{
				{Name: "uidx", Columns: *columns1},
			},
			renameMap:      map[string]string{},
			expectSourceUK: &vrepl.UniqueKey{Name: "uidx", Columns: *columns1},
			expectTargetUK: &vrepl.UniqueKey{Name: "uidx", Columns: *columns1},
		},
		{
			name: "single identical, source is nullable",
			sourceUKs: []*vrepl.UniqueKey{
				{Name: "uidx", Columns: *columns1, HasNullable: true},
			},
			targetUKs: []*vrepl.UniqueKey{
				{Name: "uidx", Columns: *columns1},
			},
			renameMap:      map[string]string{},
			expectSourceUK: nil,
			expectTargetUK: nil,
		},
		{
			name: "single identical, target is nullable",
			sourceUKs: []*vrepl.UniqueKey{
				{Name: "uidx", Columns: *columns1},
			},
			targetUKs: []*vrepl.UniqueKey{
				{Name: "uidx", Columns: *columns1, HasNullable: true},
			},
			renameMap:      map[string]string{},
			expectSourceUK: nil,
			expectTargetUK: nil,
		},
		{
			name: "single no shared",
			sourceUKs: []*vrepl.UniqueKey{
				{Name: "uidx", Columns: *columns1},
			},
			targetUKs: []*vrepl.UniqueKey{
				{Name: "uidx", Columns: *columns12},
			},
			renameMap:      map[string]string{},
			expectSourceUK: nil,
			expectTargetUK: nil,
		},
		{
			name: "single no shared different order",
			sourceUKs: []*vrepl.UniqueKey{
				{Name: "uidx", Columns: *columns12},
			},
			targetUKs: []*vrepl.UniqueKey{
				{Name: "uidx", Columns: *columns21},
			},
			renameMap:      map[string]string{},
			expectSourceUK: nil,
			expectTargetUK: nil,
		},
		{
			name: "exact match",
			sourceUKs: []*vrepl.UniqueKey{
				{Name: "uidx", Columns: *columns1},
				{Name: "uidx123", Columns: *columns123},
			},
			targetUKs: []*vrepl.UniqueKey{
				{Name: "uidx", Columns: *columns12},
				{Name: "uidx123", Columns: *columns123},
			},
			renameMap:      map[string]string{},
			expectSourceUK: &vrepl.UniqueKey{Name: "uidx123", Columns: *columns123},
			expectTargetUK: &vrepl.UniqueKey{Name: "uidx123", Columns: *columns123},
		},
		{
			name: "exact match from multiple options",
			sourceUKs: []*vrepl.UniqueKey{
				{Name: "uidx", Columns: *columns1},
				{Name: "uidx123", Columns: *columns123},
				{Name: "uidx12", Columns: *columns12},
			},
			targetUKs: []*vrepl.UniqueKey{
				{Name: "uidx12", Columns: *columns12},
				{Name: "uidx", Columns: *columns12},
				{Name: "uidx123", Columns: *columns123},
			},
			renameMap:      map[string]string{},
			expectSourceUK: &vrepl.UniqueKey{Name: "uidx123", Columns: *columns123},
			expectTargetUK: &vrepl.UniqueKey{Name: "uidx123", Columns: *columns123},
		},
		{
			name: "exact match from multiple options reorder",
			sourceUKs: []*vrepl.UniqueKey{
				{Name: "uidx12", Columns: *columns12},
				{Name: "uidx", Columns: *columns1},
				{Name: "uidx123", Columns: *columns123},
			},
			targetUKs: []*vrepl.UniqueKey{
				{Name: "uidx", Columns: *columns21},
				{Name: "uidx123", Columns: *columns123},
				{Name: "uidx12", Columns: *columns12},
			},
			renameMap:      map[string]string{},
			expectSourceUK: &vrepl.UniqueKey{Name: "uidx12", Columns: *columns12},
			expectTargetUK: &vrepl.UniqueKey{Name: "uidx12", Columns: *columns12},
		},
		{
			name: "match different names",
			sourceUKs: []*vrepl.UniqueKey{
				{Name: "uidx1", Columns: *columns1},
				{Name: "uidx12", Columns: *columns12},
				{Name: "uidx123", Columns: *columns123},
			},
			targetUKs: []*vrepl.UniqueKey{
				{Name: "uidx21", Columns: *columns21},
				{Name: "uidx123", Columns: *columns123},
				{Name: "uidxother", Columns: *columns12},
			},
			renameMap:      map[string]string{},
			expectSourceUK: &vrepl.UniqueKey{Name: "uidx12", Columns: *columns12},
			expectTargetUK: &vrepl.UniqueKey{Name: "uidxother", Columns: *columns12},
		},
		{
			name: "match different names, nullable",
			sourceUKs: []*vrepl.UniqueKey{
				{Name: "uidx1", Columns: *columns1},
				{Name: "uidx12", Columns: *columns12},
				{Name: "uidx123", Columns: *columns123},
			},
			targetUKs: []*vrepl.UniqueKey{
				{Name: "uidx21", Columns: *columns21},
				{Name: "uidx123other", Columns: *columns123},
				{Name: "uidx12", Columns: *columns12, HasNullable: true},
			},
			renameMap:      map[string]string{},
			expectSourceUK: &vrepl.UniqueKey{Name: "uidx123", Columns: *columns123},
			expectTargetUK: &vrepl.UniqueKey{Name: "uidx123other", Columns: *columns123},
		},
		{
			name: "match different column names",
			sourceUKs: []*vrepl.UniqueKey{
				{Name: "uidx1", Columns: *columns1},
				{Name: "uidx12", Columns: *columns12},
				{Name: "uidx123", Columns: *columns123},
			},
			targetUKs: []*vrepl.UniqueKey{
				{Name: "uidx21", Columns: *columns21},
				{Name: "uidx12A", Columns: *columns12A},
			},
			renameMap:      map[string]string{"c3": "ca"},
			expectSourceUK: &vrepl.UniqueKey{Name: "uidx123", Columns: *columns123},
			expectTargetUK: &vrepl.UniqueKey{Name: "uidx12A", Columns: *columns12A},
		},
		{
			// enforce mapping from c3 to ca; will not match c3<->c3
			name: "no match identical column names",
			sourceUKs: []*vrepl.UniqueKey{
				{Name: "uidx1", Columns: *columns1},
				{Name: "uidx12", Columns: *columns12},
				{Name: "uidx123", Columns: *columns123},
			},
			targetUKs: []*vrepl.UniqueKey{
				{Name: "uidx21", Columns: *columns21},
				{Name: "uidx123", Columns: *columns123},
			},
			renameMap:      map[string]string{"c3": "ca"},
			expectSourceUK: nil,
			expectTargetUK: nil,
		},
		{
			name: "no match different column names",
			sourceUKs: []*vrepl.UniqueKey{
				{Name: "uidx1", Columns: *columns1},
				{Name: "uidx12", Columns: *columns12},
				{Name: "uidx123", Columns: *columns123},
			},
			targetUKs: []*vrepl.UniqueKey{
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
			sourceUK, targetUK := getSharedUniqueKeys(tc.sourceUKs, tc.targetUKs, tc.renameMap)
			assert.Equal(t, tc.expectSourceUK, sourceUK)
			assert.Equal(t, tc.expectTargetUK, targetUK)
		})
	}
}

func TestAddedUniqueKeys(t *testing.T) {
	emptyUniqueKeys := []*vrepl.UniqueKey{}
	tt := []struct {
		name                 string
		sourceUKs, targetUKs [](*vrepl.UniqueKey)
		renameMap            map[string]string
		expectAddedUKs       [](*vrepl.UniqueKey)
		expectRemovedUKs     [](*vrepl.UniqueKey)
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
			sourceUKs: []*vrepl.UniqueKey{
				{Name: "PRIMARY", Columns: *columns1},
			},
			targetUKs:      emptyUniqueKeys,
			renameMap:      map[string]string{},
			expectAddedUKs: emptyUniqueKeys,
			expectRemovedUKs: []*vrepl.UniqueKey{
				{Name: "PRIMARY", Columns: *columns1},
			},
		},
		{
			name:      "UK added",
			sourceUKs: emptyUniqueKeys,
			targetUKs: []*vrepl.UniqueKey{
				{Name: "PRIMARY", Columns: *columns1},
			},
			renameMap: map[string]string{},
			expectAddedUKs: []*vrepl.UniqueKey{
				{Name: "PRIMARY", Columns: *columns1},
			},
			expectRemovedUKs: emptyUniqueKeys,
		},
		{
			name: "single identical",
			sourceUKs: []*vrepl.UniqueKey{
				{Name: "PRIMARY", Columns: *columns1},
			},
			targetUKs: []*vrepl.UniqueKey{
				{Name: "PRIMARY", Columns: *columns1},
			},
			renameMap:        map[string]string{},
			expectAddedUKs:   emptyUniqueKeys,
			expectRemovedUKs: emptyUniqueKeys,
		},
		{
			name: "single identical non pk",
			sourceUKs: []*vrepl.UniqueKey{
				{Name: "uidx", Columns: *columns1},
			},
			targetUKs: []*vrepl.UniqueKey{
				{Name: "uidx", Columns: *columns1},
			},
			renameMap:        map[string]string{},
			expectAddedUKs:   emptyUniqueKeys,
			expectRemovedUKs: emptyUniqueKeys,
		},
		{
			name: "single identical, source is nullable",
			sourceUKs: []*vrepl.UniqueKey{
				{Name: "uidx", Columns: *columns1, HasNullable: true},
			},
			targetUKs: []*vrepl.UniqueKey{
				{Name: "uidx", Columns: *columns1},
			},
			renameMap:        map[string]string{},
			expectAddedUKs:   emptyUniqueKeys,
			expectRemovedUKs: emptyUniqueKeys,
		},
		{
			name: "single identical, target is nullable",
			sourceUKs: []*vrepl.UniqueKey{
				{Name: "uidx", Columns: *columns1},
			},
			targetUKs: []*vrepl.UniqueKey{
				{Name: "uidx", Columns: *columns1, HasNullable: true},
			},
			renameMap:        map[string]string{},
			expectAddedUKs:   emptyUniqueKeys,
			expectRemovedUKs: emptyUniqueKeys,
		},
		{
			name: "expand columns: not considered added",
			sourceUKs: []*vrepl.UniqueKey{
				{Name: "uidx", Columns: *columns1},
			},
			targetUKs: []*vrepl.UniqueKey{
				{Name: "uidx", Columns: *columns12},
			},
			renameMap:      map[string]string{},
			expectAddedUKs: emptyUniqueKeys,
			expectRemovedUKs: []*vrepl.UniqueKey{
				{Name: "uidx", Columns: *columns1},
			},
		},
		{
			name: "expand columns, different order: not considered added",
			sourceUKs: []*vrepl.UniqueKey{
				{Name: "uidx", Columns: *columns1},
			},
			targetUKs: []*vrepl.UniqueKey{
				{Name: "uidx", Columns: *columns21},
			},
			renameMap:      map[string]string{},
			expectAddedUKs: emptyUniqueKeys,
			expectRemovedUKs: []*vrepl.UniqueKey{
				{Name: "uidx", Columns: *columns1},
			},
		},
		{
			name: "reduced columns: considered added",
			sourceUKs: []*vrepl.UniqueKey{
				{Name: "uidx", Columns: *columns12},
			},
			targetUKs: []*vrepl.UniqueKey{
				{Name: "uidx", Columns: *columns1},
			},
			renameMap: map[string]string{},
			expectAddedUKs: []*vrepl.UniqueKey{
				{Name: "uidx", Columns: *columns1},
			},
			expectRemovedUKs: emptyUniqueKeys,
		},
		{
			name: "reduced columns, multiple: considered added",
			sourceUKs: []*vrepl.UniqueKey{
				{Name: "uidx12", Columns: *columns12},
				{Name: "uidx123", Columns: *columns123},
				{Name: "uidx2", Columns: *columns2},
			},
			targetUKs: []*vrepl.UniqueKey{
				{Name: "uidx", Columns: *columns1},
			},
			renameMap: map[string]string{},
			expectAddedUKs: []*vrepl.UniqueKey{
				{Name: "uidx", Columns: *columns1},
			},
			expectRemovedUKs: []*vrepl.UniqueKey{
				{Name: "uidx2", Columns: *columns2},
			},
		},
		{
			name: "different order: not considered added",
			sourceUKs: []*vrepl.UniqueKey{
				{Name: "uidx", Columns: *columns12},
			},
			targetUKs: []*vrepl.UniqueKey{
				{Name: "uidx", Columns: *columns21},
			},
			renameMap:        map[string]string{},
			expectAddedUKs:   emptyUniqueKeys,
			expectRemovedUKs: emptyUniqueKeys,
		},
		{
			name: "no match, different columns",
			sourceUKs: []*vrepl.UniqueKey{
				{Name: "uidx1", Columns: *columns1},
			},
			targetUKs: []*vrepl.UniqueKey{
				{Name: "uidx2", Columns: *columns2},
			},
			renameMap: map[string]string{},
			expectAddedUKs: []*vrepl.UniqueKey{
				{Name: "uidx2", Columns: *columns2},
			},
			expectRemovedUKs: []*vrepl.UniqueKey{
				{Name: "uidx1", Columns: *columns1},
			},
		},
		{
			name: "one match, one expand",
			sourceUKs: []*vrepl.UniqueKey{
				{Name: "uidx", Columns: *columns1},
				{Name: "uidx123", Columns: *columns123},
			},
			targetUKs: []*vrepl.UniqueKey{
				{Name: "uidx", Columns: *columns12},
				{Name: "uidx123", Columns: *columns123},
			},
			renameMap:      map[string]string{},
			expectAddedUKs: emptyUniqueKeys,
			expectRemovedUKs: []*vrepl.UniqueKey{
				{Name: "uidx", Columns: *columns1},
			},
		},
		{
			name: "exact match from multiple options",
			sourceUKs: []*vrepl.UniqueKey{
				{Name: "uidx", Columns: *columns1},
				{Name: "uidx123", Columns: *columns123},
				{Name: "uidx12", Columns: *columns12},
			},
			targetUKs: []*vrepl.UniqueKey{
				{Name: "uidx12", Columns: *columns12},
				{Name: "uidx", Columns: *columns12},
				{Name: "uidx123", Columns: *columns123},
			},
			renameMap:      map[string]string{},
			expectAddedUKs: emptyUniqueKeys,
			expectRemovedUKs: []*vrepl.UniqueKey{
				{Name: "uidx", Columns: *columns1},
			},
		},
		{
			name: "exact match from multiple options reorder",
			sourceUKs: []*vrepl.UniqueKey{
				{Name: "uidx12", Columns: *columns12},
				{Name: "uidx", Columns: *columns1},
				{Name: "uidx123", Columns: *columns123},
			},
			targetUKs: []*vrepl.UniqueKey{
				{Name: "uidx", Columns: *columns21},
				{Name: "uidx123", Columns: *columns123},
				{Name: "uidx12", Columns: *columns12},
			},
			renameMap:      map[string]string{},
			expectAddedUKs: emptyUniqueKeys,
			expectRemovedUKs: []*vrepl.UniqueKey{
				{Name: "uidx", Columns: *columns1},
			},
		},
		{
			name: "match different names",
			sourceUKs: []*vrepl.UniqueKey{
				{Name: "uidx1", Columns: *columns1},
				{Name: "uidx12", Columns: *columns12},
				{Name: "uidx123", Columns: *columns123},
			},
			targetUKs: []*vrepl.UniqueKey{
				{Name: "uidx21", Columns: *columns21},
				{Name: "uidx123", Columns: *columns123},
				{Name: "uidxother", Columns: *columns12},
			},
			renameMap:      map[string]string{},
			expectAddedUKs: emptyUniqueKeys,
			expectRemovedUKs: []*vrepl.UniqueKey{
				{Name: "uidx1", Columns: *columns1},
			},
		},
		{
			name: "match different names, nullable",
			sourceUKs: []*vrepl.UniqueKey{
				{Name: "uidx1", Columns: *columns1},
				{Name: "uidx12", Columns: *columns12},
				{Name: "uidx123", Columns: *columns123},
			},
			targetUKs: []*vrepl.UniqueKey{
				{Name: "uidx21", Columns: *columns21},
				{Name: "uidx123other", Columns: *columns123},
				{Name: "uidx12", Columns: *columns12, HasNullable: true},
			},
			renameMap:      map[string]string{},
			expectAddedUKs: emptyUniqueKeys,
			expectRemovedUKs: []*vrepl.UniqueKey{
				{Name: "uidx1", Columns: *columns1},
			},
		},
		{
			name: "match different column names, expand",
			sourceUKs: []*vrepl.UniqueKey{
				{Name: "uidx1", Columns: *columns1},
				{Name: "uidx12", Columns: *columns12},
				{Name: "uidx123", Columns: *columns123},
			},
			targetUKs: []*vrepl.UniqueKey{
				{Name: "uidx21", Columns: *columns21},
				{Name: "uidx12A", Columns: *columns12A},
			},
			renameMap:      map[string]string{"c3": "ca"},
			expectAddedUKs: emptyUniqueKeys,
			expectRemovedUKs: []*vrepl.UniqueKey{
				{Name: "uidx1", Columns: *columns1},
			},
		},
		{
			name: "match different column names, no expand",
			sourceUKs: []*vrepl.UniqueKey{
				{Name: "uidx123", Columns: *columns123},
			},
			targetUKs: []*vrepl.UniqueKey{
				{Name: "uidx12A", Columns: *columns12A},
			},
			renameMap:        map[string]string{"c3": "ca"},
			expectAddedUKs:   emptyUniqueKeys,
			expectRemovedUKs: emptyUniqueKeys,
		},
		{
			// enforce mapping from c3 to ca; will not match c3<->c3
			name: "no match identical column names, expand",
			sourceUKs: []*vrepl.UniqueKey{
				{Name: "uidx1", Columns: *columns1},
				{Name: "uidx12", Columns: *columns12},
				{Name: "uidx123", Columns: *columns123},
			},
			targetUKs: []*vrepl.UniqueKey{
				{Name: "uidx21", Columns: *columns21},
				{Name: "uidx123", Columns: *columns123},
			},
			renameMap: map[string]string{"c3": "ca"},
			// 123 expands 12, so even though 3 is mapped to A, 123 is still not more constrained.
			expectAddedUKs: emptyUniqueKeys,
			expectRemovedUKs: []*vrepl.UniqueKey{
				{Name: "uidx1", Columns: *columns1},
			},
		},
		{
			// enforce mapping from c3 to ca; will not match c3<->c3
			name: "no match identical column names, no expand",
			sourceUKs: []*vrepl.UniqueKey{
				{Name: "uidx123", Columns: *columns123},
			},
			targetUKs: []*vrepl.UniqueKey{
				{Name: "uidx123", Columns: *columns123},
			},
			renameMap: map[string]string{"c3": "ca"},
			expectAddedUKs: []*vrepl.UniqueKey{
				{Name: "uidx123", Columns: *columns123},
			},
			expectRemovedUKs: emptyUniqueKeys,
		},
		{
			name: "no match for different column names, expand",
			sourceUKs: []*vrepl.UniqueKey{
				{Name: "uidx1", Columns: *columns1},
				{Name: "uidx12", Columns: *columns12},
				{Name: "uidx123", Columns: *columns123},
			},
			targetUKs: []*vrepl.UniqueKey{
				{Name: "uidx21", Columns: *columns21},
				{Name: "uidx12A", Columns: *columns12A},
			},
			renameMap: map[string]string{"c3": "cx"},
			// 123 expands 12, so even though 3 is mapped to x, 123 is still not more constrained.
			expectAddedUKs: emptyUniqueKeys,
			expectRemovedUKs: []*vrepl.UniqueKey{
				{Name: "uidx1", Columns: *columns1},
			},
		},
		{
			name: "no match for different column names, no expand",
			sourceUKs: []*vrepl.UniqueKey{
				{Name: "uidx123", Columns: *columns123},
			},
			targetUKs: []*vrepl.UniqueKey{
				{Name: "uidx12A", Columns: *columns12A},
			},
			renameMap: map[string]string{"c3": "cx"},
			expectAddedUKs: []*vrepl.UniqueKey{
				{Name: "uidx12A", Columns: *columns12A},
			},
			expectRemovedUKs: []*vrepl.UniqueKey{
				{Name: "uidx123", Columns: *columns123},
			},
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			addedUKs := addedUniqueKeys(tc.sourceUKs, tc.targetUKs, tc.renameMap)
			assert.Equal(t, tc.expectAddedUKs, addedUKs)
			removedUKs := removedUniqueKeys(tc.sourceUKs, tc.targetUKs, tc.renameMap)
			assert.Equal(t, tc.expectRemovedUKs, removedUKs)
		})
	}
}
