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

package vreplication

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRecalculatePKColsInfoByColumnNames(t *testing.T) {
	tt := []struct {
		name             string
		colNames         []string
		colInfos         []*ColumnInfo
		expectPKColInfos []*ColumnInfo
	}{
		{
			name:             "trivial, single column",
			colNames:         []string{"c1"},
			colInfos:         []*ColumnInfo{{Name: "c1", IsPK: true}},
			expectPKColInfos: []*ColumnInfo{{Name: "c1", IsPK: true}},
		},
		{
			name:             "trivial, multiple columns",
			colNames:         []string{"c1"},
			colInfos:         []*ColumnInfo{{Name: "c1", IsPK: true}, {Name: "c2", IsPK: false}, {Name: "c3", IsPK: false}},
			expectPKColInfos: []*ColumnInfo{{Name: "c1", IsPK: true}, {Name: "c2", IsPK: false}, {Name: "c3", IsPK: false}},
		},
		{
			name:             "last column, multiple columns",
			colNames:         []string{"c3"},
			colInfos:         []*ColumnInfo{{Name: "c1", IsPK: false}, {Name: "c2", IsPK: false}, {Name: "c3", IsPK: true}},
			expectPKColInfos: []*ColumnInfo{{Name: "c3", IsPK: true}, {Name: "c1", IsPK: false}, {Name: "c2", IsPK: false}},
		},
		{
			name:             "change of key, single column",
			colNames:         []string{"c2"},
			colInfos:         []*ColumnInfo{{Name: "c1", IsPK: false}, {Name: "c2", IsPK: false}, {Name: "c3", IsPK: true}},
			expectPKColInfos: []*ColumnInfo{{Name: "c2", IsPK: true}, {Name: "c1", IsPK: false}, {Name: "c3", IsPK: false}},
		},
		{
			name:             "change of key, multiple columns",
			colNames:         []string{"c2", "c3"},
			colInfos:         []*ColumnInfo{{Name: "c1", IsPK: false}, {Name: "c2", IsPK: false}, {Name: "c3", IsPK: true}},
			expectPKColInfos: []*ColumnInfo{{Name: "c2", IsPK: true}, {Name: "c3", IsPK: true}, {Name: "c1", IsPK: false}},
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			pkColInfos := recalculatePKColsInfoByColumnNames(tc.colNames, tc.colInfos)
			assert.Equal(t, tc.expectPKColInfos, pkColInfos)
		})
	}
}
