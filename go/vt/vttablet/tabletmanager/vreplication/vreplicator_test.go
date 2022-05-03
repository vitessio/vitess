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
	"context"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/vttablet/tabletserver/vstreamer/testenv"
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

func TestPrimaryKeyEquivalentColumns(t *testing.T) {
	ctx := context.Background()
	testEnv, err := testenv.Init()
	defer testEnv.Close()
	require.NoError(t, err)
	tests := []struct {
		name    string
		table   string
		ddl     string
		want    []string
		wantErr bool
	}{
		{
			name:  "0PKE",
			table: "zeropke_t",
			ddl:   `CREATE TABLE zeropke_t (id INT NULL, col1 VARCHAR(25), UNIQUE KEY (id))`,
			want:  []string{},
		},
		{
			name:  "1PKE",
			table: "onepke_t",
			ddl:   `CREATE TABLE onepke_t (id INT NOT NULL, col1 VARCHAR(25), UNIQUE KEY (id))`,
			want:  []string{"id"},
		},
		{
			name:  "2PKE",
			table: "twopke_t",
			ddl:   `CREATE TABLE twopke_t (id INT NOT NULL, col1 VARCHAR(25), col2 VARCHAR(25) NOT NULL, id2 INT NOT NULL, UNIQUE KEY (id), UNIQUE KEY (col2, id2))`,
			want:  []string{"id", "col2", "id2"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.NoError(t, testEnv.Mysqld.ExecuteSuperQuery(ctx, tt.ddl))
			got, err := testEnv.Mysqld.GetPrimaryKeyEquivalentColumns(ctx, testEnv.Dbcfgs.DBName, tt.table)
			if (err != nil) != tt.wantErr {
				t.Errorf("Mysqld.GetPrimaryKeyEquivalentColumns() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Mysqld.GetPrimaryKeyEquivalentColumns() = %v, want %v", got, tt.want)
			}
		})
	}
}
