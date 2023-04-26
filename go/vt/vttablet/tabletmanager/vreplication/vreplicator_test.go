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
	require.NoError(t, err)
	defer testEnv.Close()
	tests := []struct {
		name    string
		table   string
		ddl     string
		want    []string
		wantErr bool
	}{
		{
			name:  "WITHPK",
			table: "withpk_t",
			ddl: `CREATE TABLE withpk_t (pkid INT NOT NULL AUTO_INCREMENT, col1 VARCHAR(25),
				PRIMARY KEY (pkid))`,
			want: []string{"pkid"},
		},
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
			name:  "3MULTICOL1PKE",
			table: "onemcpke_t",
			ddl: `CREATE TABLE onemcpke_t (col1 VARCHAR(25) NOT NULL, col2 VARCHAR(25) NOT NULL,
					col3 VARCHAR(25) NOT NULL, col4 VARCHAR(25), UNIQUE KEY c4_c2_c1 (col4, col2, col1),
					UNIQUE KEY c1_c2 (col1, col2), UNIQUE KEY c1_c2_c4 (col1, col2, col4),
					KEY nc1_nc2 (col1, col2))`,
			want: []string{"col1", "col2"},
		},
		{
			name:  "3MULTICOL2PKE",
			table: "twomcpke_t",
			ddl: `CREATE TABLE twomcpke_t (col1 VARCHAR(25) NOT NULL, col2 VARCHAR(25) NOT NULL,
					col3 VARCHAR(25) NOT NULL, col4 VARCHAR(25), UNIQUE KEY (col4), UNIQUE KEY c4_c2_c1 (col4, col2, col1),
					UNIQUE KEY c1_c2_c3 (col1, col2, col3), UNIQUE KEY c1_c2 (col1, col2))`,
			want: []string{"col1", "col2"},
		},
		{
			name:  "1INTPKE1CHARPKE",
			table: "oneintpke1charpke_t",
			ddl: `CREATE TABLE oneintpke1charpke_t (col1 VARCHAR(25) NOT NULL, col2 VARCHAR(25) NOT NULL,
					col3 VARCHAR(25) NOT NULL, id1 INT NOT NULL, id2 INT NOT NULL, 
					UNIQUE KEY c1_c2 (col1, col2), UNIQUE KEY id1_id2 (id1, id2))`,
			want: []string{"id1", "id2"},
		},
		{
			name:  "INTINTVSVCHAR",
			table: "twointvsvcharpke_t",
			ddl: `CREATE TABLE twointvsvcharpke_t (col1 VARCHAR(25) NOT NULL, id1 INT NOT NULL, id2 INT NOT NULL, 
					UNIQUE KEY c1 (col1), UNIQUE KEY id1_id2 (id1, id2))`,
			want: []string{"id1", "id2"},
		},
		{
			name:  "TINYINTVSBIGINT",
			table: "tinyintvsbigint_t",
			ddl: `CREATE TABLE tinyintvsbigint_t (tid1 TINYINT NOT NULL, id1 INT NOT NULL, 
					UNIQUE KEY tid1 (tid1), UNIQUE KEY id1 (id1))`,
			want: []string{"tid1"},
		},
		{
			name:  "VCHARINTVSINT2VARCHAR",
			table: "vcharintvsinttwovchar_t",
			ddl: `CREATE TABLE vcharintvsinttwovchar_t (id1 INT NOT NULL, col1 VARCHAR(25) NOT NULL, col2 VARCHAR(25) NOT NULL,
					UNIQUE KEY col1_id1 (col1, id1), UNIQUE KEY id1_col1_col2 (id1, col1, col2))`,
			want: []string{"col1", "id1"},
		},
		{
			name:  "VCHARVSINT3",
			table: "vcharvsintthree_t",
			ddl: `CREATE TABLE vcharvsintthree_t (id1 INT NOT NULL, id2 INT NOT NULL, id3 INT NOT NULL, col1 VARCHAR(50) NOT NULL,
					UNIQUE KEY col1 (col1), UNIQUE KEY id1_id2_id3 (id1, id2, id3))`,
			want: []string{"id1", "id2", "id3"},
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
