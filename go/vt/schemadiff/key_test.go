/*
Copyright 2024 The Vitess Authors.

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

package schemadiff

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIndexDefinitionEntityMap(t *testing.T) {
	table := `
	create table t (
		id int,
		col1 int,
		Col2 int not null,
		col3 int not null default 3,
		f float not null,
		v varchar(32),
		primary key (id),
		unique key ukid (id),
		unique key uk1 (col1),
		unique key uk2 (Col2),
		unique key uk3 (col3),
		key k1 (col1),
		key k2 (Col2),
		key k3 (col3),
		key kf (f),
		key kf2 (f, Col2),
		key kv (v),
		key kv1 (v, col1),
		key kv2 (v(10), Col2),
		unique key uk12 (col1, Col2),
		unique key uk21 (col2, Col1),
		unique key uk23 (col2, col3),
		unique key ukid3 (id, col3)
	)`
	tcases := []struct {
		key      string
		unique   bool
		columns  []string
		nullable bool
		float    bool
		prefix   bool
	}{
		{
			key:      "primary",
			unique:   true,
			columns:  []string{"id"},
			nullable: false,
		},
		{
			key:      "ukid",
			unique:   true,
			columns:  []string{"id"},
			nullable: false,
		},
		{
			key:      "uk1",
			unique:   true,
			columns:  []string{"col1"},
			nullable: true,
		},
		{
			key:      "uk2",
			unique:   true,
			columns:  []string{"Col2"},
			nullable: false,
		},
		{
			key:      "uk3",
			unique:   true,
			columns:  []string{"col3"},
			nullable: false,
		},
		{
			key:      "k1",
			unique:   false,
			columns:  []string{"col1"},
			nullable: true,
		},
		{
			key:      "k2",
			unique:   false,
			columns:  []string{"Col2"},
			nullable: false,
		},
		{
			key:      "k3",
			unique:   false,
			columns:  []string{"col3"},
			nullable: false,
		},
		{
			key:      "kf",
			unique:   false,
			columns:  []string{"f"},
			nullable: false,
			float:    true,
		},
		{
			key:      "kf2",
			unique:   false,
			columns:  []string{"f", "Col2"},
			nullable: false,
			float:    true,
		},
		{
			key:      "kv",
			unique:   false,
			columns:  []string{"v"},
			nullable: true,
		},
		{
			key:      "kv1",
			unique:   false,
			columns:  []string{"v", "col1"},
			nullable: true,
		},
		{
			key:      "kv2",
			unique:   false,
			columns:  []string{"v", "Col2"},
			nullable: true,
			prefix:   true,
		},
		{
			key:      "uk12",
			unique:   true,
			columns:  []string{"col1", "Col2"},
			nullable: true,
		},
		{
			key:      "uk21",
			unique:   true,
			columns:  []string{"col2", "Col1"},
			nullable: true,
		},
		{
			key:      "uk23",
			unique:   true,
			columns:  []string{"col2", "col3"},
			nullable: false,
		},
		{
			key:      "ukid3",
			unique:   true,
			columns:  []string{"id", "col3"},
			nullable: false,
		},
	}
	env := NewTestEnv()
	createTableEntity, err := NewCreateTableEntityFromSQL(env, table)
	require.NoError(t, err)
	err = createTableEntity.validate()
	require.NoError(t, err)
	m := createTableEntity.IndexDefinitionEntitiesMap()
	require.NotEmpty(t, m)
	for _, tcase := range tcases {
		t.Run(tcase.key, func(t *testing.T) {
			key := m[tcase.key]
			require.NotNil(t, key)
			assert.Equal(t, tcase.unique, key.IsUnique())
			assert.Equal(t, tcase.columns, key.ColumnNames())
			assert.Equal(t, tcase.nullable, key.HasNullable())
			assert.Equal(t, tcase.float, key.HasFloat())
			assert.Equal(t, tcase.prefix, key.HasColumnPrefix())
		})
	}
}
