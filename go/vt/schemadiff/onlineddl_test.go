/*
Copyright 2022 The Vitess Authors.

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

func TestUniqueKeysForOnlineDDL(t *testing.T) {
	table := `
	create table t (
		idsha varchar(64),
		col1 int,
		col2 int not null,
		col3 bigint not null default 3,
		col4 smallint not null,
		f float not null,
		v varchar(32) not null,
		primary key (idsha),
		unique key ukidsha (idsha),
		unique key uk1 (col1),
		unique key uk2 (col2),
		unique key uk3 (col3),
		key k1 (col1),
		key kf (f),
		key k1f (col1, f),
		key kv (v),
		unique key ukv (v),
		unique key ukvprefix (v(10)),
		unique key uk2vprefix (col2, v(10)),
		unique key uk1f (col1, f),
		unique key uk41 (col4, col1),
		unique key uk42 (col4, col2)
	)`
	env := NewTestEnv()
	createTableEntity, err := NewCreateTableEntityFromSQL(env, table)
	require.NoError(t, err)
	err = createTableEntity.validate()
	require.NoError(t, err)

	keys := UniqueKeysForOnlineDDL(createTableEntity)
	require.NotEmpty(t, keys)
	names := make([]string, 0, len(keys))
	for _, key := range keys {
		names = append(names, key.Name())
	}
	expect := []string{
		"PRIMARY",
		"uk42",
		"uk2",
		"uk3",
		"ukidsha",
		"ukv",
		"uk41",
		"uk1",
	}
	assert.Equal(t, expect, names)
}
