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

package schema

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/vtenv"
)

func TestParseAlterTableOptions(t *testing.T) {
	type expect struct {
		schema, table, options string
	}
	tests := map[string]expect{
		"add column i int, drop column d":                               {schema: "", table: "", options: "add column i int, drop column d"},
		"  add column i int, drop column d  ":                           {schema: "", table: "", options: "add column i int, drop column d"},
		"alter table t add column i int, drop column d":                 {schema: "", table: "t", options: "add column i int, drop column d"},
		"alter    table   t      add column i int, drop column d":       {schema: "", table: "t", options: "add column i int, drop column d"},
		"alter table `t` add column i int, drop column d":               {schema: "", table: "t", options: "add column i int, drop column d"},
		"alter table `scm`.`t` add column i int, drop column d":         {schema: "scm", table: "t", options: "add column i int, drop column d"},
		"alter table `scm`.t add column i int, drop column d":           {schema: "scm", table: "t", options: "add column i int, drop column d"},
		"alter table scm.`t` add column i int, drop column d":           {schema: "scm", table: "t", options: "add column i int, drop column d"},
		"alter table scm.t add column i int, drop column d":             {schema: "scm", table: "t", options: "add column i int, drop column d"},
		"  alter       table   scm.`t` add column i int, drop column d": {schema: "scm", table: "t", options: "add column i int, drop column d"},
		"ALTER  table scm.t ADD COLUMN i int, DROP COLUMN d":            {schema: "scm", table: "t", options: "ADD COLUMN i int, DROP COLUMN d"},
		"ALTER TABLE scm.t ADD COLUMN i int, DROP COLUMN d":             {schema: "scm", table: "t", options: "ADD COLUMN i int, DROP COLUMN d"},
	}
	for query, expect := range tests {
		schema, table, options := ParseAlterTableOptions(query)
		assert.Equal(t, expect.schema, schema)
		assert.Equal(t, expect.table, table)
		assert.Equal(t, expect.options, options)
	}
}

func TestParseEnumTokens(t *testing.T) {
	env := vtenv.NewTestEnv()
	{
		input := `'x-small','small','medium','large','x-large'`
		enumTokens, err := parseEnumOrSetTokens(env, input)
		require.NoError(t, err)
		expect := []string{"x-small", "small", "medium", "large", "x-large"}
		assert.Equal(t, expect, enumTokens)
	}
	{
		input := `'x small','small','medium','large','x large'`
		enumTokens, err := parseEnumOrSetTokens(env, input)
		require.NoError(t, err)
		expect := []string{"x small", "small", "medium", "large", "x large"}
		assert.Equal(t, expect, enumTokens)
	}
	{
		input := `'with '' quote','and \n newline'`
		enumTokens, err := parseEnumOrSetTokens(env, input)
		require.NoError(t, err)
		expect := []string{"with ' quote", "and \n newline"}
		assert.Equal(t, expect, enumTokens)
	}
	{
		input := `enum('x-small','small','medium','large','x-large')`
		enumTokens, err := parseEnumOrSetTokens(env, input)
		assert.Error(t, err)
		assert.Nil(t, enumTokens)
	}
	{
		input := `set('x-small','small','medium','large','x-large')`
		enumTokens, err := parseEnumOrSetTokens(env, input)
		assert.Error(t, err)
		assert.Nil(t, enumTokens)
	}
}

func TestParseEnumTokensMap(t *testing.T) {
	env := vtenv.NewTestEnv()
	{
		input := `'x-small','small','medium','large','x-large'`

		enumTokensMap, err := ParseEnumOrSetTokensMap(env, input)
		require.NoError(t, err)
		expect := map[int]string{
			1: "x-small",
			2: "small",
			3: "medium",
			4: "large",
			5: "x-large",
		}
		assert.Equal(t, expect, enumTokensMap)
	}
	{
		inputs := []string{
			`enum('x-small','small','medium','large','x-large')`,
			`set('x-small','small','medium','large','x-large')`,
		}
		for _, input := range inputs {
			enumTokensMap, err := ParseEnumOrSetTokensMap(env, input)
			assert.Error(t, err)
			assert.Nil(t, enumTokensMap)
		}
	}
	{
		input := `'x-small','small','med''ium','large','x-large'`

		enumTokensMap, err := ParseEnumOrSetTokensMap(env, input)
		require.NoError(t, err)
		expect := map[int]string{
			1: "x-small",
			2: "small",
			3: "med'ium",
			4: "large",
			5: "x-large",
		}
		assert.Equal(t, expect, enumTokensMap)
	}
}
