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

package planbuilder

import (
	"context"
	"fmt"
	"testing"

	"vitess.io/vitess/go/test/vschemawrapper"
	"vitess.io/vitess/go/vt/vtenv"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func TestBuildDBPlan(t *testing.T) {
	vschema := &vschemawrapper.VSchemaWrapper{
		Keyspace: &vindexes.Keyspace{Name: "main"},
		Env:      vtenv.NewTestEnv(),
	}

	testCases := []struct {
		query    string
		expected string
	}{{
		query:    "show databases like 'main'",
		expected: `[[VARCHAR("main")]]`,
	}, {
		query:    "show databases like '%ys%'",
		expected: `[[VARCHAR("mysql")] [VARCHAR("sys")]]`,
	}}

	for _, s := range testCases {
		t.Run(s.query, func(t *testing.T) {
			parserOut, err := sqlparser.NewTestParser().Parse(s.query)
			require.NoError(t, err)

			show := parserOut.(*sqlparser.Show)
			primitive, err := buildDBPlan(show.Internal.(*sqlparser.ShowBasic), vschema)
			require.NoError(t, err)

			result, err := primitive.TryExecute(context.Background(), nil, nil, false)
			require.NoError(t, err)
			require.Equal(t, s.expected, fmt.Sprintf("%v", result.Rows))
		})
	}
}

func TestGenerateCharsetRows(t *testing.T) {
	rows0 := [][]sqltypes.Value{
		append(buildVarCharRow(
			"utf8mb3",
			"UTF-8 Unicode",
			"utf8mb3_general_ci"),
			sqltypes.NewUint32(3)),
	}
	rows1 := [][]sqltypes.Value{
		append(buildVarCharRow(
			"utf8mb4",
			"UTF-8 Unicode",
			collations.MySQL8().LookupName(collations.MySQL8().DefaultConnectionCharset())),
			sqltypes.NewUint32(4)),
	}
	rows2 := [][]sqltypes.Value{
		append(buildVarCharRow(
			"utf8mb3",
			"UTF-8 Unicode",
			"utf8mb3_general_ci"),
			sqltypes.NewUint32(3)),
		append(buildVarCharRow(
			"utf8mb4",
			"UTF-8 Unicode",
			collations.MySQL8().LookupName(collations.MySQL8().DefaultConnectionCharset())),
			sqltypes.NewUint32(4)),
	}

	testcases := []struct {
		input    string
		expected [][]sqltypes.Value
	}{
		{input: "show charset", expected: charsets()},
		{input: "show character set", expected: charsets()},
		{input: "show charset where charset like 'foo%'", expected: nil},
		{input: "show charset where charset like 'utf8%'", expected: rows2},
		{input: "show charset where charset like 'utf8mb3%'", expected: rows0},
		{input: "show charset where charset like 'foo%'", expected: nil},
		{input: "show character set where charset like '%foo'", expected: nil},
		{input: "show charset where charset = 'utf8mb3'", expected: rows0},
		{input: "show charset where charset = 'foo%'", expected: nil},
		{input: "show charset where charset = 'utf8mb4'", expected: rows1},
	}

	for _, tc := range testcases {
		t.Run(tc.input, func(t *testing.T) {
			stmt, err := sqlparser.NewTestParser().Parse(tc.input)
			require.NoError(t, err)
			match := stmt.(*sqlparser.Show).Internal.(*sqlparser.ShowBasic)
			filter := match.Filter
			actual, err := generateCharsetRows(filter)
			require.NoError(t, err)
			require.Equal(t, tc.expected, actual)
		})
	}
}
