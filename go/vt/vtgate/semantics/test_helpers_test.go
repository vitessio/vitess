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

package semantics

import (
	"testing"

	"github.com/stretchr/testify/require"

	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func parseAndAnalyze(t *testing.T, query string, dbName string) (sqlparser.Statement, *SemTable) {
	t.Helper()
	parse, semTable, err := parseAndAnalyzeAllowErr(t, query, dbName)
	require.NoError(t, err)
	return parse, semTable
}

func parseAndAnalyzeAllowErr(t *testing.T, query string, dbName string) (sqlparser.Statement, *SemTable, error) {
	t.Helper()
	parse, err := sqlparser.NewTestParser().Parse(query)
	require.NoError(t, err)

	semTable, err := Analyze(parse, dbName, fakeSchemaInfo(), nil)
	return parse, semTable, err
}

func parseAndAnalyzeStrict(t *testing.T, query, dbName string) (sqlparser.Statement, *SemTable) {
	t.Helper()
	parse, semTable, err := parseAndAnalyzeStrictAllowErr(t, query, dbName)
	require.NoError(t, err)
	return parse, semTable
}

func parseAndAnalyzeStrictAllowErr(t *testing.T, query, dbName string) (sqlparser.Statement, *SemTable, error) {
	t.Helper()
	parse, err := sqlparser.NewTestParser().Parse(query)
	require.NoError(t, err)

	semTable, err := AnalyzeStrict(parse, dbName, fakeSchemaInfo())
	return parse, semTable, err
}

func extract(in *sqlparser.Select, idx int) sqlparser.Expr {
	return in.SelectExprs[idx].(*sqlparser.AliasedExpr).Expr
}

var unsharded = &vindexes.Keyspace{
	Name:    "unsharded",
	Sharded: false,
}
var ks2 = &vindexes.Keyspace{
	Name:    "ks2",
	Sharded: true,
}
var ks3 = &vindexes.Keyspace{
	Name:    "ks3",
	Sharded: true,
}

// create table t(<no column info>)
// create table t1(id bigint)
// create table t2(uid bigint, name varchar(255) utf8_bin, textcol varchar(255) big5_bin)
// create table t3(uid bigint, name varchar(255) textcol varchar(255) big5_bin, invcol varchar(255) big5_bin invisible)
func fakeSchemaInfo() *FakeSI {
	si := &FakeSI{
		Tables: map[string]*vindexes.Table{
			"t":  tableT(),
			"t1": tableT1(),
			"t2": tableT2(),
			"t3": tableT3(),
		},
	}
	return si
}
func tableT() *vindexes.Table {
	return &vindexes.Table{
		Name:     sqlparser.NewIdentifierCS("t"),
		Keyspace: unsharded,
	}
}
func tableT1() *vindexes.Table {
	return &vindexes.Table{
		Name: sqlparser.NewIdentifierCS("t1"),
		Columns: []vindexes.Column{{
			Name: sqlparser.NewIdentifierCI("id"),
			Type: querypb.Type_INT64,
		}},
		ColumnListAuthoritative: true,
		ColumnVindexes: []*vindexes.ColumnVindex{
			{Name: "id_vindex"},
		},
		Keyspace: ks2,
	}
}
func tableT2() *vindexes.Table {
	return &vindexes.Table{
		Name: sqlparser.NewIdentifierCS("t2"),
		Columns: []vindexes.Column{{
			Name: sqlparser.NewIdentifierCI("uid"),
			Type: querypb.Type_INT64,
		}, {
			Name:          sqlparser.NewIdentifierCI("name"),
			Type:          querypb.Type_VARCHAR,
			CollationName: "utf8_bin",
		}, {
			Name:          sqlparser.NewIdentifierCI("textcol"),
			Type:          querypb.Type_VARCHAR,
			CollationName: "big5_bin",
		}},
		ColumnListAuthoritative: true,
		Keyspace:                ks3,
	}
}
func tableT3() *vindexes.Table {
	return &vindexes.Table{
		Name: sqlparser.NewIdentifierCS("t3"),
		Columns: []vindexes.Column{{
			Name: sqlparser.NewIdentifierCI("uid"),
			Type: querypb.Type_INT64,
		}, {
			Name:          sqlparser.NewIdentifierCI("name"),
			Type:          querypb.Type_VARCHAR,
			CollationName: "utf8_bin",
		}, {
			Name:          sqlparser.NewIdentifierCI("textcol"),
			Type:          querypb.Type_VARCHAR,
			CollationName: "big5_bin",
		}, {
			Name:          sqlparser.NewIdentifierCI("invcol"),
			Type:          querypb.Type_VARCHAR,
			CollationName: "big5_bin",
			Invisible:     true,
		}},
		ColumnListAuthoritative: true,
		Keyspace:                ks3,
	}
}
