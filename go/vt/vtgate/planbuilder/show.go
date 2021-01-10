/*
Copyright 2020 The Vitess Authors.

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
	"regexp"
	"strings"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

const (
	utf8    = "utf8"
	utf8mb4 = "utf8mb4"
	both    = "both"
	charset = "charset"
)

func buildShowPlan(stmt *sqlparser.Show, vschema ContextVSchema) (engine.Primitive, error) {
	switch show := stmt.Internal.(type) {
	case *sqlparser.ShowBasic:
		return buildShowBasicPlan(show, vschema)
	case *sqlparser.ShowColumns:
		return buildShowColumnsPlan(show, vschema)
	case *sqlparser.ShowTableStatus:
		return buildShowTableStatusPlan(show, vschema)
	default:
		return nil, ErrPlanNotSupported
	}
}

func buildShowBasicPlan(show *sqlparser.ShowBasic, vschema ContextVSchema) (engine.Primitive, error) {
	switch show.Command {
	case sqlparser.Charset:
		return showCharset(show)
	case sqlparser.Collation, sqlparser.Function, sqlparser.Privilege, sqlparser.Procedure,
		sqlparser.VariableGlobal, sqlparser.VariableSession:
		return showSendAnywhere(show, vschema)
	case sqlparser.Database:
		ks, err := vschema.AllKeyspace()
		if err != nil {
			return nil, err
		}

		var filter *regexp.Regexp

		if show.Filter != nil {
			filter = sqlparser.LikeToRegexp(show.Filter.Like)
		}

		if filter == nil {
			filter = regexp.MustCompile(".*")
		}

		rows := make([][]sqltypes.Value, 0, len(ks))
		for _, v := range ks {
			if filter.MatchString(v.Name) {
				rows = append(rows, buildVarCharRow(v.Name))
			}
		}
		return engine.NewRowsPrimitive(rows, buildVarCharFields("Database")), nil
	case sqlparser.StatusGlobal, sqlparser.StatusSession:
		return engine.NewRowsPrimitive(make([][]sqltypes.Value, 0, 2), buildVarCharFields("Variable_name", "Value")), nil
	}
	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "BUG: unknown show query type %s", show.Command.ToString())

}

func showSendAnywhere(show *sqlparser.ShowBasic, vschema ContextVSchema) (engine.Primitive, error) {
	ks, err := vschema.FirstSortedKeyspace()
	if err != nil {
		return nil, err
	}
	return &engine.Send{
		Keyspace:          ks,
		TargetDestination: key.DestinationAnyShard{},
		Query:             sqlparser.String(show),
		IsDML:             false,
		SingleShardOnly:   true,
	}, nil
}

func showCharset(show *sqlparser.ShowBasic) (engine.Primitive, error) {
	fields := buildVarCharFields("Charset", "Description", "Default collation")
	maxLenField := &querypb.Field{Name: "Maxlen", Type: sqltypes.Int32}
	fields = append(fields, maxLenField)

	charsets := []string{utf8, utf8mb4}
	rows, err := generateCharsetRows(show.Filter, charsets)
	if err != nil {
		return nil, err
	}

	return engine.NewRowsPrimitive(rows, fields), nil
}

func buildShowColumnsPlan(show *sqlparser.ShowColumns, vschema ContextVSchema) (engine.Primitive, error) {
	if show.DbName != "" {
		show.Table.Qualifier = sqlparser.NewTableIdent(show.DbName)
	}
	table, _, _, _, destination, err := vschema.FindTableOrVindex(show.Table)
	if err != nil {
		return nil, err
	}
	if table == nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "table does not exists: %s", show.Table.Name.String())
	}
	if destination == nil {
		destination = key.DestinationAnyShard{}
	}

	// Remove Database Name from the query.
	show.DbName = ""
	show.Table.Qualifier = sqlparser.NewTableIdent("")
	show.Table.Name = table.Name

	return &engine.Send{
		Keyspace:          table.Keyspace,
		TargetDestination: destination,
		Query:             sqlparser.String(show),
		IsDML:             false,
		SingleShardOnly:   true,
	}, nil

}

func buildShowTableStatusPlan(show *sqlparser.ShowTableStatus, vschema ContextVSchema) (engine.Primitive, error) {
	destination, keyspace, _, err := vschema.TargetDestination(show.DatabaseName)
	if err != nil {
		return nil, err
	}
	if destination == nil {
		destination = key.DestinationAnyShard{}
	}

	// Remove Database Name from the query.
	show.DatabaseName = ""

	return &engine.Send{
		Keyspace:          keyspace,
		TargetDestination: destination,
		Query:             sqlparser.String(show),
		IsDML:             false,
		SingleShardOnly:   true,
	}, nil

}

func buildVarCharFields(names ...string) []*querypb.Field {
	fields := make([]*querypb.Field, len(names))
	for i, v := range names {
		fields[i] = &querypb.Field{
			Name:    v,
			Type:    sqltypes.VarChar,
			Charset: mysql.CharacterSetUtf8,
			Flags:   uint32(querypb.MySqlFlag_NOT_NULL_FLAG),
		}
	}
	return fields
}

func buildVarCharRow(values ...string) []sqltypes.Value {
	row := make([]sqltypes.Value, len(values))
	for i, v := range values {
		row[i] = sqltypes.NewVarChar(v)
	}
	return row
}

func generateCharsetRows(showFilter *sqlparser.ShowFilter, colNames []string) ([][]sqltypes.Value, error) {
	if showFilter == nil {
		return buildCharsetRows(both), nil
	}

	var filteredColName string
	var err error

	if showFilter.Like != "" {
		filteredColName, err = checkLikeOpt(showFilter.Like, colNames)
		if err != nil {
			return nil, err
		}

	} else {
		cmpExp, ok := showFilter.Filter.(*sqlparser.ComparisonExpr)
		if !ok {
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "expect a 'LIKE' or '=' expression")
		}

		left, ok := cmpExp.Left.(*sqlparser.ColName)
		if !ok {
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "expect left side to be 'charset'")
		}
		leftOk := left.Name.EqualString(charset)

		if leftOk {
			literal, ok := cmpExp.Right.(*sqlparser.Literal)
			if !ok {
				return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "we expect the right side to be a string")
			}
			rightString := string(literal.Val)

			switch cmpExp.Operator {
			case sqlparser.EqualOp:
				for _, colName := range colNames {
					if rightString == colName {
						filteredColName = colName
					}
				}
			case sqlparser.LikeOp:
				filteredColName, err = checkLikeOpt(rightString, colNames)
				if err != nil {
					return nil, err
				}
			}
		}

	}

	return buildCharsetRows(filteredColName), nil
}

func buildCharsetRows(colName string) [][]sqltypes.Value {
	row0 := buildVarCharRow(
		"utf8",
		"UTF-8 Unicode",
		"utf8_general_ci")
	row0 = append(row0, sqltypes.NewInt32(3))
	row1 := buildVarCharRow(
		"utf8mb4",
		"UTF-8 Unicode",
		"utf8mb4_general_ci")
	row1 = append(row1, sqltypes.NewInt32(4))

	switch colName {
	case utf8:
		return [][]sqltypes.Value{row0}
	case utf8mb4:
		return [][]sqltypes.Value{row1}
	case both:
		return [][]sqltypes.Value{row0, row1}
	}

	return [][]sqltypes.Value{}
}

func checkLikeOpt(likeOpt string, colNames []string) (string, error) {
	likeRegexp := strings.ReplaceAll(likeOpt, "%", ".*")
	for _, v := range colNames {
		match, err := regexp.MatchString(likeRegexp, v)
		if err != nil {
			return "", err
		}
		if match {
			return v, nil
		}
	}

	return "", nil
}
