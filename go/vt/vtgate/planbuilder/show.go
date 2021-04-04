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
	"fmt"
	"regexp"
	"strings"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

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
	case *sqlparser.ShowCreate:
		return buildShowCreatePlan(show, vschema)
	default:
		return nil, ErrPlanNotSupported
	}
}

func buildShowBasicPlan(show *sqlparser.ShowBasic, vschema ContextVSchema) (engine.Primitive, error) {
	switch show.Command {
	case sqlparser.Charset:
		return buildCharsetPlan(show)
	case sqlparser.Collation, sqlparser.Function, sqlparser.Privilege, sqlparser.Procedure:
		return buildSendAnywherePlan(show, vschema)
	case sqlparser.VariableGlobal, sqlparser.VariableSession:
		return buildVariablePlan(show, vschema)
	case sqlparser.Column, sqlparser.Index:
		return buildShowTblPlan(show, vschema)
	case sqlparser.Database, sqlparser.Keyspace:
		return buildDBPlan(show, vschema)
	case sqlparser.OpenTable, sqlparser.TableStatus, sqlparser.Table, sqlparser.Trigger:
		return buildPlanWithDB(show, vschema)
	case sqlparser.StatusGlobal, sqlparser.StatusSession:
		return engine.NewRowsPrimitive(make([][]sqltypes.Value, 0, 2), buildVarCharFields("Variable_name", "Value")), nil
	case sqlparser.VitessMigrations:
		return buildShowVMigrationsPlan(show, vschema)
	}
	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] unknown show query type %s", show.Command.ToString())

}

func buildCharsetPlan(show *sqlparser.ShowBasic) (engine.Primitive, error) {
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

func buildSendAnywherePlan(show *sqlparser.ShowBasic, vschema ContextVSchema) (engine.Primitive, error) {
	ks, err := vschema.AnyKeyspace()
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

func buildVariablePlan(show *sqlparser.ShowBasic, vschema ContextVSchema) (engine.Primitive, error) {
	plan, err := buildSendAnywherePlan(show, vschema)
	if err != nil {
		return nil, err
	}
	plan = engine.NewReplaceVariables(plan)
	return plan, nil
}

func buildShowTblPlan(show *sqlparser.ShowBasic, vschema ContextVSchema) (engine.Primitive, error) {
	if !show.DbName.IsEmpty() {
		show.Tbl.Qualifier = sqlparser.NewTableIdent(show.DbName.String())
		// Remove Database Name from the query.
		show.DbName = sqlparser.NewTableIdent("")
	}

	dest := key.Destination(key.DestinationAnyShard{})
	var ks *vindexes.Keyspace
	var err error

	if !show.Tbl.Qualifier.IsEmpty() && sqlparser.SystemSchema(show.Tbl.Qualifier.String()) {
		ks, err = vschema.AnyKeyspace()
		if err != nil {
			return nil, err
		}
	} else {
		table, _, _, _, destination, err := vschema.FindTableOrVindex(show.Tbl)
		if err != nil {
			return nil, err
		}
		if table == nil {
			return nil, vterrors.NewErrorf(vtrpcpb.Code_NOT_FOUND, vterrors.UnknownTable, "Table '%s' doesn't exist", show.Tbl.Name.String())
		}
		// Update the table.
		show.Tbl.Qualifier = sqlparser.NewTableIdent("")
		show.Tbl.Name = table.Name

		if destination != nil {
			dest = destination
		}
		ks = table.Keyspace
	}

	return &engine.Send{
		Keyspace:          ks,
		TargetDestination: dest,
		Query:             sqlparser.String(show),
		IsDML:             false,
		SingleShardOnly:   true,
	}, nil
}

func buildDBPlan(show *sqlparser.ShowBasic, vschema ContextVSchema) (engine.Primitive, error) {
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

	//rows := make([][]sqltypes.Value, 0, len(ks)+4)
	var rows [][]sqltypes.Value

	if show.Command == sqlparser.Database {
		//Hard code default databases
		rows = append(rows, buildVarCharRow("information_schema"))
		rows = append(rows, buildVarCharRow("mysql"))
		rows = append(rows, buildVarCharRow("sys"))
		rows = append(rows, buildVarCharRow("performance_schema"))
	}

	for _, v := range ks {
		if filter.MatchString(v.Name) {
			rows = append(rows, buildVarCharRow(v.Name))
		}
	}
	return engine.NewRowsPrimitive(rows, buildVarCharFields("Database")), nil
}

// buildShowVMigrationsPlan serves `SHOW VITESS_MIGRATIONS ...` queries. It invokes queries on _vt.schema_migrations on all MASTER tablets on keyspace's shards.
func buildShowVMigrationsPlan(show *sqlparser.ShowBasic, vschema ContextVSchema) (engine.Primitive, error) {
	dest, ks, tabletType, err := vschema.TargetDestination(show.DbName.String())
	if err != nil {
		return nil, err
	}
	if ks == nil {
		return nil, vterrors.NewErrorf(vtrpcpb.Code_FAILED_PRECONDITION, vterrors.NoDB, "No database selected: use keyspace<:shard><@type> or keyspace<[range]><@type> (<> are optional)")
	}

	if tabletType != topodatapb.TabletType_MASTER {
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "show vitess_migrations works only on primary tablet")
	}

	if dest == nil {
		dest = key.DestinationAllShards{}
	}

	sql := "SELECT * FROM _vt.schema_migrations"

	if show.Filter != nil {
		if show.Filter.Filter != nil {
			sql += fmt.Sprintf(" where %s", sqlparser.String(show.Filter.Filter))
		} else if show.Filter.Like != "" {
			lit := sqlparser.String(sqlparser.NewStrLiteral(show.Filter.Like))
			sql += fmt.Sprintf(" where migration_uuid LIKE %s OR migration_context LIKE %s OR migration_status LIKE %s", lit, lit, lit)
		}
	}
	return &engine.Send{
		Keyspace:          ks,
		TargetDestination: dest,
		Query:             sql,
	}, nil
}

func buildPlanWithDB(show *sqlparser.ShowBasic, vschema ContextVSchema) (engine.Primitive, error) {
	dbName := show.DbName
	dbDestination := show.DbName.String()
	if sqlparser.SystemSchema(dbDestination) {
		ks, err := vschema.AnyKeyspace()
		if err != nil {
			return nil, err
		}
		dbDestination = ks.Name
	} else {
		// Remove Database Name from the query.
		show.DbName = sqlparser.NewTableIdent("")
	}
	destination, keyspace, _, err := vschema.TargetDestination(dbDestination)
	if err != nil {
		return nil, err
	}
	if destination == nil {
		destination = key.DestinationAnyShard{}
	}

	if dbName.IsEmpty() {
		dbName = sqlparser.NewTableIdent(keyspace.Name)
	}

	query := sqlparser.String(show)
	var plan engine.Primitive
	plan = &engine.Send{
		Keyspace:          keyspace,
		TargetDestination: destination,
		Query:             query,
		IsDML:             false,
		SingleShardOnly:   true,
	}
	if show.Command == sqlparser.Table {
		plan, err = engine.NewRenameField([]string{"Tables_in_" + dbName.String()}, []int{0}, plan)
		if err != nil {
			return nil, err
		}
	}
	return plan, nil

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
			return nil, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.SyntaxError, "expect a 'LIKE' or '=' expression")
		}

		left, ok := cmpExp.Left.(*sqlparser.ColName)
		if !ok {
			return nil, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.SyntaxError, "expect left side to be 'charset'")
		}
		leftOk := left.Name.EqualString(charset)

		if leftOk {
			literal, ok := cmpExp.Right.(*sqlparser.Literal)
			if !ok {
				return nil, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.SyntaxError, "we expect the right side to be a string")
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

func buildShowCreatePlan(show *sqlparser.ShowCreate, vschema ContextVSchema) (engine.Primitive, error) {
	switch show.Command {
	case sqlparser.CreateDb:
		return buildCreateDbPlan(show, vschema)
	case sqlparser.CreateE, sqlparser.CreateF, sqlparser.CreateProc, sqlparser.CreateTr, sqlparser.CreateV:
		return buildCreatePlan(show, vschema)
	case sqlparser.CreateTbl:
		return buildCreateTblPlan(show, vschema)
	}
	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] unknown show query type %s", show.Command.ToString())
}

func buildCreateDbPlan(show *sqlparser.ShowCreate, vschema ContextVSchema) (engine.Primitive, error) {
	dbName := show.Op.Name.String()
	if sqlparser.SystemSchema(dbName) {
		ks, err := vschema.AnyKeyspace()
		if err != nil {
			return nil, err
		}
		dbName = ks.Name
	}

	dest, ks, _, err := vschema.TargetDestination(dbName)
	if err != nil {
		return nil, err
	}

	if dest == nil {
		dest = key.DestinationAnyShard{}
	}

	return &engine.Send{
		Keyspace:          ks,
		TargetDestination: dest,
		Query:             sqlparser.String(show),
		IsDML:             false,
		SingleShardOnly:   true,
	}, nil
}

func buildCreateTblPlan(show *sqlparser.ShowCreate, vschema ContextVSchema) (engine.Primitive, error) {
	dest := key.Destination(key.DestinationAnyShard{})
	var ks *vindexes.Keyspace
	var err error

	if !show.Op.Qualifier.IsEmpty() && sqlparser.SystemSchema(show.Op.Qualifier.String()) {
		ks, err = vschema.AnyKeyspace()
		if err != nil {
			return nil, err
		}
	} else {
		tbl, _, _, _, destKs, err := vschema.FindTableOrVindex(show.Op)
		if err != nil {
			return nil, err
		}
		if tbl == nil {
			return nil, vterrors.NewErrorf(vtrpcpb.Code_NOT_FOUND, vterrors.UnknownTable, "Table '%s' doesn't exist", sqlparser.String(show.Op))
		}
		ks = tbl.Keyspace
		if destKs != nil {
			dest = destKs
		}
		show.Op.Qualifier = sqlparser.NewTableIdent("")
		show.Op.Name = tbl.Name
	}

	return &engine.Send{
		Keyspace:          ks,
		TargetDestination: dest,
		Query:             sqlparser.String(show),
		IsDML:             false,
		SingleShardOnly:   true,
	}, nil

}

func buildCreatePlan(show *sqlparser.ShowCreate, vschema ContextVSchema) (engine.Primitive, error) {
	dbName := ""
	if !show.Op.Qualifier.IsEmpty() {
		dbName = show.Op.Qualifier.String()
	}

	if sqlparser.SystemSchema(dbName) {
		ks, err := vschema.AnyKeyspace()
		if err != nil {
			return nil, err
		}
		dbName = ks.Name
	} else {
		show.Op.Qualifier = sqlparser.NewTableIdent("")
	}

	dest, ks, _, err := vschema.TargetDestination(dbName)
	if err != nil {
		return nil, err
	}
	if dest == nil {
		dest = key.DestinationAnyShard{}
	}

	return &engine.Send{
		Keyspace:          ks,
		TargetDestination: dest,
		Query:             sqlparser.String(show),
		IsDML:             false,
		SingleShardOnly:   true,
	}, nil

}
