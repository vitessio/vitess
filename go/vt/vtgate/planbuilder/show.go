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
	"sort"
	"strings"
	"sync"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/log"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sidecardb"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	popcode "vitess.io/vitess/go/vt/vtgate/engine/opcode"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

const (
	charset = "charset"
)

func buildShowPlan(sql string, stmt *sqlparser.Show, _ *sqlparser.ReservedVars, vschema plancontext.VSchema) (*planResult, error) {
	if vschema.Destination() != nil {
		return buildByPassDDLPlan(sql, vschema)
	}

	var prim engine.Primitive
	var err error
	switch show := stmt.Internal.(type) {
	case *sqlparser.ShowBasic:
		prim, err = buildShowBasicPlan(show, vschema)
	case *sqlparser.ShowCreate:
		prim, err = buildShowCreatePlan(show, vschema)
	case *sqlparser.ShowOther:
		prim, err = buildShowOtherPlan(sql, vschema)
	default:
		return nil, vterrors.VT13001(fmt.Sprintf("undefined SHOW type: %T", stmt.Internal))
	}
	if err != nil {
		return nil, err
	}

	return newPlanResult(prim), nil
}

func buildShowOtherPlan(sql string, vschema plancontext.VSchema) (engine.Primitive, error) {
	ks, err := vschema.AnyKeyspace()
	if err != nil {
		return nil, err
	}
	return &engine.Send{
		Keyspace:          ks,
		TargetDestination: key.DestinationAnyShard{},
		Query:             sql,
		SingleShardOnly:   true,
	}, nil
}

func buildShowBasicPlan(show *sqlparser.ShowBasic, vschema plancontext.VSchema) (engine.Primitive, error) {
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
		return buildSendAnywherePlan(show, vschema)
	case sqlparser.VitessMigrations:
		return buildShowVMigrationsPlan(show, vschema)
	case sqlparser.VGtidExecGlobal:
		return buildShowVGtidPlan(show, vschema)
	case sqlparser.GtidExecGlobal:
		return buildShowGtidPlan(show, vschema)
	case sqlparser.Warnings:
		return buildWarnings()
	case sqlparser.Plugins:
		return buildPluginsPlan()
	case sqlparser.Engines:
		return buildEnginesPlan()
	case sqlparser.VitessReplicationStatus, sqlparser.VitessShards, sqlparser.VitessTablets, sqlparser.VitessVariables:
		return &engine.ShowExec{
			Command:    show.Command,
			ShowFilter: show.Filter,
		}, nil
	case sqlparser.VitessTarget:
		return buildShowTargetPlan(vschema)
	case sqlparser.VschemaTables:
		return buildVschemaTablesPlan(vschema)
	case sqlparser.VschemaVindexes:
		return buildVschemaVindexesPlan(show, vschema)
	}
	return nil, vterrors.VT13001(fmt.Sprintf("unknown SHOW query type %s", show.Command.ToString()))

}

func buildShowTargetPlan(vschema plancontext.VSchema) (engine.Primitive, error) {
	rows := [][]sqltypes.Value{buildVarCharRow(vschema.TargetString())}
	return engine.NewRowsPrimitive(rows,
		buildVarCharFields("Target")), nil
}

func buildCharsetPlan(show *sqlparser.ShowBasic) (engine.Primitive, error) {
	fields := buildVarCharFields("Charset", "Description", "Default collation")
	maxLenField := &querypb.Field{Name: "Maxlen", Type: sqltypes.Uint32, Charset: collations.CollationBinaryID, Flags: uint32(querypb.MySqlFlag_NUM_FLAG | querypb.MySqlFlag_NOT_NULL_FLAG | querypb.MySqlFlag_UNSIGNED_FLAG | querypb.MySqlFlag_NO_DEFAULT_VALUE_FLAG)}
	fields = append(fields, maxLenField)
	cs, err := generateCharsetRows(show.Filter)
	if err != nil {
		return nil, err
	}
	return engine.NewRowsPrimitive(cs, fields), nil
}

func buildSendAnywherePlan(show *sqlparser.ShowBasic, vschema plancontext.VSchema) (engine.Primitive, error) {
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

func buildVariablePlan(show *sqlparser.ShowBasic, vschema plancontext.VSchema) (engine.Primitive, error) {
	plan, err := buildSendAnywherePlan(show, vschema)
	if err != nil {
		return nil, err
	}
	plan = engine.NewReplaceVariables(plan)
	return plan, nil
}

func buildShowTblPlan(show *sqlparser.ShowBasic, vschema plancontext.VSchema) (engine.Primitive, error) {
	if !show.DbName.IsEmpty() {
		show.Tbl.Qualifier = sqlparser.NewIdentifierCS(show.DbName.String())
		// Remove Database Name from the query.
		show.DbName = sqlparser.NewIdentifierCS("")
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
			return nil, vterrors.VT05004(show.Tbl.Name.String())
		}
		// Update the table.
		show.Tbl.Qualifier = sqlparser.NewIdentifierCS("")
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

func buildDBPlan(show *sqlparser.ShowBasic, vschema plancontext.VSchema) (engine.Primitive, error) {
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

	// rows := make([][]sqltypes.Value, 0, len(ks)+4)
	var rows [][]sqltypes.Value

	if show.Command == sqlparser.Database {
		// Hard code default databases
		ks = append(ks, &vindexes.Keyspace{Name: "information_schema"},
			&vindexes.Keyspace{Name: "mysql"},
			&vindexes.Keyspace{Name: "sys"},
			&vindexes.Keyspace{Name: "performance_schema"})
	}

	for _, v := range ks {
		if filter.MatchString(v.Name) {
			rows = append(rows, buildVarCharRow(v.Name))
		}
	}
	return engine.NewRowsPrimitive(rows, buildVarCharFields("Database")), nil
}

// buildShowVMigrationsPlan serves `SHOW VITESS_MIGRATIONS ...` queries.
// It invokes queries on the sidecar database's schema_migrations table
// on all PRIMARY tablets in the keyspace's shards.
func buildShowVMigrationsPlan(show *sqlparser.ShowBasic, vschema plancontext.VSchema) (engine.Primitive, error) {
	dest, ks, tabletType, err := vschema.TargetDestination(show.DbName.String())
	if err != nil {
		return nil, err
	}
	if ks == nil {
		return nil, vterrors.VT09005()
	}

	if tabletType != topodatapb.TabletType_PRIMARY {
		return nil, vterrors.VT09006("SHOW")
	}

	if dest == nil {
		dest = key.DestinationAllShards{}
	}

	sidecarDBID, err := sidecardb.GetIdentifierForKeyspace(ks.Name)
	if err != nil {
		log.Errorf("Failed to read sidecar database identifier for keyspace %q from the cache: %v", ks.Name, err)
		return nil, vterrors.VT14005(ks.Name)
	}

	sql := sqlparser.BuildParsedQuery("SELECT * FROM %s.schema_migrations", sidecarDBID).Query

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

func buildPlanWithDB(show *sqlparser.ShowBasic, vschema plancontext.VSchema) (engine.Primitive, error) {
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
		show.DbName = sqlparser.NewIdentifierCS("")
	}
	destination, keyspace, _, err := vschema.TargetDestination(dbDestination)
	if err != nil {
		return nil, err
	}
	if destination == nil {
		destination = key.DestinationAnyShard{}
	}

	if dbName.IsEmpty() {
		dbName = sqlparser.NewIdentifierCS(keyspace.Name)
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
			Charset: uint32(collations.SystemCollation.Collation),
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

func generateCharsetRows(showFilter *sqlparser.ShowFilter) ([][]sqltypes.Value, error) {
	if showFilter == nil {
		return charsets(), nil
	}

	if showFilter.Like != "" {
		return filterLike(showFilter.Like, charsets())
	} else {
		cmpExp, ok := showFilter.Filter.(*sqlparser.ComparisonExpr)
		if !ok {
			return nil, vterrors.VT12001("expect a 'LIKE' or '=' expression")
		}

		left, ok := cmpExp.Left.(*sqlparser.ColName)
		if !ok {
			return nil, vterrors.VT12001("expect left side to be 'charset'")
		}
		leftOk := left.Name.EqualString(charset)

		if leftOk {
			literal, ok := cmpExp.Right.(*sqlparser.Literal)
			if !ok {
				return nil, vterrors.VT12001("we expect the right side to be a string")
			}
			rightString := literal.Val

			switch cmpExp.Operator {
			case sqlparser.EqualOp:
				for _, row := range charsets() {
					colName := row[0].ToString()
					if rightString == colName {
						return [][]sqltypes.Value{row}, nil
					}
				}
				return nil, nil
			case sqlparser.LikeOp:
				return filterLike(rightString, charsets())
			}
		} else {
			return nil, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.BadFieldError, "Unknown column '%s' in 'where clause'", left.Name.String())
		}
	}

	return charsets(), nil
}

var once sync.Once
var charsetRows [][]sqltypes.Value

func charsets() [][]sqltypes.Value {
	once.Do(func() {
		charsetRows = [][]sqltypes.Value{
			append(buildVarCharRow("armscii8", "ARMSCII-8 Armenian", "armscii8_general_ci"), sqltypes.NewUint32(1)),
			append(buildVarCharRow("ascii", "US ASCII", "ascii_general_ci"), sqltypes.NewUint32(1)),
			append(buildVarCharRow("binary", "Binary pseudo charset", "binary"), sqltypes.NewUint32(1)),
			append(buildVarCharRow("cp1250", "Windows Central European", "cp1250_general_ci"), sqltypes.NewUint32(1)),
			append(buildVarCharRow("cp1251", "Windows Cyrillic", "cp1251_general_ci"), sqltypes.NewUint32(1)),
			append(buildVarCharRow("cp1256", "Windows Arabic", "cp1256_general_ci"), sqltypes.NewUint32(1)),
			append(buildVarCharRow("cp1257", "Windows Baltic", "cp1257_general_ci"), sqltypes.NewUint32(1)),
			append(buildVarCharRow("cp850", "DOS West European", "cp850_general_ci"), sqltypes.NewUint32(1)),
			append(buildVarCharRow("cp852", "DOS Central European", "cp852_general_ci"), sqltypes.NewUint32(1)),
			append(buildVarCharRow("cp866", "DOS Russian", "cp866_general_ci"), sqltypes.NewUint32(1)),
			append(buildVarCharRow("cp932", "SJIS for Windows Japanese", "cp932_japanese_ci"), sqltypes.NewUint32(2)),
			append(buildVarCharRow("dec8", "DEC West European", "dec8_swedish_ci"), sqltypes.NewUint32(1)),
			append(buildVarCharRow("eucjpms", "UJIS for Windows Japanese", "eucjpms_japanese_ci"), sqltypes.NewUint32(3)),
			append(buildVarCharRow("euckr", "EUC-KR Korean", "euckr_korean_ci"), sqltypes.NewUint32(2)),
			append(buildVarCharRow("gb2312", "GB2312 Simplified Chinese", "gb2312_chinese_ci"), sqltypes.NewUint32(2)),
			append(buildVarCharRow("geostd8", "GEOSTD8 Georgian", "geostd8_general_ci"), sqltypes.NewUint32(1)),
			append(buildVarCharRow("greek", "ISO 8859-7 Greek", "greek_general_ci"), sqltypes.NewUint32(1)),
			append(buildVarCharRow("hebrew", "ISO 8859-8 Hebrew", "hebrew_general_ci"), sqltypes.NewUint32(1)),
			append(buildVarCharRow("hp8", "HP West European", "hp8_english_ci"), sqltypes.NewUint32(1)),
			append(buildVarCharRow("keybcs2", "DOS Kamenicky Czech-Slovak", "keybcs2_general_ci"), sqltypes.NewUint32(1)),
			append(buildVarCharRow("koi8r", "KOI8-R Relcom Russian", "koi8r_general_ci"), sqltypes.NewUint32(1)),
			append(buildVarCharRow("koi8u", "KOI8-U Ukrainian", "koi8u_general_ci"), sqltypes.NewUint32(1)),
			append(buildVarCharRow("latin1", "cp1252 West European", "latin1_swedish_ci"), sqltypes.NewUint32(1)),
			append(buildVarCharRow("latin2", "ISO 8859-2 Central European", "latin2_general_ci"), sqltypes.NewUint32(1)),
			append(buildVarCharRow("latin5", "ISO 8859-9 Turkish", "latin5_turkish_ci"), sqltypes.NewUint32(1)),
			append(buildVarCharRow("latin7", "ISO 8859-13 Baltic", "latin7_general_ci"), sqltypes.NewUint32(1)),
			append(buildVarCharRow("macce", "Mac Central European", "macce_general_ci"), sqltypes.NewUint32(1)),
			append(buildVarCharRow("macroman", "Mac West European", "macroman_general_ci"), sqltypes.NewUint32(1)),
			append(buildVarCharRow("sjis", "Shift-JIS Japanese", "sjis_japanese_ci"), sqltypes.NewUint32(2)),
			append(buildVarCharRow("swe7", "7bit Swedish", "swe7_swedish_ci"), sqltypes.NewUint32(1)),
			append(buildVarCharRow("ucs2", "UCS-2 Unicode", "ucs2_general_ci"), sqltypes.NewUint32(2)),
			append(buildVarCharRow("ujis", "EUC-JP Japanese", "ujis_japanese_ci"), sqltypes.NewUint32(3)),
			append(buildVarCharRow("utf16", "UTF-16 Unicode", "utf16_general_ci"), sqltypes.NewUint32(4)),
			append(buildVarCharRow("utf16le", "UTF-16LE Unicode", "utf16le_general_ci"), sqltypes.NewUint32(4)),
			append(buildVarCharRow("utf32", "UTF-32 Unicode", "utf32_general_ci"), sqltypes.NewUint32(4)),
			append(buildVarCharRow("utf8mb3", "UTF-8 Unicode", "utf8mb3_general_ci"), sqltypes.NewUint32(3)),
			append(buildVarCharRow("utf8mb4", "UTF-8 Unicode", "utf8mb4_0900_ai_ci"), sqltypes.NewUint32(4)),
		}
	})

	return charsetRows
}

func filterLike(likeOpt string, charsets [][]sqltypes.Value) ([][]sqltypes.Value, error) {
	likeRegexp := sqlparser.LikeToRegexp(likeOpt)
	var results [][]sqltypes.Value
	for _, row := range charsets {
		colName := row[0].ToString()
		if likeRegexp.MatchString(colName) {
			results = append(results, row)
		}
	}

	return results, nil
}

func buildShowCreatePlan(show *sqlparser.ShowCreate, vschema plancontext.VSchema) (engine.Primitive, error) {
	switch show.Command {
	case sqlparser.CreateDb:
		return buildCreateDbPlan(show, vschema)
	case sqlparser.CreateE, sqlparser.CreateF, sqlparser.CreateProc, sqlparser.CreateTr, sqlparser.CreateV:
		return buildCreatePlan(show, vschema)
	case sqlparser.CreateTbl:
		return buildCreateTblPlan(show, vschema)
	}
	return nil, vterrors.VT13001("unknown SHOW query type %s", show.Command.ToString())
}

func buildCreateDbPlan(show *sqlparser.ShowCreate, vschema plancontext.VSchema) (engine.Primitive, error) {
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

func buildCreateTblPlan(show *sqlparser.ShowCreate, vschema plancontext.VSchema) (engine.Primitive, error) {
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
			return nil, vterrors.VT05004(sqlparser.String(show.Op))
		}
		ks = tbl.Keyspace
		if destKs != nil {
			dest = destKs
		}
		show.Op.Qualifier = sqlparser.NewIdentifierCS("")
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

func buildCreatePlan(show *sqlparser.ShowCreate, vschema plancontext.VSchema) (engine.Primitive, error) {
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
		show.Op.Qualifier = sqlparser.NewIdentifierCS("")
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

func buildShowVGtidPlan(show *sqlparser.ShowBasic, vschema plancontext.VSchema) (engine.Primitive, error) {
	send, err := buildShowGtidPlan(show, vschema)
	if err != nil {
		return nil, err
	}
	return &engine.OrderedAggregate{
		Aggregates: []*engine.AggregateParams{
			engine.NewAggregateParam(popcode.AggregateGtid, 1, "global vgtid_executed"),
		},
		TruncateColumnCount: 2,
		Input:               send,
	}, nil
}

func buildShowGtidPlan(show *sqlparser.ShowBasic, vschema plancontext.VSchema) (engine.Primitive, error) {
	dbName := ""
	if !show.DbName.IsEmpty() {
		dbName = show.DbName.String()
	}
	dest, ks, _, err := vschema.TargetDestination(dbName)
	if err != nil {
		return nil, err
	}
	if dest == nil {
		dest = key.DestinationAllShards{}
	}

	return &engine.Send{
		Keyspace:          ks,
		TargetDestination: dest,
		Query:             fmt.Sprintf(`select '%s' as db_name, @@global.gtid_executed as gtid_executed, :%s as shard`, ks.Name, engine.ShardName),
		ShardNameNeeded:   true,
	}, nil
}

func buildWarnings() (engine.Primitive, error) {

	f := func(sa engine.SessionActions) (*sqltypes.Result, error) {
		fields := []*querypb.Field{
			{Name: "Level", Type: sqltypes.VarChar, Charset: uint32(collations.SystemCollation.Collation)},
			{Name: "Code", Type: sqltypes.Uint16, Charset: collations.CollationBinaryID, Flags: uint32(querypb.MySqlFlag_NUM_FLAG | querypb.MySqlFlag_UNSIGNED_FLAG)},
			{Name: "Message", Type: sqltypes.VarChar, Charset: uint32(collations.SystemCollation.Collation)},
		}

		warns := sa.GetWarnings()
		rows := make([][]sqltypes.Value, 0, len(warns))

		for _, warn := range warns {
			rows = append(rows, []sqltypes.Value{
				sqltypes.NewVarChar("Warning"),
				sqltypes.NewUint32(warn.Code),
				sqltypes.NewVarChar(warn.Message),
			})
		}
		return &sqltypes.Result{
			Fields: fields,
			Rows:   rows,
		}, nil
	}

	return engine.NewSessionPrimitive("SHOW WARNINGS", f), nil
}

func buildPluginsPlan() (engine.Primitive, error) {
	var rows [][]sqltypes.Value
	rows = append(rows, buildVarCharRow(
		"InnoDB",
		"ACTIVE",
		"STORAGE ENGINE",
		"NULL",
		"GPL"))

	return engine.NewRowsPrimitive(rows,
		buildVarCharFields("Name", "Status", "Type", "Library", "License")), nil
}

func buildEnginesPlan() (engine.Primitive, error) {
	var rows [][]sqltypes.Value
	rows = append(rows, buildVarCharRow(
		"InnoDB",
		"DEFAULT",
		"Supports transactions, row-level locking, and foreign keys",
		"YES",
		"YES",
		"YES"))

	return engine.NewRowsPrimitive(rows,
		buildVarCharFields("Engine", "Support", "Comment", "Transactions", "XA", "Savepoints")), nil
}

func buildVschemaTablesPlan(vschema plancontext.VSchema) (engine.Primitive, error) {
	vs := vschema.GetVSchema()
	ks, err := vschema.DefaultKeyspace()
	if err != nil {
		return nil, err
	}
	schemaKs, ok := vs.Keyspaces[ks.Name]
	if !ok {
		return nil, vterrors.VT05003(ks.Name)
	}

	var tables []string
	for name := range schemaKs.Tables {
		tables = append(tables, name)
	}
	sort.Strings(tables)

	rows := make([][]sqltypes.Value, len(tables))
	for i, v := range tables {
		rows[i] = buildVarCharRow(v)
	}

	return engine.NewRowsPrimitive(rows, buildVarCharFields("Tables")), nil
}

func buildVschemaVindexesPlan(show *sqlparser.ShowBasic, vschema plancontext.VSchema) (engine.Primitive, error) {
	vs := vschema.GetSrvVschema()
	rows := make([][]sqltypes.Value, 0, 16)

	if !show.Tbl.IsEmpty() {
		_, ks, _, err := vschema.TargetDestination(show.Tbl.Qualifier.String())
		if err != nil {
			return nil, err
		}
		var schemaKs *vschemapb.Keyspace
		var tbl *vschemapb.Table
		if !ks.Sharded {
			tbl = &vschemapb.Table{}
		} else {
			schemaKs = vs.Keyspaces[ks.Name]
			tableName := show.Tbl.Name.String()
			schemaTbl, ok := schemaKs.Tables[tableName]
			if !ok {
				return nil, vterrors.VT05005(tableName, ks.Name)
			}
			tbl = schemaTbl
		}

		for _, colVindex := range tbl.ColumnVindexes {
			vindex, ok := schemaKs.Vindexes[colVindex.GetName()]
			columns := colVindex.GetColumns()
			if len(columns) == 0 {
				columns = []string{colVindex.GetColumn()}
			}
			if ok {
				params := make([]string, 0, 4)
				for k, v := range vindex.GetParams() {
					params = append(params, fmt.Sprintf("%s=%s", k, v))
				}
				sort.Strings(params)
				rows = append(rows, buildVarCharRow(strings.Join(columns, ", "), colVindex.GetName(), vindex.GetType(), strings.Join(params, "; "), vindex.GetOwner()))
			} else {
				rows = append(rows, buildVarCharRow(strings.Join(columns, ", "), colVindex.GetName(), "", "", ""))
			}
		}

		return engine.NewRowsPrimitive(rows,
			buildVarCharFields("Columns", "Name", "Type", "Params", "Owner"),
		), nil
	}

	// For the query interface to be stable we need to sort
	// for each of the map iterations
	ksNames := make([]string, 0, len(vs.Keyspaces))
	for name := range vs.Keyspaces {
		ksNames = append(ksNames, name)
	}
	sort.Strings(ksNames)
	for _, ksName := range ksNames {
		ks := vs.Keyspaces[ksName]

		vindexNames := make([]string, 0, len(ks.Vindexes))
		for name := range ks.Vindexes {
			vindexNames = append(vindexNames, name)
		}
		sort.Strings(vindexNames)
		for _, vindexName := range vindexNames {
			vindex := ks.Vindexes[vindexName]

			params := make([]string, 0, 4)
			for k, v := range vindex.GetParams() {
				params = append(params, fmt.Sprintf("%s=%s", k, v))
			}
			sort.Strings(params)
			rows = append(rows, buildVarCharRow(ksName, vindexName, vindex.GetType(), strings.Join(params, "; "), vindex.GetOwner()))
		}
	}
	return engine.NewRowsPrimitive(rows,
		buildVarCharFields("Keyspace", "Name", "Type", "Params", "Owner"),
	), nil

}
