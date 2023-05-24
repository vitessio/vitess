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

package vtgate

import (
	"context"
	"strings"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
)

func CheckVindexType(col string, colType string, vindexType string) error {
	Values := strings.Split(colType, " ") //eg:tinyint unsigned
	count := len(Values)

	typeStr := Values[0]
	UnsignedFlag := false
	if count == 2 {
		UnsignedFlag = true
	}
	sqlType := sqlparser.SQLTypeToQueryType(typeStr, UnsignedFlag)

	//64 bit or smaller numeric or equivalent type
	if vindexType == "hash" || vindexType == "numeric" ||
		vindexType == "numeric_static_map" || vindexType == "reverse_bits" {
		switch sqlType {
		case sqltypes.Int8, sqltypes.Int16, sqltypes.Int24, sqltypes.Int32, sqltypes.Int64,
			sqltypes.Uint8, sqltypes.Uint16, sqltypes.Uint24, sqltypes.Uint32, sqltypes.Uint64,
			sqltypes.Float32, sqltypes.Float64, sqltypes.Decimal, sqltypes.Bit:
			return nil

		default:
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "column %v should not be %v for vindex type %v", col, colType, vindexType)
		}
	}

	//String and numeric type
	if vindexType == "region_experimental" || vindexType == "region_json" {
		switch sqlType {
		case sqltypes.Int8, sqltypes.Int16, sqltypes.Int24, sqltypes.Int32, sqltypes.Int64,
			sqltypes.Uint8, sqltypes.Uint16, sqltypes.Uint24, sqltypes.Uint32, sqltypes.Uint64,
			sqltypes.Float32, sqltypes.Float64, sqltypes.Decimal, sqltypes.Bit,
			sqltypes.Text, sqltypes.VarChar, sqltypes.Char,
			sqltypes.Set, sqltypes.Enum:
			return nil

		default:
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "column %v should not be %v for vindex type %v", col, colType, vindexType)
		}
	}

	//String or binary types
	if vindexType == "unicode_loose_md5" || vindexType == "unicode_loose_xxhash" {
		switch sqlType {
		case sqltypes.Text, sqltypes.VarChar, sqltypes.Char,
			sqltypes.Set, sqltypes.Enum,
			sqltypes.VarBinary, sqltypes.Binary, sqltypes.Blob:
			break

		default:
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "column %v should not be %v for vindex type %v", col, colType, vindexType)
		}
	}

	return nil
}

func SchemaCheck(vc *vcursorImpl, ctx context.Context, ksName string, vschemaDDL *sqlparser.AlterVschema) error {
	switch vschemaDDL.Action {
	case sqlparser.AddColVindexDDLAction:
		// refer to https://vitess.io/docs/16.0/reference/features/vindexes/

		var vindexCols = vschemaDDL.VindexCols
		var tableName = vschemaDDL.Table.Name.String()
		var Qualifier = vschemaDDL.Table.Qualifier.String()
		var Type = vschemaDDL.VindexSpec.Type
		//var params = vschemaDDL.VindexSpec.Params

		var executor = vc.executor.(*Executor)
		var sql = "describe " + tableName
		if Qualifier == "" {
			Qualifier = ksName
		}

		ftRes, err := executor.fetchSchemaFromTablet(ctx, &sqlparser.ShowFilter{Like: ksName}, sql)

		if err != nil {
			return err
		}
		mapList := make(map[string]string)
		for _, row := range ftRes.Rows {
			field := row[0].ToString()
			fieldType := row[1].ToString()
			idx := strings.Index(fieldType, "(")
			if idx != -1 {
				fieldType = fieldType[:idx]
			}
			fieldType = strings.ToLower(fieldType)
			mapList[field] = fieldType
		}

		for _, col := range vindexCols {
			colName := col.String()
			colType, ok := mapList[colName]
			if !ok {
				return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "colmun %v does not existes", colName)
			}
			val := Type.String()
			err = CheckVindexType(colName, colType, val)
			if err != nil {
				return err
			}
		}
		return nil

	case sqlparser.AddAutoIncDDLAction:
		var executor = vc.executor.(*Executor)
		var seqTableName = vschemaDDL.AutoIncSpec.Sequence.Name.String()
		var seqQualifier = vschemaDDL.AutoIncSpec.Sequence.Qualifier.String()
		if seqQualifier == "" {
			seqQualifier = ksName
		}

		// check add vscheam add sequence
		var srvVschema = vc.vm.GetCurrentSrvVschema()
		ks := srvVschema.Keyspaces[seqQualifier]
		_, ok := ks.Tables[seqTableName]
		if !ok {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "sequence %v is not added in keyspace %v", seqTableName, seqQualifier)
		}
		// check shema for sequence
		// eg:ALTER VSCHEMA on test2 ADD AUTO_INCREMENT customer_id using commerce.test_seq2;
		autoTable := vschemaDDL.Table.Name.String()
		autoColumn := vschemaDDL.AutoIncSpec.Column.String()
		var sql = "describe " + autoTable
		ftRes, err := executor.fetchSchemaFromTablet(ctx, &sqlparser.ShowFilter{Like: ksName}, sql)
		if err != nil {
			return err
		}
		for _, row := range ftRes.Rows {
			field := row[0].ToString()
			if field == autoColumn {
				return nil
			}
		}
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "auto column %v does not exist in table %v", autoColumn, autoTable)

	case sqlparser.AddSequenceDDLAction:
		//  ALTER VSCHEMA ADD SEQUENCE commerce.test_seq;
		var tableName = vschemaDDL.Table.Name.String()
		var qualifier = vschemaDDL.Table.Qualifier.String()
		if qualifier == "" {
			qualifier = ksName
		}

		var executor = vc.executor.(*Executor)
		var sql = "describe " + tableName
		ftRes, err := executor.fetchSchemaFromTablet(ctx, &sqlparser.ShowFilter{Like: ksName}, sql)

		if err != nil {
			return err
		}

		if len(ftRes.Rows) == 0 {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "table %v does not exist in keyspace %v", tableName, qualifier)
		}

		// check1: id/next_id/cache, type is bigint
		// check2: id is primary key
		// +---------+--------+------+-----+---------+-------+
		// | Field   | Type   | Null | Key | Default | Extra |
		// +---------+--------+------+-----+---------+-------+
		// | id      | bigint | NO   | PRI | NULL    |       |
		// | next_id | bigint | YES  |     | NULL    |       |
		// | cache   | bigint | YES  |     | NULL    |       |
		// +---------+--------+------+-----+---------+-------+

		mapList := make(map[string]int)
		mapList["id"] = 1
		mapList["next_id"] = 1
		mapList["cache"] = 1
		for _, row := range ftRes.Rows {
			field := row[0].ToString()
			fieldType := row[1].ToString()
			keyCol := row[3].ToString()
			idx := strings.Index(fieldType, "(")
			if idx != -1 {
				fieldType = fieldType[:idx]
			}

			fieldType = strings.ToLower(fieldType)
			if fieldType != "bigint" {
				return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "column type should be bigint")
			}
			if field == "id" {
				if keyCol != "PRI" {
					return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "column id should be primary key")
				}
				mapList["id"] = 0
			} else if field == "next_id" || field == "cache" {
				mapList[field] = 0
			} else {
				return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "column name should be id/next_id/cache")
			}
		}

		for col, num := range mapList {
			if num != 0 {
				return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "column %v does not exist", col)
			}
		}

		// check 3: comments is 'vitess_sequence'
		// SELECT table_name, table_comment FROM information_schema.TABLES WHERE table_schema='vt_commerce' and table_name = 'test_seq2';
		// +------------+-----------------+
		// | TABLE_NAME | TABLE_COMMENT   |
		// +------------+-----------------+
		// | test_seq2  | vitess_sequence |
		// +------------+-----------------+

		var sqlCheckTableComment = "SELECT table_name, table_comment FROM information_schema.TABLES WHERE table_schema='vt_" + qualifier + "' and table_name = '" + tableName + "'"
		ftRes, err = executor.fetchSchemaFromTablet(ctx, &sqlparser.ShowFilter{Like: qualifier}, sqlCheckTableComment)

		if err != nil {
			return err
		}

		if len(ftRes.Rows) != 1 {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "can not get table_comment from table %v in keyspace %v", tableName, qualifier)
		}

		commentCol := ftRes.Rows[0][1].ToString()
		if commentCol != "vitess_sequence" {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "table %v comment should be 'vitess_sequence'", tableName)
		}

		return nil

	case sqlparser.CreateVindexDDLAction:
		// create vindex
		owner, params := vschemaDDL.VindexSpec.ParseParams()
		lookup_table := params["table"]
		from_col := params["from"]
		to_col := params["to"]

		// not lookup vindex, ie: create vindex hash using hash
		if owner == "" && len(params) == 0 {
			return nil
		}

		// check syntax
		if owner == "" {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "Syntax error: please specify the value for the keyword `owner`")
		}
		if lookup_table == "" {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "Syntax error: please specify the value for the keyword `table`")
		}
		if from_col == "" {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "Syntax error: please specify the value for the keyword `from`")
		}
		if to_col == "" {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "Syntax error: please specify the value for the keyword `to`")
		}

		// get vschema
		srvVschema := vc.vm.GetCurrentSrvVschema()
		if srvVschema == nil {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "vschema not loaded")
		}

		// check whether owner table exists
		var ownerTable sqlparser.TableName
		if strings.Contains(owner, ".") {
			ownerTable = sqlparser.TableName{
				Name:      sqlparser.NewIdentifierCS(strings.Split(owner, ".")[1]),
				Qualifier: sqlparser.NewIdentifierCS(strings.Split(owner, ".")[0]),
			}

		} else {
			ownerTable = sqlparser.TableName{
				Name:      sqlparser.NewIdentifierCS(owner),
				Qualifier: sqlparser.NewIdentifierCS(""),
			}
		}
		var ownerksName string
		if ownerTable.Qualifier.String() != "" {
			ownerksName = ownerTable.Qualifier.String()
		} else {
			ownerksName = ksName
		}
		// check whether vschema contains owner table
		ownerks := srvVschema.Keyspaces[ownerksName]
		if _, ownervschemaok := ownerks.Tables[ownerTable.Name.String()]; !ownervschemaok {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "vschema does not contain owner table '%s' in keyspace %s",
				ownerTable.Name.String(), ownerksName)
		}
		// check whether owner table exists in the keyspace
		ownerKsTableSchema, ownerKserr := vc.FindTableSchema(ctx, ownerksName)

		if ownerKserr != nil {
			return ownerKserr
		}

		if _, ownerschemaok := ownerKsTableSchema[ownerTable.Name.String()]; !ownerschemaok {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "owner table '%s' does not exist in keyspace %s",
				ownerTable.Name.String(), ownerksName)
		}

		// check whether owner table have a primary vindex
		// The first columnvindexes is used as primary vindex
		if len(ownerks.Tables[ownerTable.Name.String()].ColumnVindexes) == 0 {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "The owner table '%s' does not have a primary vindex",
				ownerTable.Name.String())
		}

		// check whether lookup table exists
		var lookupTable sqlparser.TableName

		if strings.Contains(lookup_table, ".") {
			lookupTable = sqlparser.TableName{
				Name:      sqlparser.NewIdentifierCS(strings.Split(lookup_table, ".")[1]),
				Qualifier: sqlparser.NewIdentifierCS(strings.Split(lookup_table, ".")[0]),
			}

		} else {
			lookupTable = sqlparser.TableName{
				Name:      sqlparser.NewIdentifierCS(lookup_table),
				Qualifier: sqlparser.NewIdentifierCS(""),
			}
		}
		var lookupksName string
		if lookupTable.Qualifier.String() != "" {
			lookupksName = lookupTable.Qualifier.String()
		} else {
			lookupksName = ksName
		}

		// check whether vschema contains lookup table
		lookupks := srvVschema.Keyspaces[lookupksName]
		if _, lookupvschemaok := lookupks.Tables[lookupTable.Name.String()]; !lookupvschemaok {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "vschema does not contain lookup table '%s' in keyspace %s",
				lookupTable.Name.String(), lookupksName)
		}
		// check whether lookup table exists in the keyspace
		lookupKsTableSchema := ownerKsTableSchema // read_only map
		lookupKserr := ownerKserr
		if !strings.EqualFold(lookupksName, ownerksName) {
			lookupKsTableSchema, lookupKserr = vc.FindTableSchema(ctx, lookupksName)
		}
		if lookupKserr != nil {
			return lookupKserr
		}

		if _, lookupschemaok := lookupKsTableSchema[lookupTable.Name.String()]; !lookupschemaok {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "lookup table '%s' does not exist in keyspace %s",
				lookupTable.Name.String(), lookupksName)
		}

		// check whether from col exists in table schema
		for _, fromcol := range strings.Split(from_col, ",") {
			fromcol_exists := false
			for _, columnschema := range ownerKsTableSchema[ownerTable.Name.String()].Columns {
				if columnschema.Name.EqualString(strings.TrimSpace(fromcol)) {
					fromcol_exists = true
					break
				}
			}
			if !fromcol_exists {
				return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "from column '%s' does not exist in ownertable %s",
					fromcol, ownerTable.Name.String())
			}
		}

		if to_col != "" {
			log.Infof("TODO:")
		}
		return nil
	}

	return nil
}
