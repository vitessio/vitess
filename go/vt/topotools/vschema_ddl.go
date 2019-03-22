/*
Copyright 2018 The Vitess Authors

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

package topotools

import (
	"reflect"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"

	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// ApplyVSchemaDDL applies the given DDL statement to the vschema
// keyspace definition and returns the modified keyspace object.
func ApplyVSchemaDDL(ksName string, ks *vschemapb.Keyspace, ddl *sqlparser.DDL) (*vschemapb.Keyspace, error) {
	if ks == nil {
		ks = new(vschemapb.Keyspace)
	}

	if ks.Tables == nil {
		ks.Tables = map[string]*vschemapb.Table{}
	}

	if ks.Vindexes == nil {
		ks.Vindexes = map[string]*vschemapb.Vindex{}
	}

	var tableName string
	var table *vschemapb.Table
	if !ddl.Table.IsEmpty() {
		tableName = ddl.Table.Name.String()
		table = ks.Tables[tableName]
	}

	switch ddl.Action {
	case sqlparser.CreateVindexStr:
		name := ddl.VindexSpec.Name.String()
		if _, ok := ks.Vindexes[name]; ok {
			return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "vindex %s already exists in keyspace %s", name, ksName)
		}

		// Make sure the keyspace has the sharded bit set to true
		// if this is the first vindex defined in the keyspace.
		if len(ks.Vindexes) == 0 {
			ks.Sharded = true
		}

		owner, params := ddl.VindexSpec.ParseParams()
		ks.Vindexes[name] = &vschemapb.Vindex{
			Type:   ddl.VindexSpec.Type.String(),
			Params: params,
			Owner:  owner,
		}

		return ks, nil

	case sqlparser.AddVschemaTableStr:
		if ks.Sharded {
			return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "add vschema table: unsupported on sharded keyspace %s", ksName)
		}

		name := ddl.Table.Name.String()
		if _, ok := ks.Tables[name]; ok {
			return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "vschema already contains table %s in keyspace %s", name, ksName)
		}

		ks.Tables[name] = &vschemapb.Table{}

		return ks, nil

	case sqlparser.DropVschemaTableStr:
		name := ddl.Table.Name.String()
		if _, ok := ks.Tables[name]; !ok {
			return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "vschema does not contain table %s in keyspace %s", name, ksName)
		}

		delete(ks.Tables, name)

		return ks, nil

	case sqlparser.AddColVindexStr:
		// Support two cases:
		//
		// 1. The vindex type / params / owner are specified. If the
		//    named vindex doesn't exist, create it. If it does exist,
		//    require the parameters to match.
		//
		// 2. The vindex type is not specified. Make sure the vindex
		//    already exists.
		spec := ddl.VindexSpec
		name := spec.Name.String()
		if !spec.Type.IsEmpty() {
			owner, params := spec.ParseParams()
			if vindex, ok := ks.Vindexes[name]; ok {
				if vindex.Type != spec.Type.String() {
					return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "vindex %s defined with type %s not %s", name, vindex.Type, spec.Type.String())
				}
				if vindex.Owner != owner {
					return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "vindex %s defined with owner %s not %s", name, vindex.Owner, owner)
				}
				if (len(vindex.Params) != 0 || len(params) != 0) && !reflect.DeepEqual(vindex.Params, params) {
					return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "vindex %s defined with different parameters", name)
				}
			} else {
				// Make sure the keyspace has the sharded bit set to true
				// if this is the first vindex defined in the keyspace.
				if len(ks.Vindexes) == 0 {
					ks.Sharded = true
				}
				ks.Vindexes[name] = &vschemapb.Vindex{
					Type:   spec.Type.String(),
					Params: params,
					Owner:  owner,
				}
			}
		} else {
			if _, ok := ks.Vindexes[name]; !ok {
				return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "vindex %s does not exist in keyspace %s", name, ksName)
			}
		}

		// If this is the first vindex being defined on the table, create
		// the empty table record
		if table == nil {
			table = &vschemapb.Table{
				ColumnVindexes: make([]*vschemapb.ColumnVindex, 0, 4),
			}
		}

		// Make sure there isn't already a vindex with the same name on
		// this table.
		for _, vindex := range table.ColumnVindexes {
			if vindex.Name == name {
				return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "vindex %s already defined on table %s", name, tableName)
			}
		}

		columns := make([]string, len(ddl.VindexCols))
		for i, col := range ddl.VindexCols {
			columns[i] = col.String()
		}
		table.ColumnVindexes = append(table.ColumnVindexes, &vschemapb.ColumnVindex{
			Name:    name,
			Columns: columns,
		})
		ks.Tables[tableName] = table

		return ks, nil

	case sqlparser.DropColVindexStr:
		spec := ddl.VindexSpec
		name := spec.Name.String()
		if table == nil {
			return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "table %s.%s not defined in vschema", ksName, tableName)
		}

		for i, colVindex := range table.ColumnVindexes {
			if colVindex.Name == name {
				table.ColumnVindexes = append(table.ColumnVindexes[:i], table.ColumnVindexes[i+1:]...)
				if len(table.ColumnVindexes) == 0 {
					delete(ks.Tables, tableName)
				}
				return ks, nil
			}
		}
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "vindex %s not defined in table %s.%s", name, ksName, tableName)
	}

	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unexpected vindex ddl operation %s", ddl.Action)
}
