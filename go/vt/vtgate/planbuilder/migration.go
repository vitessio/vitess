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
	"vitess.io/vitess/go/vt/key"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

func buildAlterMigrationPlan(query string, vschema plancontext.VSchema, enableOnlineDDL bool) (*planResult, error) {
	if !enableOnlineDDL {
		return nil, schema.ErrOnlineDDLDisabled
	}
	dest, ks, tabletType, err := vschema.TargetDestination("")
	if err != nil {
		return nil, err
	}
	if ks == nil {
		return nil, vterrors.VT09005()
	}

	if tabletType != topodatapb.TabletType_PRIMARY {
		return nil, vterrors.VT09006("ALTER")
	}

	if dest == nil {
		dest = key.DestinationAllShards{}
	}

	send := &engine.Send{
		Keyspace:          ks,
		TargetDestination: dest,
		Query:             query,
	}
	return newPlanResult(send), nil
}

func buildRevertMigrationPlan(query string, stmt *sqlparser.RevertMigration, vschema plancontext.VSchema, enableOnlineDDL bool) (*planResult, error) {
	if !enableOnlineDDL {
		return nil, schema.ErrOnlineDDLDisabled
	}
	dest, ks, tabletType, err := vschema.TargetDestination("")
	if err != nil {
		return nil, err
	}
	if ks == nil {
		return nil, vterrors.VT09005()
	}

	if tabletType != topodatapb.TabletType_PRIMARY {
		return nil, vterrors.VT09006("REVERT")
	}

	if dest == nil {
		dest = key.DestinationAllShards{}
	}

	emig := &engine.RevertMigration{
		Keyspace:          ks,
		TargetDestination: dest,
		Stmt:              stmt,
		Query:             query,
	}
	return newPlanResult(emig), nil
}

func buildShowMigrationLogsPlan(query string, vschema plancontext.VSchema, enableOnlineDDL bool) (*planResult, error) {
	if !enableOnlineDDL {
		return nil, schema.ErrOnlineDDLDisabled
	}
	dest, ks, tabletType, err := vschema.TargetDestination("")
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

	send := &engine.Send{
		Keyspace:          ks,
		TargetDestination: dest,
		Query:             query,
	}
	return newPlanResult(send), nil
}

// buildShowVMigrationsPlan serves `SHOW VITESS_MIGRATIONS ...` queries.
// It invokes queries on the sidecar database's schema_migrations table
// on all PRIMARY tablets in the keyspace's shards.
func buildShowVMigrationsPlan(show *sqlparser.ShowMigrations, vschema plancontext.VSchema) (*planResult, error) {
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

	send := &engine.Send{
		Keyspace:          ks,
		TargetDestination: dest,
		Query:             sqlparser.String(show),
	}
	return newPlanResult(send), nil

	// sidecarDBID, err := sidecardb.GetIdentifierForKeyspace(ks.Name)
	// if err != nil {
	// 	log.Errorf("Failed to read sidecar database identifier for keyspace %q from the cache: %v", ks.Name, err)
	// 	return nil, vterrors.VT14005(ks.Name)
	// }

	// sql := sqlparser.BuildParsedQuery("SELECT * FROM %s.schema_migrations", sidecarDBID).Query

	// if show.Filter != nil {
	// 	if show.Filter.Filter != nil {
	// 		sql += fmt.Sprintf(" where %s", sqlparser.String(show.Filter.Filter))
	// 	} else if show.Filter.Like != "" {
	// 		lit := sqlparser.String(sqlparser.NewStrLiteral(show.Filter.Like))
	// 		sql += fmt.Sprintf(" where migration_uuid LIKE %s OR migration_context LIKE %s OR migration_status LIKE %s", lit, lit, lit)
	// 	}
	// }
	// return &engine.Send{
	// 	Keyspace:          ks,
	// 	TargetDestination: dest,
	// 	Query:             sql,
	// }, nil
}
