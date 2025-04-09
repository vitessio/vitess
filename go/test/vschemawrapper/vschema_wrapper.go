/*
Copyright 2023 The Vitess Authors.

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

package vschemawrapper

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	econtext "vitess.io/vitess/go/vt/vtgate/executorcontext"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

var _ plancontext.VSchema = (*VSchemaWrapper)(nil)

// VSchemaWrapper is a wrapper around VSchema that implements the ContextVSchema interface.
// It is used in tests to provide a VSchema implementation.
type VSchemaWrapper struct {
	Vcursor               *econtext.VCursorImpl
	V                     *vindexes.VSchema
	Keyspace              *vindexes.Keyspace
	TabletType_           topodatapb.TabletType
	Dest                  key.ShardDestination
	SysVarEnabled         bool
	ForeignKeyChecksState *bool
	Version               plancontext.PlannerVersion
	EnableViews           bool
	TestBuilder           func(query string, vschema plancontext.VSchema, keyspace string) (*engine.Plan, error)
	Env                   *vtenv.Environment
}

func NewVschemaWrapper(
	env *vtenv.Environment,
	vschema *vindexes.VSchema,
	builder func(string, plancontext.VSchema, string) (*engine.Plan, error),
) (*VSchemaWrapper, error) {
	ss := econtext.NewAutocommitSession(&vtgatepb.Session{})
	vcursor, err := econtext.NewVCursorImpl(ss, sqlparser.MarginComments{}, nil, nil, nil, vschema, nil, nil, nil, econtext.VCursorConfig{
		Collation:         env.CollationEnv().DefaultConnectionCharset(),
		DefaultTabletType: topodatapb.TabletType_PRIMARY,
		SetVarEnabled:     true,
	}, nil)
	if err != nil {
		return nil, err
	}
	return &VSchemaWrapper{
		Env:           env,
		V:             vschema,
		Vcursor:       vcursor,
		TestBuilder:   builder,
		TabletType_:   topodatapb.TabletType_PRIMARY,
		SysVarEnabled: true,
	}, nil
}

func (vw *VSchemaWrapper) GetPrepareData(stmtName string) *vtgatepb.PrepareData {
	switch stmtName {
	case "prep_one_param":
		return &vtgatepb.PrepareData{
			PrepareStatement: "select 1 from user where id = :v1",
			ParamsCount:      1,
		}
	case "prep_in_param":
		return &vtgatepb.PrepareData{
			PrepareStatement: "select 1 from user where id in (:v1, :v2)",
			ParamsCount:      2,
		}
	case "prep_no_param":
		return &vtgatepb.PrepareData{
			PrepareStatement: "select 1 from user",
			ParamsCount:      0,
		}
	case "prep_delete":
		return &vtgatepb.PrepareData{
			PrepareStatement: "delete from tbl5 where id = :v1",
			ParamsCount:      1,
		}
	}
	return nil
}

func (vw *VSchemaWrapper) PlanPrepareStatement(ctx context.Context, query string) (*engine.Plan, error) {
	plan, err := vw.TestBuilder(query, vw, vw.CurrentDb())
	if err != nil {
		return nil, err
	}
	return plan, nil
}

func (vw *VSchemaWrapper) ClearPrepareData(string) {}

func (vw *VSchemaWrapper) StorePrepareData(string, *vtgatepb.PrepareData) {}

func (vw *VSchemaWrapper) GetUDV(name string) *querypb.BindVariable {
	if strings.EqualFold(name, "prep_stmt") {
		return sqltypes.StringBindVariable("select * from user where id in (?, ?, ?)")
	}
	return nil
}

func (vw *VSchemaWrapper) IsShardRoutingEnabled() bool {
	return false
}

func (vw *VSchemaWrapper) GetVSchema() *vindexes.VSchema {
	return vw.V
}

func (vw *VSchemaWrapper) GetSrvVschema() *vschemapb.SrvVSchema {
	return &vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"user": {
				Sharded:  true,
				Vindexes: map[string]*vschemapb.Vindex{},
				Tables: map[string]*vschemapb.Table{
					"user": {},
				},
			},
		},
	}
}

func (vw *VSchemaWrapper) ConnCollation() collations.ID {
	return vw.Env.CollationEnv().DefaultConnectionCharset()
}

func (vw *VSchemaWrapper) Environment() *vtenv.Environment {
	return vw.Env
}

func (vw *VSchemaWrapper) PlannerWarning(_ string) {
}

func (vw *VSchemaWrapper) ForeignKeyMode(keyspace string) (vschemapb.Keyspace_ForeignKeyMode, error) {
	defaultFkMode := vschemapb.Keyspace_unmanaged
	if vw.V.Keyspaces[keyspace] != nil && vw.V.Keyspaces[keyspace].ForeignKeyMode != vschemapb.Keyspace_unspecified {
		return vw.V.Keyspaces[keyspace].ForeignKeyMode, nil
	}
	return defaultFkMode, nil
}

func (vw *VSchemaWrapper) KeyspaceError(keyspace string) error {
	return nil
}

func (vw *VSchemaWrapper) GetAggregateUDFs() (udfs []string) {
	return vw.V.GetAggregateUDFs()
}

func (vw *VSchemaWrapper) GetForeignKeyChecksState() *bool {
	return vw.ForeignKeyChecksState
}

func (vw *VSchemaWrapper) AllKeyspace() ([]*vindexes.Keyspace, error) {
	if vw.Keyspace == nil {
		return nil, vterrors.VT13001("keyspace not available")
	}
	return []*vindexes.Keyspace{vw.Keyspace}, nil
}

// FindKeyspace implements the VSchema interface
func (vw *VSchemaWrapper) FindKeyspace(keyspace string) (*vindexes.Keyspace, error) {
	if vw.Keyspace == nil {
		return nil, vterrors.VT13001("keyspace not available")
	}
	if vw.Keyspace.Name == keyspace {
		return vw.Keyspace, nil
	}
	return nil, nil
}

func (vw *VSchemaWrapper) Planner() plancontext.PlannerVersion {
	return vw.Version
}

// SetPlannerVersion implements the ContextVSchema interface
func (vw *VSchemaWrapper) SetPlannerVersion(v plancontext.PlannerVersion) {
	vw.Version = v
}

func (vw *VSchemaWrapper) GetSemTable() *semantics.SemTable {
	return nil
}

func (vw *VSchemaWrapper) KeyspaceExists(keyspace string) bool {
	if vw.Keyspace != nil {
		return vw.Keyspace.Name == keyspace
	}
	return false
}

func (vw *VSchemaWrapper) SysVarSetEnabled() bool {
	return vw.SysVarEnabled
}

func (vw *VSchemaWrapper) TargetDestination(qualifier string) (key.ShardDestination, *vindexes.Keyspace, topodatapb.TabletType, error) {
	return vw.Vcursor.TargetDestination(qualifier)
}

func (vw *VSchemaWrapper) TabletType() topodatapb.TabletType {
	return vw.TabletType_
}

func (vw *VSchemaWrapper) ShardDestination() key.ShardDestination {
	return vw.Dest
}

func (vw *VSchemaWrapper) FindTable(tab sqlparser.TableName) (*vindexes.BaseTable, string, topodatapb.TabletType, key.ShardDestination, error) {
	destKeyspace, destTabletType, destTarget, err := topoproto.ParseDestination(tab.Qualifier.String(), topodatapb.TabletType_PRIMARY)
	if err != nil {
		return nil, destKeyspace, destTabletType, destTarget, err
	}
	table, err := vw.V.FindTable(destKeyspace, tab.Name.String())
	if err != nil {
		return nil, destKeyspace, destTabletType, destTarget, err
	}
	return table, destKeyspace, destTabletType, destTarget, nil
}

func (vw *VSchemaWrapper) FindView(tab sqlparser.TableName) sqlparser.TableStatement {
	destKeyspace, _, _, err := topoproto.ParseDestination(tab.Qualifier.String(), topodatapb.TabletType_PRIMARY)
	if err != nil {
		return nil
	}
	return vw.V.FindView(destKeyspace, tab.Name.String())
}

func (vw *VSchemaWrapper) FindViewTarget(name sqlparser.TableName) (*vindexes.Keyspace, error) {
	destKeyspace, _, _, err := topoproto.ParseDestination(name.Qualifier.String(), topodatapb.TabletType_PRIMARY)
	if err != nil {
		return nil, err
	}
	if ks, ok := vw.V.Keyspaces[destKeyspace]; ok {
		return ks.Keyspace, nil
	}
	return nil, nil
}

func (vw *VSchemaWrapper) FindTableOrVindex(tab sqlparser.TableName) (*vindexes.BaseTable, vindexes.Vindex, string, topodatapb.TabletType, key.ShardDestination, error) {
	return vw.Vcursor.FindTableOrVindex(tab)
}

func (vw *VSchemaWrapper) SelectedKeyspace() (*vindexes.Keyspace, error) {
	return vw.AnyKeyspace()
}

func (vw *VSchemaWrapper) AnyKeyspace() (*vindexes.Keyspace, error) {
	ks, found := vw.V.Keyspaces["main"]
	if found {
		return ks.Keyspace, nil
	}

	size := len(vw.V.Keyspaces)
	if size == 0 {
		return nil, errors.New("no keyspace found in vschema")
	}

	// Find the first keyspace in the map alphabetically to get deterministic results
	keys := make([]string, size)
	for key := range vw.V.Keyspaces {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	return vw.V.Keyspaces[keys[0]].Keyspace, nil
}

func (vw *VSchemaWrapper) FirstSortedKeyspace() (*vindexes.Keyspace, error) {
	return vw.AnyKeyspace()
}

func (vw *VSchemaWrapper) TargetString() string {
	return "targetString"
}

func (vw *VSchemaWrapper) WarnUnshardedOnly(_ string, _ ...any) {
}

func (vw *VSchemaWrapper) ErrorIfShardedF(keyspace *vindexes.Keyspace, _, errFmt string, params ...any) error {
	if keyspace.Sharded {
		return fmt.Errorf(errFmt, params...)
	}
	return nil
}

func (vw *VSchemaWrapper) GetBindVars() map[string]*querypb.BindVariable {
	return nil
}

func (vw *VSchemaWrapper) CurrentDb() string {
	ksName := ""
	if vw.Keyspace != nil {
		ksName = vw.Keyspace.Name
	}
	return ksName
}

func (vw *VSchemaWrapper) FindRoutedShard(keyspace, shard string) (string, error) {
	return "", nil
}

func (vw *VSchemaWrapper) IsViewsEnabled() bool {
	return vw.EnableViews
}

// FindMirrorRule finds the mirror rule for the requested keyspace, table
// name, and the tablet type in the VSchema.
func (vw *VSchemaWrapper) FindMirrorRule(tab sqlparser.TableName) (*vindexes.MirrorRule, error) {
	destKeyspace, destTabletType, _, err := topoproto.ParseDestination(tab.Qualifier.String(), topodatapb.TabletType_PRIMARY)
	if err != nil {
		return nil, err
	}
	mirrorRule, err := vw.V.FindMirrorRule(destKeyspace, tab.Name.String(), destTabletType)
	if err != nil {
		return nil, err
	}
	return mirrorRule, err
}
