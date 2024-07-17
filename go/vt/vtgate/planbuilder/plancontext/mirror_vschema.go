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

package plancontext

import (
	"context"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/vt/key"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// mirrorVSchema is a wrapper which returns mirrored versions of values
// return by the wrapped vschema.
//
// For example, if the wrapped VSchema defines any mirror rules from ks1 to
// ks2, calls to FindTable for which the wrapped VSchema returns tables in
// ks1 will return tables in ks2.
//
// The returned VSchema cannot be reflected back again by passing it to
// ForMirroring. This restriction allows the returned VSchema to be used in
// recursive planning calls without creating an infinite loop.
type mirrorVSchema struct {
	vschema VSchema
}

// FindTable finds and returns the table with the requested name.
//
// mirrorVSchema returns a mirrored version of the value returned by the underlying VSchema.
// If the underlying VSchema returns a table t1 in ks1, and there is exists a
// routing rule from ks1 to ks2, and ks2 has a table t1, then mirrorVSchema
// returns ks2.t1.
//
// If no table with the requested name can be found, or not mirror rule is
// defined on the keyspace of the found table, or no table is found in the
// target keyspace defined by the mirror rule, then nil is returned.
func (m *mirrorVSchema) FindTable(tablename sqlparser.TableName) (*vindexes.Table, string, topodatapb.TabletType, key.Destination, error) {
	mirrorRule, destKeyspace, destTabletType, dest, err := m.vschema.FindMirrorRule(tablename)
	if err != nil {
		return nil, "", destTabletType, nil, err
	}
	return mirrorRule.Table, destKeyspace, destTabletType, dest, err
}

func (m *mirrorVSchema) FindView(name sqlparser.TableName) sqlparser.SelectStatement {
	// TODO(maxeng): we don't have all the information we need here to mirror
	// views. May need to expose a new method on VSchema interface, such as
	// ParseDestinationTarget
	return nil
}

// FindTableOrVindex returns a routed table or vindex.
//
// mirrorVSchema returns a mirrored version of the value returned by the
// underlying VSchema. If the underlying VSchema returns a table t1 in ks1,
// and there is exists a mirror rule from ks1.t1 to ks2.t1, then mirrorVSchema
// returns ks2.t1. Vindexes are not mirrored.
func (m *mirrorVSchema) FindTableOrVindex(tablename sqlparser.TableName) (*vindexes.Table, vindexes.Vindex, string, topodatapb.TabletType, key.Destination, error) {
	fromTable, vindex, destKeyspace, destTabletType, dest, err := m.vschema.FindTableOrVindex(tablename)
	if err != nil {
		return nil, vindex, "", destTabletType, nil, err
	}
	mirrorRule, err := m.GetVSchema().FindMirrorRule(fromTable.Keyspace.Name, fromTable.Name.String(), destTabletType)
	if err != nil {
		return nil, vindex, "", destTabletType, nil, err
	}
	// If mirror rule not found, just use the table we initially found.
	if mirrorRule == nil {
		return fromTable, vindex, destKeyspace, destTabletType, dest, nil
	}
	return mirrorRule.Table, vindex, destKeyspace, destTabletType, dest, err
}

func (m *mirrorVSchema) DefaultKeyspace() (*vindexes.Keyspace, error) {
	return m.vschema.DefaultKeyspace()
}

func (m *mirrorVSchema) TargetString() string {
	return m.vschema.TargetString()
}

func (m *mirrorVSchema) Destination() key.Destination {
	return m.vschema.Destination()
}

func (m *mirrorVSchema) TabletType() topodatapb.TabletType {
	return m.vschema.TabletType()
}

func (m *mirrorVSchema) TargetDestination(qualifier string) (key.Destination, *vindexes.Keyspace, topodatapb.TabletType, error) {
	return m.vschema.TargetDestination(qualifier)
}

func (m *mirrorVSchema) AnyKeyspace() (*vindexes.Keyspace, error) {
	return m.vschema.AnyKeyspace()
}

func (m *mirrorVSchema) FirstSortedKeyspace() (*vindexes.Keyspace, error) {
	return m.vschema.FirstSortedKeyspace()
}

func (m *mirrorVSchema) SysVarSetEnabled() bool {
	return m.vschema.SysVarSetEnabled()
}

func (m *mirrorVSchema) KeyspaceExists(keyspace string) bool {
	return m.vschema.KeyspaceExists(keyspace)
}

func (m *mirrorVSchema) AllKeyspace() ([]*vindexes.Keyspace, error) {
	return m.vschema.AllKeyspace()
}

func (m *mirrorVSchema) FindKeyspace(keyspace string) (*vindexes.Keyspace, error) {
	return m.vschema.FindKeyspace(keyspace)
}

func (m *mirrorVSchema) GetSemTable() *semantics.SemTable {
	return m.vschema.GetSemTable()
}

func (m *mirrorVSchema) Planner() PlannerVersion {
	return m.vschema.Planner()
}

func (m *mirrorVSchema) SetPlannerVersion(pv PlannerVersion) {
	m.vschema.SetPlannerVersion(pv)
}

func (m *mirrorVSchema) ConnCollation() collations.ID {
	return m.vschema.ConnCollation()
}

func (m *mirrorVSchema) Environment() *vtenv.Environment {
	return m.vschema.Environment()
}

// ErrorIfShardedF will return an error if the keyspace is sharded,
// and produce a warning if the vtgate if configured to do so
func (m *mirrorVSchema) ErrorIfShardedF(keyspace *vindexes.Keyspace, warn string, errFmt string, params ...any) error {
	return m.vschema.ErrorIfShardedF(keyspace, warn, errFmt, params...)
}

// WarnUnshardedOnly is used when a feature is only supported in unsharded mode.
// This will let the user know that they are using something
// that could become a problem if they move to a sharded keyspace
func (m *mirrorVSchema) WarnUnshardedOnly(format string, params ...any) {
	m.vschema.WarnUnshardedOnly(format, params...)
}

// PlannerWarning records warning created during planning.
func (m *mirrorVSchema) PlannerWarning(message string) {
	m.vschema.PlannerWarning(message)
}

// ForeignKeyMode returns the foreign_key flag value
func (m *mirrorVSchema) ForeignKeyMode(keyspace string) (vschemapb.Keyspace_ForeignKeyMode, error) {
	return m.vschema.ForeignKeyMode(keyspace)
}

// KeyspaceError returns any error in the keyspace vschema.
func (m *mirrorVSchema) KeyspaceError(keyspace string) error {
	return m.vschema.KeyspaceError(keyspace)
}

func (m *mirrorVSchema) GetForeignKeyChecksState() *bool {
	return m.vschema.GetForeignKeyChecksState()
}

// GetVSchema returns the latest cached vindexes.VSchema
func (m *mirrorVSchema) GetVSchema() *vindexes.VSchema {
	return m.vschema.GetVSchema()
}

func (m *mirrorVSchema) GetSrvVschema() *vschemapb.SrvVSchema {
	return m.vschema.GetSrvVschema()
}

// FindRoutedShard looks up shard routing rules for a shard
func (m *mirrorVSchema) FindRoutedShard(keyspace string, shard string) (string, error) {
	return m.vschema.FindRoutedShard(keyspace, shard)
}

// IsShardRoutingEnabled returns true if partial shard routing is enabled
func (m *mirrorVSchema) IsShardRoutingEnabled() bool {
	return m.vschema.IsShardRoutingEnabled()
}

// IsViewsEnabled returns true if Vitess manages the views.
func (m *mirrorVSchema) IsViewsEnabled() bool {
	return m.vschema.IsViewsEnabled()
}

// GetUDV returns user defined value from the variable passed.
func (m *mirrorVSchema) GetUDV(name string) *querypb.BindVariable {
	return m.vschema.GetUDV(name)
}

// PlanPrepareStatement plans the prepared statement.
func (m *mirrorVSchema) PlanPrepareStatement(ctx context.Context, query string) (*engine.Plan, sqlparser.Statement, error) {
	return m.vschema.PlanPrepareStatement(ctx, query)
}

// ClearPrepareData clears the prepared data from the session.
func (m *mirrorVSchema) ClearPrepareData(stmtName string) {
	m.vschema.ClearPrepareData(stmtName)
}

// GetPrepareData returns the prepared data for the statement from the session.
func (m *mirrorVSchema) GetPrepareData(stmtName string) *vtgatepb.PrepareData {
	return m.vschema.GetPrepareData(stmtName)
}

// StorePrepareData stores the prepared data in the session.
func (m *mirrorVSchema) StorePrepareData(name string, v *vtgatepb.PrepareData) {
	m.vschema.StorePrepareData(name, v)
}

// GetAggregateUDFs returns the list of aggregate UDFs.
func (m *mirrorVSchema) GetAggregateUDFs() []string {
	return m.vschema.GetAggregateUDFs()
}

// FindMirrorRule finds the mirror rule for the requested table name and
// VSchema tablet type.
func (m *mirrorVSchema) FindMirrorRule(name sqlparser.TableName) (*vindexes.MirrorRule, string, topodatapb.TabletType, key.Destination, error) {
	return nil, "", topodatapb.TabletType_UNKNOWN, nil, vterrors.Errorf(vtrpc.Code_INTERNAL, "[BUG] refusing to perform chained traffic mirroring")
}

// MirrorVSchema returns a wrapper which returns mirrored versions of values
// return by the wrapped vschema.
//
// For example, if the underlying VSchema defines any mirror rules from ks1 to
// ks2, calls to FindTable for which the wrapped VSchema returns tables in
// ks1 will return tables in ks2.
//
// The returned VSchema cannot be reflected back again by passing it to
// MirrorVSchema. This restriction prevents infinite mirroring loops.
func MirrorVSchema(vschema VSchema) VSchema {
	return &mirrorVSchema{vschema}
}
