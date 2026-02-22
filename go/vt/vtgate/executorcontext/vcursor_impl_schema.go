/*
Copyright 2026 The Vitess Authors.

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

package executorcontext

import (
	"fmt"
	"sort"

	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/vt/key"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	topoprotopb "vitess.io/vitess/go/vt/topo/topoproto"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// ParseDestinationTarget parses the target string and returns the keyspace, tablet type, destination, and tablet alias.
func ParseDestinationTarget(targetString string, tablet topodatapb.TabletType, vschema *vindexes.VSchema) (string, topodatapb.TabletType, key.ShardDestination, *topodatapb.TabletAlias, error) {
	destKeyspace, destTabletType, dest, tabletAlias, err := topoprotopb.ParseDestination(targetString, tablet)
	// If the keyspace is not specified, and there is only one keyspace in the VSchema, use that.
	if destKeyspace == "" && len(vschema.Keyspaces) == 1 {
		for k := range vschema.Keyspaces {
			destKeyspace = k
		}
	}
	return destKeyspace, destTabletType, dest, tabletAlias, err
}

// FindTable finds the specified table. If the keyspace what specified in the input, it gets used as qualifier.
// Otherwise, the keyspace from the request is used, if one was provided.
func (vc *VCursorImpl) FindTable(name sqlparser.TableName) (*vindexes.BaseTable, string, topodatapb.TabletType, key.ShardDestination, error) {
	destKeyspace, destTabletType, dest, err := vc.parseDestinationTarget(name.Qualifier.String())
	if err != nil {
		return nil, "", destTabletType, nil, err
	}
	if destKeyspace == "" {
		destKeyspace = vc.keyspace
	}
	table, err := vc.vschema.FindTable(destKeyspace, name.Name.String())
	if err != nil {
		return nil, "", destTabletType, nil, err
	}
	return table, destKeyspace, destTabletType, dest, err
}

func (vc *VCursorImpl) FindView(name sqlparser.TableName) (sqlparser.TableStatement, *sqlparser.TableName) {
	ks, _, _, err := vc.parseDestinationTarget(name.Qualifier.String())
	if err != nil {
		return nil, nil
	}
	if ks == "" {
		ks = vc.keyspace
	}
	return vc.vschema.FindRoutedView(ks, name.Name.String(), vc.tabletType)
}

func (vc *VCursorImpl) FindRoutedTable(name sqlparser.TableName) (*vindexes.BaseTable, error) {
	destKeyspace, destTabletType, _, err := vc.parseDestinationTarget(name.Qualifier.String())
	if err != nil {
		return nil, err
	}
	if destKeyspace == "" {
		destKeyspace = vc.keyspace
	}

	table, err := vc.vschema.FindRoutedTable(destKeyspace, name.Name.String(), destTabletType)
	if err != nil {
		return nil, err
	}

	return table, nil
}

// FindTableOrVindex finds the specified table or vindex.
func (vc *VCursorImpl) FindTableOrVindex(name sqlparser.TableName) (*vindexes.BaseTable, vindexes.Vindex, string, topodatapb.TabletType, key.ShardDestination, error) {
	if name.Qualifier.IsEmpty() && name.Name.String() == "dual" {
		// The magical MySQL dual table should only be resolved
		// when it is not qualified by a database name.
		return vc.getDualTable()
	}

	destKeyspace, destTabletType, dest, err := vc.parseDestinationTarget(name.Qualifier.String())
	if err != nil {
		return nil, nil, "", destTabletType, nil, err
	}
	if destKeyspace == "" {
		destKeyspace = vc.getActualKeyspace()
	}
	table, vindex, err := vc.vschema.FindTableOrVindex(destKeyspace, name.Name.String(), vc.tabletType)
	if err != nil {
		return nil, nil, "", destTabletType, nil, err
	}
	return table, vindex, destKeyspace, destTabletType, dest, nil
}

// FindViewTarget finds the specified view's target keyspace.
func (vc *VCursorImpl) FindViewTarget(name sqlparser.TableName) (*vindexes.Keyspace, error) {
	destKeyspace, _, _, err := vc.parseDestinationTarget(name.Qualifier.String())
	if err != nil {
		return nil, err
	}
	if destKeyspace != "" {
		return vc.FindKeyspace(destKeyspace)
	}

	tbl, err := vc.vschema.FindRoutedTable("", name.Name.String(), vc.tabletType)
	if err != nil || tbl == nil {
		return nil, err
	}
	return tbl.Keyspace, nil
}

func (vc *VCursorImpl) parseDestinationTarget(targetString string) (string, topodatapb.TabletType, key.ShardDestination, error) {
	keyspace, tabletType, dest, _, err := ParseDestinationTarget(targetString, vc.tabletType, vc.vschema)
	return keyspace, tabletType, dest, err
}

func (vc *VCursorImpl) getDualTable() (*vindexes.BaseTable, vindexes.Vindex, string, topodatapb.TabletType, key.ShardDestination, error) {
	ksName := vc.getActualKeyspace()
	var ks *vindexes.Keyspace
	if ksName == "" {
		ks = vc.vschema.FirstKeyspace()
		ksName = ks.Name
	} else {
		ks = vc.vschema.Keyspaces[ksName].Keyspace
	}
	tbl := &vindexes.BaseTable{
		Name:     sqlparser.NewIdentifierCS("dual"),
		Keyspace: ks,
		Type:     vindexes.TypeReference,
	}
	return tbl, nil, ksName, topodatapb.TabletType_PRIMARY, nil, nil
}

func (vc *VCursorImpl) getActualKeyspace() string {
	if !sqlparser.SystemSchema(vc.keyspace) {
		return vc.keyspace
	}
	ks, err := vc.AnyKeyspace()
	if err != nil {
		return ""
	}
	return ks.Name
}

// SelectedKeyspace returns the selected keyspace of the current request
// if there is one. If the keyspace specified in the target cannot be
// identified, it returns an error.
func (vc *VCursorImpl) SelectedKeyspace() (*vindexes.Keyspace, error) {
	if ignoreKeyspace(vc.keyspace) {
		return nil, ErrNoKeyspace
	}
	ks, ok := vc.vschema.Keyspaces[vc.keyspace]
	if !ok {
		return nil, vterrors.VT05003(vc.keyspace)
	}
	return ks.Keyspace, nil
}

var errNoDbAvailable = vterrors.NewErrorf(vtrpcpb.Code_FAILED_PRECONDITION, vterrors.NoDB, "no database available")

func (vc *VCursorImpl) AnyKeyspace() (*vindexes.Keyspace, error) {
	keyspace, err := vc.SelectedKeyspace()
	if err == nil {
		return keyspace, nil
	}
	if err != ErrNoKeyspace {
		return nil, err
	}

	if len(vc.vschema.Keyspaces) == 0 {
		return nil, errNoDbAvailable
	}

	keyspaces := vc.getSortedServingKeyspaces()

	// Look for any sharded keyspace if present, otherwise take the first keyspace,
	// sorted alphabetically
	for _, ks := range keyspaces {
		if ks.Sharded {
			return ks, nil
		}
	}
	return keyspaces[0], nil
}

// getSortedServingKeyspaces gets the sorted serving keyspaces
func (vc *VCursorImpl) getSortedServingKeyspaces() []*vindexes.Keyspace {
	var keyspaces []*vindexes.Keyspace

	if vc.resolver != nil && vc.resolver.GetGateway() != nil {
		keyspaceNames := vc.resolver.GetGateway().GetServingKeyspaces()
		for _, ksName := range keyspaceNames {
			ks, exists := vc.vschema.Keyspaces[ksName]
			if exists {
				keyspaces = append(keyspaces, ks.Keyspace)
			}
		}
	}

	if len(keyspaces) == 0 {
		for _, ks := range vc.vschema.Keyspaces {
			keyspaces = append(keyspaces, ks.Keyspace)
		}
	}
	sort.Slice(keyspaces, func(i, j int) bool {
		return keyspaces[i].Name < keyspaces[j].Name
	})
	return keyspaces
}

func (vc *VCursorImpl) FirstSortedKeyspace() (*vindexes.Keyspace, error) {
	if len(vc.vschema.Keyspaces) == 0 {
		return nil, errNoDbAvailable
	}
	keyspaces := vc.getSortedServingKeyspaces()

	return keyspaces[0], nil
}

// SysVarSetEnabled implements the ContextVSchema interface
func (vc *VCursorImpl) SysVarSetEnabled() bool {
	return vc.GetSessionEnableSystemSettings()
}

// KeyspaceExists provides whether the keyspace exists or not.
func (vc *VCursorImpl) KeyspaceExists(ks string) bool {
	return vc.vschema.Keyspaces[ks] != nil
}

// AllKeyspace implements the ContextVSchema interface
func (vc *VCursorImpl) AllKeyspace() ([]*vindexes.Keyspace, error) {
	if len(vc.vschema.Keyspaces) == 0 {
		return nil, errNoDbAvailable
	}
	var kss []*vindexes.Keyspace
	for _, ks := range vc.vschema.Keyspaces {
		kss = append(kss, ks.Keyspace)
	}
	return kss, nil
}

// FindKeyspace implements the VSchema interface
func (vc *VCursorImpl) FindKeyspace(keyspace string) (*vindexes.Keyspace, error) {
	if len(vc.vschema.Keyspaces) == 0 {
		return nil, errNoDbAvailable
	}
	for _, ks := range vc.vschema.Keyspaces {
		if ks.Keyspace.Name == keyspace {
			return ks.Keyspace, nil
		}
	}
	return nil, nil
}

// Planner implements the ContextVSchema interface
func (vc *VCursorImpl) Planner() plancontext.PlannerVersion {
	if vc.SafeSession.Options != nil &&
		vc.SafeSession.Options.PlannerVersion != querypb.ExecuteOptions_DEFAULT_PLANNER {
		return vc.SafeSession.Options.PlannerVersion
	}
	return vc.config.PlannerVersion
}

// GetSemTable implements the ContextVSchema interface
func (vc *VCursorImpl) GetSemTable() *semantics.SemTable {
	return vc.semTable
}

// IsShardRoutingEnabled implements the VCursor interface.
func (vc *VCursorImpl) IsShardRoutingEnabled() bool {
	return vc.config.EnableShardRouting
}

// KeyspaceAvailable implements the VCursor interface
func (vc *VCursorImpl) KeyspaceAvailable(ks string) bool {
	_, exists := vc.executor.VSchema().Keyspaces[ks]
	return exists
}

// ErrorIfShardedF implements the VCursor interface
func (vc *VCursorImpl) ErrorIfShardedF(ks *vindexes.Keyspace, warn, errFormat string, params ...any) error {
	if ks.Sharded {
		return vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, errFormat, params...)
	}
	vc.WarnUnshardedOnly("'%s' not supported in sharded mode", warn)

	return nil
}

// WarnUnshardedOnly implements the VCursor interface
func (vc *VCursorImpl) WarnUnshardedOnly(format string, params ...any) {
	if vc.config.WarnShardedOnly {
		vc.warnings = append(vc.warnings, &querypb.QueryWarning{
			Code:    uint32(sqlerror.ERNotSupportedYet),
			Message: fmt.Sprintf(format, params...),
		})
		vc.executor.AddWarningCount("WarnUnshardedOnly", 1)
	}
}

// PlannerWarning implements the VCursor interface
func (vc *VCursorImpl) PlannerWarning(message string) {
	if message == "" {
		return
	}
	vc.warnings = append(vc.warnings, &querypb.QueryWarning{
		Code:    uint32(sqlerror.ERNotSupportedYet),
		Message: message,
	})
}

func (vc *VCursorImpl) GetAndEmptyWarnings() []*querypb.QueryWarning {
	w := vc.warnings
	vc.warnings = nil
	return w
}

// ForeignKeyMode implements the VCursor interface
func (vc *VCursorImpl) ForeignKeyMode(keyspace string) (vschemapb.Keyspace_ForeignKeyMode, error) {
	if vc.config.ForeignKeyMode == vschemapb.Keyspace_disallow {
		return vschemapb.Keyspace_disallow, nil
	}
	ks := vc.vschema.Keyspaces[keyspace]
	if ks == nil {
		return 0, vterrors.VT14004(keyspace)
	}
	return ks.ForeignKeyMode, nil
}

func (vc *VCursorImpl) KeyspaceError(keyspace string) error {
	ks := vc.vschema.Keyspaces[keyspace]
	if ks == nil {
		return vterrors.VT14004(keyspace)
	}
	return ks.Error
}

func (vc *VCursorImpl) GetAggregateUDFs() []string {
	return vc.vschema.GetAggregateUDFs()
}

// FindMirrorRule finds the mirror rule for the requested table name and
// VSchema tablet type.
func (vc *VCursorImpl) FindMirrorRule(name sqlparser.TableName) (*vindexes.MirrorRule, error) {
	destKeyspace, destTabletType, _, err := vc.parseDestinationTarget(name.Qualifier.String())
	if err != nil {
		return nil, err
	}
	if destKeyspace == "" {
		destKeyspace = vc.keyspace
	}
	mirrorRule, err := vc.vschema.FindMirrorRule(destKeyspace, name.Name.String(), destTabletType)
	if err != nil {
		return nil, err
	}
	return mirrorRule, err
}

func (vc *VCursorImpl) FindRoutedShard(keyspace, shard string) (keyspaceName string, err error) {
	return vc.vschema.FindRoutedShard(keyspace, shard)
}

func (vc *VCursorImpl) IsViewsEnabled() bool {
	return vc.config.EnableViews
}

func (vc *VCursorImpl) GetVSchema() *vindexes.VSchema {
	return vc.vschema
}

func (vc *VCursorImpl) GetSrvVschema() *vschemapb.SrvVSchema {
	return vc.vm.GetCurrentSrvVschema()
}
