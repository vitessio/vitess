package plancontext

import (
	"context"
	"strings"

	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vtgate/engine"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/vt/key"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// PlannerVersion is an alias here to make the code more readable
type PlannerVersion = querypb.ExecuteOptions_PlannerVersion

// VSchema defines the interface for this package to fetch
// info about tables.
type VSchema interface {
	FindTable(tablename sqlparser.TableName) (*vindexes.Table, string, topodatapb.TabletType, key.Destination, error)
	FindView(name sqlparser.TableName) sqlparser.SelectStatement
	FindTableOrVindex(tablename sqlparser.TableName) (*vindexes.Table, vindexes.Vindex, string, topodatapb.TabletType, key.Destination, error)
	DefaultKeyspace() (*vindexes.Keyspace, error)
	TargetString() string
	Destination() key.Destination
	TabletType() topodatapb.TabletType
	TargetDestination(qualifier string) (key.Destination, *vindexes.Keyspace, topodatapb.TabletType, error)
	AnyKeyspace() (*vindexes.Keyspace, error)
	FirstSortedKeyspace() (*vindexes.Keyspace, error)
	SysVarSetEnabled() bool
	KeyspaceExists(keyspace string) bool
	AllKeyspace() ([]*vindexes.Keyspace, error)
	FindKeyspace(keyspace string) (*vindexes.Keyspace, error)
	GetSemTable() *semantics.SemTable
	Planner() PlannerVersion
	SetPlannerVersion(pv PlannerVersion)
	ConnCollation() collations.ID
	Environment() *vtenv.Environment

	// ErrorIfShardedF will return an error if the keyspace is sharded,
	// and produce a warning if the vtgate if configured to do so
	ErrorIfShardedF(keyspace *vindexes.Keyspace, warn, errFmt string, params ...any) error

	// WarnUnshardedOnly is used when a feature is only supported in unsharded mode.
	// This will let the user know that they are using something
	// that could become a problem if they move to a sharded keyspace
	WarnUnshardedOnly(format string, params ...any)

	// PlannerWarning records warning created during planning.
	PlannerWarning(message string)

	// ForeignKeyMode returns the foreign_key flag value
	ForeignKeyMode(keyspace string) (vschemapb.Keyspace_ForeignKeyMode, error)

	// KeyspaceError returns any error in the keyspace vschema.
	KeyspaceError(keyspace string) error

	GetForeignKeyChecksState() *bool

	// GetVSchema returns the latest cached vindexes.VSchema
	GetVSchema() *vindexes.VSchema

	// GetSrvVschema returns the latest cached vschema.SrvVSchema
	GetSrvVschema() *vschemapb.SrvVSchema

	// FindRoutedShard looks up shard routing rules for a shard
	FindRoutedShard(keyspace, shard string) (string, error)

	// IsShardRoutingEnabled returns true if partial shard routing is enabled
	IsShardRoutingEnabled() bool

	// IsViewsEnabled returns true if Vitess manages the views.
	IsViewsEnabled() bool

	// GetUDV returns user defined value from the variable passed.
	GetUDV(name string) *querypb.BindVariable

	// PlanPrepareStatement plans the prepared statement.
	PlanPrepareStatement(ctx context.Context, query string) (*engine.Plan, sqlparser.Statement, error)

	// ClearPrepareData clears the prepared data from the session.
	ClearPrepareData(stmtName string)

	// GetPrepareData returns the prepared data for the statement from the session.
	GetPrepareData(stmtName string) *vtgatepb.PrepareData

	// StorePrepareData stores the prepared data in the session.
	StorePrepareData(name string, v *vtgatepb.PrepareData)

	// FindMirrorRule returns the mirror rule associated with the requested
	// keyspace.
	FindMirrorRule(keyspace string) (*vindexes.MirrorRule, error)

	// HasMirroRules returns true if the vschema has any mirror rules.
	HasMirrorRules() bool
}

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
	fromTable, destKeyspace, destTabletType, dest, err := m.vschema.FindTable(tablename)
	if err != nil {
		return nil, "", destTabletType, nil, err
	}
	mirrorRule, err := m.vschema.FindMirrorRule(fromTable.Keyspace.Name)
	if err != nil {
		return nil, "", destTabletType, nil, err
	}
	if mirrorRule == nil {
		return nil, "", destTabletType, nil, nil
	}
	toTable, err := m.GetVSchema().FindTable(mirrorRule.ToKeyspace, tablename.Name.String())
	return toTable, destKeyspace, destTabletType, dest, err
}

func (m *mirrorVSchema) FindView(name sqlparser.TableName) sqlparser.SelectStatement {
	// TODO(maxeng): we don't have all the information we need here to mirror
	// views. May need to expose a new method on VSchema interface, such as
	// ParseDestinationTarget
	return nil
}

// FindTableOrVindex returns a routed table or vindex.
//
// mirrorVSchema returns a mirrored version of the value returned by the underlying VSchema.
// If the underlying VSchema returns a table t1 in ks1, and there is exists a
// routing rule from ks1 to ks2, and ks2 has a table t1, then mirrorVSchema
// returns ks2.t1.
//
// If no table with the requested name can be found, or not mirror rule is
// defined on the keyspace of the found table, or no table is found in the
// target keyspace defined by the mirror rule, then nil is returned.
func (m *mirrorVSchema) FindTableOrVindex(tablename sqlparser.TableName) (*vindexes.Table, vindexes.Vindex, string, topodatapb.TabletType, key.Destination, error) {
	fromTable, _, destKeyspace, destTabletType, dest, err := m.vschema.FindTableOrVindex(tablename)
	if err != nil {
		return nil, nil, "", destTabletType, nil, err
	}
	mirrorRule, err := m.vschema.FindMirrorRule(fromTable.Keyspace.Name)
	if err != nil {
		return nil, nil, "", destTabletType, nil, err
	}
	if mirrorRule == nil {
		return nil, nil, "", destTabletType, nil, nil
	}
	toTable, toVindex, err := m.GetVSchema().FindTableOrVindex(mirrorRule.ToKeyspace, tablename.Name.String(), m.TabletType())
	return toTable, toVindex, destKeyspace, destTabletType, dest, err
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

// FindMirrorRule returns the mirror rule associated with the requested
// keyspace.
//
// The returned VSchema cannot be reflected back again by passing it to
// ForMirroring. This restriction allows the returned VSchema to be passed to a
// recursive planning calls without creating an infinite loop.
func (m *mirrorVSchema) FindMirrorRule(keyspace string) (*vindexes.MirrorRule, error) {
	return nil, nil
}

// HasMirrorRules returns true if the vschema has any mirror rules.
//
// The returned VSchema cannot be reflected back again by passing it to
// ForMirroring. This restriction allows the returned VSchema to be passed to a
// recursive planning calls without creating an infinite loop.
func (m *mirrorVSchema) HasMirrorRules() bool {
	return false
}

// PlannerNameToVersion returns the numerical representation of the planner
func PlannerNameToVersion(s string) (PlannerVersion, bool) {
	switch strings.ToLower(s) {
	case "gen4":
		return querypb.ExecuteOptions_Gen4, true
	case "gen4greedy", "greedy":
		return querypb.ExecuteOptions_Gen4Greedy, true
	case "left2right":
		return querypb.ExecuteOptions_Gen4Left2Right, true
	}
	return 0, false
}

// ForMirroring returns a wrapper which returns mirrored versions of values
// return by the wrapped vschema.
//
// For example, if the underlying VSchema defines any mirror rules from ks1 to
// ks2, calls to FindTable for which the wrapped VSchema returns tables in
// ks1 will return tables in ks2.
//
// The returned VSchema cannot be reflected back again by passing it to
// ForMirroring. This restriction allows the returned VSchema to be passed in
// recursive planning calls without creating an infinite loop.
func ForMirroring(vschema VSchema) VSchema {
	return &mirrorVSchema{vschema}
}
