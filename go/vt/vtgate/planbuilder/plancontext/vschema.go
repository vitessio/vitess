package plancontext

import (
	"strings"

	vschemapb "vitess.io/vitess/go/vt/proto/vschema"

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
	ForeignKeyMode() string

	// GetVSchema returns the latest cached vindexes.VSchema
	GetVSchema() *vindexes.VSchema

	// GetSrvVschema returns the latest cached vschema.SrvVSchema
	GetSrvVschema() *vschemapb.SrvVSchema
}

// PlannerNameToVersion returns the numerical representation of the planner
func PlannerNameToVersion(s string) (PlannerVersion, bool) {
	switch strings.ToLower(s) {
	case "v3":
		return querypb.ExecuteOptions_V3, true
	case "gen4":
		return querypb.ExecuteOptions_Gen4, true
	case "gen4greedy", "greedy":
		return querypb.ExecuteOptions_Gen4Greedy, true
	case "left2right":
		return querypb.ExecuteOptions_Gen4Left2Right, true
	case "gen4fallback":
		return querypb.ExecuteOptions_Gen4WithFallback, true
	case "gen4comparev3":
		return querypb.ExecuteOptions_Gen4CompareV3, true
	}
	return 0, false
}
