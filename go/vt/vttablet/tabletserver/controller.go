package tabletserver

import (
	"context"

	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vttablet/queryservice"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/rules"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
	"vitess.io/vitess/go/vt/vttablet/vexec"

	"time"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// Controller defines the control interface for TabletServer.
type Controller interface {
	// Register registers this query service with the RPC layer.
	Register()

	// AddStatusHeader adds the header part to the status page.
	AddStatusHeader()

	// AddStatusHeader adds the status part to the status page
	AddStatusPart()

	// Stats returns stats vars.
	Stats() *tabletenv.Stats

	// InitDBConfig sets up the db config vars.
	InitDBConfig(target *querypb.Target, dbConfigs *dbconfigs.DBConfigs, mysqlDaemon mysqlctl.MysqlDaemon) error

	// SetServingType transitions the query service to the required serving type.
	// Returns true if the state of QueryService or the tablet type changed.
	SetServingType(tabletType topodatapb.TabletType, terTimestamp time.Time, serving bool, reason string) error

	// EnterLameduck causes tabletserver to enter the lameduck state.
	EnterLameduck()

	// IsServing returns true if the query service is running
	IsServing() bool

	// IsHealthy returns the health status of the QueryService
	IsHealthy() error

	// ClearQueryPlanCache clears internal query plan cache
	ClearQueryPlanCache()

	// ReloadSchema makes the quey service reload its schema cache
	ReloadSchema(ctx context.Context) error

	// RegisterQueryRuleSource adds a query rule source
	RegisterQueryRuleSource(ruleSource string)

	// RegisterQueryRuleSource removes a query rule source
	UnRegisterQueryRuleSource(ruleSource string)

	// SetQueryRules sets the query rules for this QueryService
	SetQueryRules(ruleSource string, qrs *rules.Rules) error

	// QueryService returns the QueryService object used by this Controller
	QueryService() queryservice.QueryService

	// OnlineDDLExecutor the online DDL executor used by this Controller
	OnlineDDLExecutor() vexec.Executor

	// SchemaEngine returns the SchemaEngine object used by this Controller
	SchemaEngine() *schema.Engine

	// BroadcastHealth sends the current health to all listeners
	BroadcastHealth()

	// TopoServer returns the topo server.
	TopoServer() *topo.Server
}

// Ensure TabletServer satisfies Controller interface.
var _ Controller = (*TabletServer)(nil)
