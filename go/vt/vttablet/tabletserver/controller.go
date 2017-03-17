// Copyright 2017, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/vttablet/queryservice"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/rules"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/schema"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// Controller defines the control interface for TabletServer.
type Controller interface {
	// Register registers this query service with the RPC layer.
	Register()
	// AddStatusPart adds the status part to the status page
	AddStatusPart()

	// InitDBConfig sets up the db config vars.
	InitDBConfig(querypb.Target, dbconfigs.DBConfigs, mysqlctl.MysqlDaemon) error

	// SetServingType transitions the query service to the required serving type.
	// Returns true if the state of QueryService or the tablet type changed.
	SetServingType(tabletType topodatapb.TabletType, serving bool, alsoAllow []topodatapb.TabletType) (bool, error)

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

	// SchemaEngine returns the SchemaEngine object used by this Controller
	SchemaEngine() *schema.Engine

	// BroadcastHealth sends the current health to all listeners
	BroadcastHealth(terTimestamp int64, stats *querypb.RealtimeStats)
}

// Ensure TabletServer satisfies Controller interface.
var _ Controller = (*TabletServer)(nil)
