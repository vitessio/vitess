/*
Copyright 2017 Google Inc.

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

package tabletserver

import (
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vttablet/queryservice"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/rules"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/schema"

	"time"

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
	InitDBConfig(querypb.Target, dbconfigs.DBConfigs) error

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

	// HeartbeatLag returns the current lag as calculated by the heartbeat
	// package, if heartbeat is enabled. Otherwise returns 0.
	HeartbeatLag() (time.Duration, error)

	// TopoServer returns the topo server.
	TopoServer() *topo.Server
}

// Ensure TabletServer satisfies Controller interface.
var _ Controller = (*TabletServer)(nil)
