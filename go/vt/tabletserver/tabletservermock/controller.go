// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package tabletservermock provides mock interfaces for tabletserver.
package tabletservermock

import (
	"sync"

	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	"github.com/youtube/vitess/go/vt/tabletserver"
	"github.com/youtube/vitess/go/vt/tabletserver/queryservice"
)

// BroadcastData is used by the mock Controller to send data
// so the tests can check what was sent.
type BroadcastData struct {
	// TERTimestamp stores the last broadcast timestamp.
	TERTimestamp int64

	// RealtimeStats stores the last broadcast stats.
	RealtimeStats querypb.RealtimeStats

	// Serving contains the QueryServiceEnabled flag
	Serving bool
}

// StateChange stores the state the controller changed to.
// Tests can use this to verify that the state changed as expected.
type StateChange struct {
	// Serving is true when the QueryService is enabled.
	Serving bool
	// TabletType is the type of tablet e.g. REPLICA.
	TabletType topodatapb.TabletType
}

// Controller is a mock tabletserver.Controller
type Controller struct {
	// mu protects the fields in this structure
	mu sync.Mutex

	// CurrentTarget stores the last known target
	CurrentTarget querypb.Target

	// QueryServiceEnabled is a state variable
	QueryServiceEnabled bool

	// IsInLameduck is a state variable
	IsInLameduck bool

	// SetServingTypeError is the return value for SetServingType
	SetServingTypeError error

	// IsHealthy is the return value for IsHealthy
	IsHealthyError error

	// ReloadSchemaCount counts how many times ReloadSchema was called
	ReloadSchemaCount int

	// BroadcastData is a channel where we send BroadcastHealth data
	BroadcastData chan *BroadcastData

	// StateChanges has the list of state changes done by SetServingType().
	StateChanges chan *StateChange
}

// NewController returns a mock of tabletserver.Controller
func NewController() *Controller {
	return &Controller{
		QueryServiceEnabled: false,
		IsHealthyError:      nil,
		ReloadSchemaCount:   0,
		BroadcastData:       make(chan *BroadcastData, 10),
		StateChanges:        make(chan *StateChange, 10),
	}
}

// Register is part of the tabletserver.Controller interface
func (tqsc *Controller) Register() {
}

// AddStatusPart is part of the tabletserver.Controller interface
func (tqsc *Controller) AddStatusPart() {
}

// InitDBConfig is part of the tabletserver.Controller interface
func (tqsc *Controller) InitDBConfig(target querypb.Target, dbConfigs dbconfigs.DBConfigs, mysqld mysqlctl.MysqlDaemon) error {
	tqsc.mu.Lock()
	defer tqsc.mu.Unlock()

	tqsc.CurrentTarget = target
	return nil
}

// SetServingType is part of the tabletserver.Controller interface
func (tqsc *Controller) SetServingType(tabletType topodatapb.TabletType, serving bool, alsoAllow []topodatapb.TabletType) (bool, error) {
	tqsc.mu.Lock()
	defer tqsc.mu.Unlock()

	stateChanged := false
	if tqsc.SetServingTypeError == nil {
		stateChanged = tqsc.QueryServiceEnabled != serving || tqsc.CurrentTarget.TabletType != tabletType
		tqsc.CurrentTarget.TabletType = tabletType
		tqsc.QueryServiceEnabled = serving
	}
	if stateChanged {
		tqsc.StateChanges <- &StateChange{
			Serving:    serving,
			TabletType: tabletType,
		}
	}
	tqsc.IsInLameduck = false
	return stateChanged, tqsc.SetServingTypeError
}

// IsServing is part of the tabletserver.Controller interface
func (tqsc *Controller) IsServing() bool {
	tqsc.mu.Lock()
	defer tqsc.mu.Unlock()

	return tqsc.QueryServiceEnabled
}

// IsHealthy is part of the tabletserver.Controller interface
func (tqsc *Controller) IsHealthy() error {
	tqsc.mu.Lock()
	defer tqsc.mu.Unlock()

	return tqsc.IsHealthyError
}

// ReloadSchema is part of the tabletserver.Controller interface
func (tqsc *Controller) ReloadSchema() {
	tqsc.mu.Lock()
	defer tqsc.mu.Unlock()

	tqsc.ReloadSchemaCount++
}

//ClearQueryPlanCache is part of the tabletserver.Controller interface
func (tqsc *Controller) ClearQueryPlanCache() {
}

// RegisterQueryRuleSource is part of the tabletserver.Controller interface
func (tqsc *Controller) RegisterQueryRuleSource(ruleSource string) {
}

// UnRegisterQueryRuleSource is part of the tabletserver.Controller interface
func (tqsc *Controller) UnRegisterQueryRuleSource(ruleSource string) {
}

// SetQueryRules is part of the tabletserver.Controller interface
func (tqsc *Controller) SetQueryRules(ruleSource string, qrs *tabletserver.QueryRules) error {
	return nil
}

// QueryService is part of the tabletserver.Controller interface
func (tqsc *Controller) QueryService() queryservice.QueryService {
	return nil
}

// QueryServiceStats is part of the tabletserver.Controller interface
func (tqsc *Controller) QueryServiceStats() *tabletserver.QueryServiceStats {
	return nil
}

// BroadcastHealth is part of the tabletserver.Controller interface
func (tqsc *Controller) BroadcastHealth(terTimestamp int64, stats *querypb.RealtimeStats) {
	tqsc.mu.Lock()
	defer tqsc.mu.Unlock()

	tqsc.BroadcastData <- &BroadcastData{
		TERTimestamp:  terTimestamp,
		RealtimeStats: *stats,
		Serving:       tqsc.QueryServiceEnabled && (!tqsc.IsInLameduck),
	}
}

// EnterLameduck implements tabletserver.Controller.
func (tqsc *Controller) EnterLameduck() {
	tqsc.mu.Lock()
	defer tqsc.mu.Unlock()

	tqsc.IsInLameduck = true
}
