// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package mock provides mock interfaces for tabletserver.
package mock

import (
	"github.com/youtube/vitess/go/vt/dbconfigs"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	pb "github.com/youtube/vitess/go/vt/proto/query"
	"github.com/youtube/vitess/go/vt/proto/topodata"
	"github.com/youtube/vitess/go/vt/tabletserver"
	"github.com/youtube/vitess/go/vt/tabletserver/queryservice"
)

// TestQueryServiceControl is a fake version of QueryServiceControl
type TestQueryServiceControl struct {
	// QueryServiceEnabled is a state variable
	QueryServiceEnabled bool

	// InitDBConfigError is the return value for InitDBConfig
	InitDBConfigError error

	// SetServingTypeError is the return value for SetServingType
	SetServingTypeError error

	// StartServiceError is the return value for StartService
	StartServiceError error

	// IsHealthy is the return value for IsHealthy
	IsHealthyError error

	// ReloadSchemaCount counts how many times ReloadSchema was called
	ReloadSchemaCount int
}

// NewTestQueryServiceControl returns an implementation of QueryServiceControl
// that is entirely fake
func NewTestQueryServiceControl() *TestQueryServiceControl {
	return &TestQueryServiceControl{
		QueryServiceEnabled: false,
		InitDBConfigError:   nil,
		StartServiceError:   nil,
		IsHealthyError:      nil,
		ReloadSchemaCount:   0,
	}
}

// Register is part of the QueryServiceControl interface
func (tqsc *TestQueryServiceControl) Register() {
}

// AddStatusPart is part of the QueryServiceControl interface
func (tqsc *TestQueryServiceControl) AddStatusPart() {
}

// InitDBConfig is part of the QueryServiceControl interface
func (tqsc *TestQueryServiceControl) InitDBConfig(*pb.Target, *dbconfigs.DBConfigs, []tabletserver.SchemaOverride, mysqlctl.MysqlDaemon) error {
	tqsc.QueryServiceEnabled = tqsc.InitDBConfigError == nil
	return tqsc.InitDBConfigError
}

// SetServingType is part of the QueryServiceControl interface
func (tqsc *TestQueryServiceControl) SetServingType(topodata.TabletType, bool) error {
	tqsc.QueryServiceEnabled = tqsc.SetServingTypeError == nil
	return tqsc.SetServingTypeError
}

// StartService is part of the QueryServiceControl interface
func (tqsc *TestQueryServiceControl) StartService(*pb.Target, *dbconfigs.DBConfigs, []tabletserver.SchemaOverride, mysqlctl.MysqlDaemon) error {
	tqsc.QueryServiceEnabled = tqsc.StartServiceError == nil
	return tqsc.StartServiceError
}

// StopService is part of the QueryServiceControl interface
func (tqsc *TestQueryServiceControl) StopService() {
	tqsc.QueryServiceEnabled = false
}

// IsServing is part of the QueryServiceControl interface
func (tqsc *TestQueryServiceControl) IsServing() bool {
	return tqsc.QueryServiceEnabled
}

// IsHealthy is part of the QueryServiceControl interface
func (tqsc *TestQueryServiceControl) IsHealthy() error {
	return tqsc.IsHealthyError
}

// ReloadSchema is part of the QueryServiceControl interface
func (tqsc *TestQueryServiceControl) ReloadSchema() {
	tqsc.ReloadSchemaCount++
}

// RegisterQueryRuleSource is part of the QueryServiceControl interface
func (tqsc *TestQueryServiceControl) RegisterQueryRuleSource(ruleSource string) {
}

// UnRegisterQueryRuleSource is part of the QueryServiceControl interface
func (tqsc *TestQueryServiceControl) UnRegisterQueryRuleSource(ruleSource string) {
}

// SetQueryRules is part of the QueryServiceControl interface
func (tqsc *TestQueryServiceControl) SetQueryRules(ruleSource string, qrs *tabletserver.QueryRules) error {
	return nil
}

// QueryService is part of the QueryServiceControl interface
func (tqsc *TestQueryServiceControl) QueryService() queryservice.QueryService {
	return nil
}

// BroadcastHealth is part of the QueryServiceControl interface
func (tqsc *TestQueryServiceControl) BroadcastHealth(terTimestamp int64, stats *pb.RealtimeStats) {
}
