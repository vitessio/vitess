/*
Copyright 2022 The Vitess Authors.

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

package vdiff

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	"vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vreplication"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

type Engine struct {
	mu     sync.Mutex
	isOpen bool

	controllers map[int64]*controller

	// ctx is the root context for all controllers
	ctx         context.Context
	cancel      context.CancelFunc
	cancelRetry context.CancelFunc

	ts                      *topo.Server
	dbClientFactoryFiltered func() binlogplayer.DBClient
	dbClientFactoryDba      func() binlogplayer.DBClient
	dbName                  string

	vre *vreplication.Engine

	wg         sync.WaitGroup
	thisTablet *topodata.Tablet

	// snapshotMu is used to ensure that only one vdiff snapshot cycle is active at a time,
	// because we stop/start vreplication workflows during this process
	snapshotMu            sync.Mutex
	vdiffSchemaCreateOnce sync.Once
}

func NewEngine(config *tabletenv.TabletConfig, ts *topo.Server, tablet *topodata.Tablet) *Engine {
	vde := &Engine{
		controllers: make(map[int64]*controller),
		ts:          ts,
		thisTablet:  tablet,
	}
	return vde
}

func (vde *Engine) InitDBConfig(dbcfgs *dbconfigs.DBConfigs) {
	// If we're already initilized, it's a test engine. Ignore the call.
	if vde.dbClientFactoryFiltered != nil && vde.dbClientFactoryDba != nil {
		return
	}
	vde.dbClientFactoryFiltered = func() binlogplayer.DBClient {
		return binlogplayer.NewDBClient(dbcfgs.FilteredWithDB())
	}
	vde.dbClientFactoryDba = func() binlogplayer.DBClient {
		return binlogplayer.NewDBClient(dbcfgs.DbaWithDB())
	}
	vde.dbName = dbcfgs.DBName
}

func (vde *Engine) Open(ctx context.Context, vre *vreplication.Engine) {
	vde.mu.Lock()
	defer vde.mu.Unlock()
	if vde.ts == nil || vde.isOpen {
		return
	}
	log.Infof("VDiff Engine: opening")
	if vde.cancelRetry != nil {
		vde.cancelRetry()
		vde.cancelRetry = nil
	}
	vde.vre = vre
	if err := vde.openLocked(ctx); err != nil {
		log.Infof("openLocked error: %s", err)
		ctx, cancel := context.WithCancel(ctx)
		vde.cancelRetry = cancel
		go vde.retry(ctx, err)
	}
}

func (vde *Engine) openLocked(ctx context.Context) error {
	rows, err := vde.getPendingVDiffs(ctx)
	if err != nil {
		return err
	}

	vde.ctx, vde.cancel = context.WithCancel(ctx)
	vde.isOpen = true
	if err := vde.initControllers(rows); err != nil {
		return err
	}
	return nil
}

var openRetryInterval = sync2.NewAtomicDuration(1 * time.Second)

func (vde *Engine) retry(ctx context.Context, err error) {
	log.Errorf("Error starting vdiff engine: %v, will keep retrying.", err)
	for {
		timer := time.NewTimer(openRetryInterval.Get())
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		vde.mu.Lock()
		// Recheck the context within the lock.
		// This guarantees that we will not retry
		// after the context was canceled. This
		// can almost never happen.
		select {
		case <-ctx.Done():
			vde.mu.Unlock()
			return
		default:
		}
		if err := vde.openLocked(ctx); err == nil {
			log.Infof("VDiff engine opened successfully")
			// Don't invoke cancelRetry because openLocked
			// will hold on to this context for later cancelation.
			vde.cancelRetry = nil
			vde.mu.Unlock()
			return
		}
		vde.mu.Unlock()
	}
}

func (vde *Engine) addController(row sqltypes.RowNamedValues, options *tabletmanagerdata.VDiffOptions) error {
	ct, err := newController(vde.ctx, row, vde.dbClientFactoryDba, vde.ts, vde, options)
	if err != nil {
		return fmt.Errorf("controller could not be initialized for stream: %+v", row)
	}
	vde.controllers[ct.id] = ct
	return nil
}

func (vde *Engine) initControllers(qr *sqltypes.Result) error {
	if qr == nil || len(qr.Rows) == 0 {
		return nil
	}
	for _, row := range qr.Named().Rows {
		options := &tabletmanagerdata.VDiffOptions{}
		if err := json.Unmarshal([]byte(row.AsString("options", "{}")), options); err != nil {
			return err
		}
		if err := vde.addController(row, options); err != nil {
			return err
		}
	}
	return nil
}

// IsOpen returns true if Engine is open.
func (vde *Engine) IsOpen() bool {
	vde.mu.Lock()
	defer vde.mu.Unlock()
	return vde.isOpen
}

// Close closes the Engine service.
func (vde *Engine) Close() {
	vde.mu.Lock()
	defer vde.mu.Unlock()

	// If we're retrying, we're not open.
	// Just cancel the retry loop.
	if vde.cancelRetry != nil {
		vde.cancelRetry()
		vde.cancelRetry = nil
		return
	}

	if !vde.isOpen {
		return
	}

	vde.cancel()
	// We still have to wait for all controllers to stop.
	for _, ct := range vde.controllers {
		ct.Stop()
	}
	vde.controllers = make(map[int64]*controller)

	// Wait for long-running functions to exit.
	vde.wg.Wait()

	vde.isOpen = false

	log.Infof("VDiff Engine: closed")
}

func (vde *Engine) getDBClient(isAdmin bool) binlogplayer.DBClient {
	if isAdmin {
		return vde.dbClientFactoryDba()
	}
	return vde.dbClientFactoryFiltered()
}
func (vde *Engine) getPendingVDiffs(ctx context.Context) (*sqltypes.Result, error) {
	dbClient := vde.dbClientFactoryFiltered()
	if err := dbClient.Connect(); err != nil {
		return nil, err
	}
	defer dbClient.Close()
	qr, err := withDDL.ExecIgnore(ctx, sqlGetPendingVDiffs, dbClient.ExecuteFetch)
	if err != nil {
		return nil, err
	}
	if len(qr.Rows) == 0 {
		return nil, nil
	}
	return qr, nil
}

func (vde *Engine) getVDiffByID(ctx context.Context, id int64) (*sqltypes.Result, error) {
	dbClient := vde.dbClientFactoryFiltered()
	if err := dbClient.Connect(); err != nil {
		return nil, err
	}
	defer dbClient.Close()
	qr, err := withDDL.Exec(ctx, fmt.Sprintf(sqlGetVDiffByID, id), dbClient.ExecuteFetch, dbClient.ExecuteFetch)
	if err != nil {
		return nil, err
	}
	if len(qr.Rows) != 1 {
		return nil, fmt.Errorf("unable to read vdiff table for %d", id)
	}
	return qr, nil
}
