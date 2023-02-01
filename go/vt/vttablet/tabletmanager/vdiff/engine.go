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
	"errors"
	"fmt"
	"sync"
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	"vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vreplication"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/sync2"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

type Engine struct {
	isOpen bool

	mu          sync.Mutex // guards controllers
	controllers map[int64]*controller

	// ctx is the root context for all controllers
	ctx         context.Context
	cancel      context.CancelFunc
	cancelRetry context.CancelFunc

	ts                      *topo.Server
	tmClientFactory         func() tmclient.TabletManagerClient
	dbClientFactoryFiltered func() binlogplayer.DBClient
	dbClientFactoryDba      func() binlogplayer.DBClient
	dbName                  string

	vre *vreplication.Engine

	wg         sync.WaitGroup
	thisTablet *topodata.Tablet

	// snapshotMu is used to ensure that only one vdiff snapshot cycle is active at a time,
	// because we stop/start vreplication workflows during this process
	snapshotMu sync.Mutex

	vdiffSchemaCreateOnce sync.Once

	// This should only be set when the engine is being used in tests. It then provides
	// modified behavior for that env, e.g. not starting the retry goroutine. This should
	// NOT be set in production.
	fortests bool
}

func NewEngine(config *tabletenv.TabletConfig, ts *topo.Server, tablet *topodata.Tablet) *Engine {
	vde := &Engine{
		controllers:     make(map[int64]*controller),
		ts:              ts,
		thisTablet:      tablet,
		tmClientFactory: func() tmclient.TabletManagerClient { return tmclient.NewTabletManagerClient() },
	}
	return vde
}

// NewTestEngine creates an Engine for use in tests. It uses the custom db client factory and
// tablet manager client factory, while setting the fortests field to true to modify any engine
// behavior when used in tests (e.g. not starting the retry goroutine).
func NewTestEngine(ts *topo.Server, tablet *topodata.Tablet, dbn string, dbcf func() binlogplayer.DBClient, tmcf func() tmclient.TabletManagerClient) *Engine {
	vde := &Engine{
		controllers:             make(map[int64]*controller),
		ts:                      ts,
		thisTablet:              tablet,
		dbName:                  dbn,
		dbClientFactoryFiltered: dbcf,
		dbClientFactoryDba:      dbcf,
		tmClientFactory:         tmcf,
		fortests:                true,
	}
	return vde
}

func (vde *Engine) InitDBConfig(dbcfgs *dbconfigs.DBConfigs) {
	// If it's a test engine and we're already initilized then do nothing.
	if vde.fortests && vde.dbClientFactoryFiltered != nil && vde.dbClientFactoryDba != nil {
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
	log.Infof("VDiff Engine: opening...")

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
	// This should never happen
	if len(vde.controllers) > 0 {
		log.Warningf("VDiff Engine invalid state detected: %d controllers existed when opening; resetting state", len(vde.controllers))
		vde.resetControllers()
	}

	// At this point the tablet has no controllers running. So
	// we want to start any VDiffs that have not been explicitly
	// stopped or otherwise finished.
	rows, err := vde.getVDiffsToRun(ctx)
	if err != nil {
		return err
	}
	vde.ctx, vde.cancel = context.WithCancel(ctx)
	vde.isOpen = true // now we are open and have things to close
	if err := vde.initControllers(rows); err != nil {
		return err
	}

	// At this point we've fully and succesfully opened so begin
	// retrying error'd VDiffs until the engine is closed.
	vde.wg.Add(1)
	go func() {
		defer vde.wg.Done()
		if vde.fortests {
			return
		}
		vde.retryErroredVDiffs()
	}()

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
			log.Infof("VDiff engine: opened successfully")
			// Don't invoke cancelRetry because openLocked
			// will hold on to this context for later cancelation.
			vde.cancelRetry = nil
			vde.mu.Unlock()
			return
		}
		vde.mu.Unlock()
	}
}

// addController creates a new controller using the given vdiff record and adds it to the engine.
// You must already have the main engine mutex (mu) locked before calling this.
func (vde *Engine) addController(row sqltypes.RowNamedValues, options *tabletmanagerdata.VDiffOptions) error {
	ct, err := newController(vde.ctx, row, vde.dbClientFactoryDba, vde.ts, vde, options)
	if err != nil {
		return fmt.Errorf("controller could not be initialized for stream %+v on tablet %v",
			row, vde.thisTablet.Alias)
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
		if err := json.Unmarshal(row.AsBytes("options", []byte("{}")), options); err != nil {
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
	vde.resetControllers()

	// Wait for long-running functions to exit.
	vde.wg.Wait()

	vde.isOpen = false

	log.Infof("VDiff Engine: closed")
}

func (vde *Engine) getVDiffsToRun(ctx context.Context) (*sqltypes.Result, error) {
	dbClient := vde.dbClientFactoryFiltered()
	if err := dbClient.Connect(); err != nil {
		return nil, err
	}
	defer dbClient.Close()

	// We have to use ExecIgnore here so as not to block quick tablet state
	// transitions from primary to non-primary when starting the engine
	qr, err := dbClient.ExecuteFetch(sqlGetVDiffsToRun, -1)
	if err != nil {
		return nil, err
	}
	if len(qr.Rows) == 0 {
		return nil, nil
	}
	return qr, nil
}

func (vde *Engine) getVDiffsToRetry(ctx context.Context, dbClient binlogplayer.DBClient) (*sqltypes.Result, error) {
	qr, err := dbClient.ExecuteFetch(sqlGetVDiffsToRetry, -1)
	if err != nil {
		return nil, err
	}
	if len(qr.Rows) == 0 {
		return nil, nil
	}
	return qr, nil
}

func (vde *Engine) getVDiffByID(ctx context.Context, dbClient binlogplayer.DBClient, id int64) (*sqltypes.Result, error) {
	qr, err := dbClient.ExecuteFetch(fmt.Sprintf(sqlGetVDiffByID, id), -1)
	if err != nil {
		return nil, err
	}
	if len(qr.Rows) != 1 {
		return nil, fmt.Errorf("no vdiff found for id %d on tablet %v",
			id, vde.thisTablet.Alias)
	}
	return qr, nil
}

func (vde *Engine) retryVDiffs(ctx context.Context) error {
	vde.mu.Lock()
	defer vde.mu.Unlock()
	dbClient := vde.dbClientFactoryFiltered()
	if err := dbClient.Connect(); err != nil {
		return err
	}
	defer dbClient.Close()

	qr, err := vde.getVDiffsToRetry(ctx, dbClient)
	if err != nil {
		return err
	}
	if qr == nil || len(qr.Rows) == 0 {
		return nil
	}
	for _, row := range qr.Named().Rows {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		lastError := mysql.NewSQLErrorFromError(errors.New(row.AsString("last_error", "")))
		if !mysql.IsEphemeralError(lastError) {
			continue
		}
		uuid := row.AsString("vdiff_uuid", "")
		id, err := row.ToInt64("id")
		if err != nil {
			return err
		}
		log.Infof("Retrying vdiff %s that had an ephemeral error of '%v'", uuid, lastError)
		if _, err = dbClient.ExecuteFetch(fmt.Sprintf(sqlRetryVDiff, id), 1); err != nil {
			return err
		}
		options := &tabletmanagerdata.VDiffOptions{}
		if err := json.Unmarshal(row.AsBytes("options", []byte("{}")), options); err != nil {
			return err
		}
		if err := vde.addController(row, options); err != nil {
			return err
		}
	}
	return nil
}

func (vde *Engine) retryErroredVDiffs() {
	tkr := time.NewTicker(time.Second * 30)
	defer tkr.Stop()
	for {
		select {
		case <-vde.ctx.Done():
			log.Info("VDiff engine: closing...")
			return
		case <-tkr.C:
		}

		if err := vde.retryVDiffs(vde.ctx); err != nil {
			log.Errorf("Error retrying vdiffs: %v", err)
		}
	}
}

func (vde *Engine) resetControllers() {
	for _, ct := range vde.controllers {
		ct.Stop()
	}
	vde.controllers = make(map[int64]*controller)
}
