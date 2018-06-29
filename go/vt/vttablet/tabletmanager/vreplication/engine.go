/*
Copyright 2018 The Vitess Authors.

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

package vreplication

import (
	"errors"
	"flag"
	"fmt"
	"sync"
	"time"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/topo"
)

var tabletTypesStr = flag.String("vreplication_tablet_type", "REPLICA", "comma separated list of tablet types used as a source")

// waitRetryTime can be changed to a smaller value for tests.
var waitRetryTime = 1 * time.Second

// Engine is the engine for handling vreplication.
type Engine struct {
	// mu synchronizes isOpen, controllers and wg.
	mu          sync.Mutex
	isOpen      bool
	controllers map[int]*controller
	// wg is used in-flight functions that can run for long periods.
	wg sync.WaitGroup

	// ctx is the root context for all controllers.
	ctx context.Context
	// cancel will cancel the root context, thereby all controllers.
	cancel context.CancelFunc

	ts              *topo.Server
	cell            string
	mysqld          mysqlctl.MysqlDaemon
	dbClientFactory func() binlogplayer.VtClient
}

// NewEngine creates a new Engine.
func NewEngine(ts *topo.Server, cell string, mysqld mysqlctl.MysqlDaemon, dbClientFactory func() binlogplayer.VtClient) *Engine {
	return &Engine{
		controllers:     make(map[int]*controller),
		ts:              ts,
		cell:            cell,
		mysqld:          mysqld,
		dbClientFactory: dbClientFactory,
	}
}

// Open starts the Engine service.
func (vre *Engine) Open(ctx context.Context) error {
	vre.mu.Lock()
	defer vre.mu.Unlock()
	if vre.isOpen {
		return nil
	}
	vre.ctx, vre.cancel = context.WithCancel(ctx)
	vre.isOpen = true
	return nil
}

// Close closes the Engine service.
func (vre *Engine) Close() {
	vre.mu.Lock()
	defer vre.mu.Unlock()
	if !vre.isOpen {
		return
	}

	vre.cancel()
	// We still have to wait for all controllers to stop.
	for _, ct := range vre.controllers {
		ct.Stop()
	}
	vre.controllers = make(map[int]*controller)

	// Wait for long-running functions to exit.
	vre.wg.Wait()

	vre.mysqld.DisableBinlogPlayback()
	vre.isOpen = false
}

// Exec executes the query and the related actions.
func (vre *Engine) Exec(query string) (*sqltypes.Result, error) {
	vre.mu.Lock()
	defer vre.mu.Unlock()
	if !vre.isOpen {
		return nil, errors.New("vreplication engine is closed")
	}

	plan, err := getPlan(query)
	if err != nil {
		return nil, err
	}
	dbClient := vre.dbClientFactory()
	if err := dbClient.Connect(); err != nil {
		return nil, err
	}
	defer dbClient.Close()

	switch plan.opcode {
	case insertQuery:
		qr, err := dbClient.ExecuteFetch(plan.query, 1)
		if err != nil {
			return nil, err
		}
		if qr.InsertID == 0 {
			return nil, fmt.Errorf("insert failed to generate an id")
		}
		if err := vre.initController(dbClient, int(qr.InsertID)); err != nil {
			return nil, err
		}
		return qr, nil
	case updateQuery:
		if ct := vre.controllers[plan.id]; ct != nil {
			ct.Stop()
		}
		qr, err := dbClient.ExecuteFetch(plan.query, 1)
		if err != nil {
			return nil, err
		}
		if err := vre.initController(dbClient, plan.id); err != nil {
			return nil, err
		}
		return qr, nil
	case deleteQuery:
		if ct := vre.controllers[plan.id]; ct != nil {
			ct.Stop()
			delete(vre.controllers, plan.id)
		}
		return dbClient.ExecuteFetch(plan.query, 1)
	case selectQuery:
		return dbClient.ExecuteFetch(plan.query, 10000)
	}
	panic("unreachable")
}

func (vre *Engine) initController(dbClient binlogplayer.VtClient, id int) error {
	params, err := readRow(dbClient, id)
	if err != nil {
		return err
	}
	ct, err := newController(vre.ctx, params, vre.dbClientFactory, vre.mysqld, vre.ts, vre.cell, *tabletTypesStr)
	if err != nil {
		return err
	}
	vre.controllers[id] = ct
	return nil
}

// WaitForPos waits for the replication to reach the specified position.
func (vre *Engine) WaitForPos(ctx context.Context, id int, pos string) error {
	mPos, err := mysql.DecodePosition(pos)
	if err != nil {
		return err
	}

	vre.mu.Lock()
	if !vre.isOpen {
		vre.mu.Unlock()
		return errors.New("vreplication engine is closed")
	}
	vre.wg.Add(1)
	vre.mu.Unlock()
	defer vre.wg.Done()

	dbClient := vre.dbClientFactory()
	if err := dbClient.Connect(); err != nil {
		return err
	}
	defer dbClient.Close()

	for {
		qr, err := dbClient.ExecuteFetch(binlogplayer.ReadVReplicationPos(uint32(id)), 10)
		switch {
		case err != nil:
			return err
		case len(qr.Rows) == 0:
			return fmt.Errorf("vreplication stream %d not found", id)
		case len(qr.Rows) > 1 || len(qr.Rows[0]) != 1:
			return fmt.Errorf("unexpected result: %v", qr)
		}
		current, err := mysql.DecodePosition(qr.Rows[0][0].ToString())
		if err != nil {
			return err
		}

		if current.AtLeast(mPos) {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-vre.ctx.Done():
			return fmt.Errorf("vreplication is closing: %v", vre.ctx.Err())
		case <-time.After(waitRetryTime):
		}
	}
}

func readRow(dbClient binlogplayer.VtClient, id int) (map[string]string, error) {
	qr, err := dbClient.ExecuteFetch(fmt.Sprintf("select * from _vt.vreplication where id = %d", id), 10)
	if err != nil {
		return nil, err
	}
	return resultToMap(qr)
}

func resultToMap(qr *sqltypes.Result) (map[string]string, error) {
	if len(qr.Rows) != 1 {
		return nil, fmt.Errorf("unexpected number of rows: %v", qr)
	}
	row := qr.Rows[0]
	m := make(map[string]string, len(row))
	for i, fld := range qr.Fields {
		if row[i].IsNull() {
			continue
		}
		m[fld.Name] = row[i].ToString()
	}
	return m, nil
}
