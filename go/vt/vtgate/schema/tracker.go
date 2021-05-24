/*
Copyright 2021 The Vitess Authors.

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

package schema

import (
	"context"
	"sync"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"

	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// Tracker contains the required fields to perform schema tracking.
type Tracker struct {
	ch     chan *discovery.TabletHealth
	cancel context.CancelFunc

	mu       sync.Mutex
	tableMap map[string][]vindexes.Column
	ctx      context.Context
}

// NewTracker creates the tracker object.
func NewTracker(ch chan *discovery.TabletHealth) *Tracker {
	return &Tracker{ch: ch, tableMap: map[string][]vindexes.Column{}}
}

// waitFor is an interface we use to make it possible to test this concurrent code
// without having to use time.Sleep. In production, these are empty methods that would
// be called every time we have to fetch schema, which is not very often at all
type waitFor interface {
	done()
	wait()
	reset()
}

// Start starts the schema tracking.
func (t *Tracker) Start() {
	log.Info("Starting schema tracking")
	t.startWithWaiter(&noWaiter{})
}

// startWithWaiter starts the schema tracking with a custom waitFor
func (t *Tracker) startWithWaiter(i waitFor) {
	ctx, cancel := context.WithCancel(context.Background())
	t.cancel = cancel
	go func(ctx context.Context, t *Tracker) {
		for {
			select {
			case th := <-t.ch:
				if len(th.TablesUpdated) > 0 {
					t.updateSchema(th)
					i.done()
				}
			case <-ctx.Done():
				close(t.ch)
				return
			}
		}
	}(ctx, t)
}

// Stop stops the schema tracking
func (t *Tracker) Stop() {
	log.Info("Stopping schema tracking")
	t.cancel()
}

// GetColumns returns the column list for table in the given keyspace.
func (t *Tracker) GetColumns(ks string, tbl string) []vindexes.Column {
	t.mu.Lock()
	defer t.mu.Unlock()
	key := ks + "." + tbl
	return t.tableMap[key]
}

func (t *Tracker) updateSchema(th *discovery.TabletHealth) {
	tables, err := sqltypes.BuildBindVariable(th.TablesUpdated)
	if err != nil {
		log.Errorf("failed to read updated tables from TabletHealth: %v", err)
		return
	}
	bv := map[string]*querypb.BindVariable{"tableNames": tables}
	res, err := th.Conn.Execute(t.ctx, th.Target, mysql.FetchUpdatedTables, bv, 0, 0, nil)
	if err != nil {
		// TODO: these tables should now become non-authoritative
		log.Warningf("error fetching new schema for %v, making them non-authoritative: %v", th.TablesUpdated, err)
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	// first we empty all prior schema. deleted tables will not show up in the result,
	// so this is the only chance to delete
	for _, tbl := range th.TablesUpdated {
		key := th.Target.Keyspace + "." + tbl
		delete(t.tableMap, key)
	}

	for _, row := range res.Rows {
		tbl := row[0].ToString()
		colName := row[1].ToString()
		colType := row[2].ToString()
		key := th.Target.Keyspace + "." + tbl

		cType := sqlparser.ColumnType{Type: colType}
		col := vindexes.Column{Name: sqlparser.NewColIdent(colName), Type: cType.SQLType()}

		t.tableMap[key] = append(t.tableMap[key], col)
	}
}

type noWaiter struct{}

func (n *noWaiter) done() {}

func (n *noWaiter) wait() {}

func (n *noWaiter) reset() {}
