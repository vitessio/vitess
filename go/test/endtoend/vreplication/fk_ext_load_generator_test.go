/*
Copyright 2023 The Vitess Authors.

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
	"context"
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/log"
)

const (
	// Only used when debugging tests.
	queryLog = "queries.txt"

	LoadGeneratorStateLoading = "loading"
	LoadGeneratorStateRunning = "running"
	LoadGeneratorStateStopped = "stopped"

	dataLoadTimeout = 1 * time.Minute
	tickInterval    = 1 * time.Second
	queryTimeout    = 1 * time.Minute

	getRandomIdQuery                    = "SELECT id FROM %s.parent ORDER BY RAND() LIMIT 1"
	insertQuery                         = "INSERT INTO %s.parent (id, name) VALUES (%d, 'name-%d')"
	updateQuery                         = "UPDATE %s.parent SET name = 'rename-%d' WHERE id = %d"
	deleteQuery                         = "DELETE FROM %s.parent WHERE id = %d"
	insertChildQuery                    = "INSERT INTO %s.child (id, parent_id) VALUES (%d, %d)"
	insertChildQueryOverrideConstraints = "INSERT /*+ SET_VAR(foreign_key_checks=0) */ INTO %s.child (id, parent_id) VALUES (%d, %d)"
)

// ILoadGenerator is an interface for load generators that we will use to simulate different types of loads.
type ILoadGenerator interface {
	Init(ctx context.Context, vc *VitessCluster) // name & description only for logging.
	Teardown()

	// "direct", use direct db connection to primary, only for unsharded keyspace.
	// or "vtgate" to use vtgate routing.
	// Stop() before calling SetDBStrategy().
	SetDBStrategy(direct, keyspace string)
	SetOverrideConstraints(allow bool) // true if load generator can insert rows without FK constraints.

	Keyspace() string
	DBStrategy() string        // direct or vtgate
	State() string             // state of load generator (stopped, running)
	OverrideConstraints() bool // true if load generator can insert rows without FK constraints.

	Load() error  // initial load of data.
	Start() error // start populating additional data.
	Stop() error  // stop populating additional data.

	// Implementation will decide which table to wait for extra rows on.
	WaitForAdditionalRows(count int) error
	// table == "", implementation will decide which table to get rows from, same table as in WaitForAdditionalRows().
	GetRowCount(table string) (int, error)
}

var lg ILoadGenerator

var _ ILoadGenerator = (*SimpleLoadGenerator)(nil)

type LoadGenerator struct {
	ctx                 context.Context
	vc                  *VitessCluster
	state               string
	dbStrategy          string
	overrideConstraints bool
	keyspace            string
	tables              []string
}

// SimpleLoadGenerator, which has a single parent table and a single child table for which different types
// of DMLs are run.
type SimpleLoadGenerator struct {
	LoadGenerator
	currentParentId int
	currentChildId  int
	ch              chan bool
	runCtx          context.Context
	runCtxCancel    context.CancelFunc
}

func (lg *SimpleLoadGenerator) SetOverrideConstraints(allow bool) {
	lg.overrideConstraints = allow
}

func (lg *SimpleLoadGenerator) OverrideConstraints() bool {
	return lg.overrideConstraints
}

func (lg *SimpleLoadGenerator) GetRowCount(table string) (int, error) {
	vtgateConn, err := lg.getVtgateConn(context.Background())
	if err != nil {
		return 0, err
	}
	defer vtgateConn.Close()
	return lg.getNumRows(vtgateConn, table), nil
}

func (lg *SimpleLoadGenerator) getVtgateConn(ctx context.Context) (*mysql.Conn, error) {
	vtParams := mysql.ConnParams{
		Host:  lg.vc.ClusterConfig.hostname,
		Port:  lg.vc.ClusterConfig.vtgateMySQLPort,
		Uname: "vt_dba",
	}
	conn, err := mysql.Connect(ctx, &vtParams)
	return conn, err
}

func (lg *SimpleLoadGenerator) getNumRows(vtgateConn *mysql.Conn, table string) int {
	t := lg.vc.t
	return getRowCount(t, vtgateConn, table)
}

func (lg *SimpleLoadGenerator) WaitForAdditionalRows(count int) error {
	t := lg.vc.t
	vtgateConn := getConnection(t, vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateMySQLPort)
	defer vtgateConn.Close()
	numRowsStart := lg.getNumRows(vtgateConn, "parent")
	shortCtx, cancel := context.WithTimeout(context.Background(), dataLoadTimeout)
	defer cancel()
	for {
		select {
		case <-shortCtx.Done():
			t.Fatalf("Timed out waiting for additional rows in %q table", "parent")
		default:
			numRows := lg.getNumRows(vtgateConn, "parent")
			if numRows >= numRowsStart+count {
				return nil
			}
			time.Sleep(tickInterval)
		}
	}
}

func (lg *SimpleLoadGenerator) exec(query string) (*sqltypes.Result, error) {
	switch lg.dbStrategy {
	case "direct":
		// direct is expected to be used only for unsharded keyspaces to simulate an unmanaged keyspace
		// that proxies to an external database.
		primary := lg.vc.getPrimaryTablet(lg.vc.t, lg.keyspace, "0")
		qr, err := primary.QueryTablet(query, lg.keyspace, true)
		require.NoError(lg.vc.t, err)
		return qr, err
	case "vtgate":
		return lg.execQueryWithRetry(query)
	default:
		err := fmt.Errorf("invalid dbStrategy: %v", lg.dbStrategy)
		return nil, err
	}
}

// When a workflow switches traffic it is possible to get transient errors from vtgate while executing queries
// due to cluster-level changes. isQueryRetryable() checks for such errors so that tests can wait for such changes
// to complete before proceeding.
func isQueryRetryable(err error) bool {
	retryableErrorStrings := []string{
		"retry",
		"resharded",
		"VT13001",
		"Lock wait timeout exceeded",
		"errno 2003",
	}
	for _, e := range retryableErrorStrings {
		if strings.Contains(err.Error(), e) {
			return true
		}
	}
	return false
}

func (lg *SimpleLoadGenerator) execQueryWithRetry(query string) (*sqltypes.Result, error) {
	ctx, cancel := context.WithTimeout(context.Background(), queryTimeout)
	defer cancel()
	errCh := make(chan error)
	qrCh := make(chan *sqltypes.Result)
	var vtgateConn *mysql.Conn
	go func() {
		var qr *sqltypes.Result
		var err error
		retry := false
		for {
			select {
			case <-ctx.Done():
				errCh <- fmt.Errorf("query %q did not succeed before the timeout of %s", query, queryTimeout)
				return
			default:
			}
			if lg.runCtx != nil && lg.runCtx.Err() != nil {
				log.Infof("Load generator run context done, query never completed: %q", query)
				errCh <- fmt.Errorf("load generator stopped")
				return
			}
			if retry {
				time.Sleep(tickInterval)
			}
			// We need to parse the error as well as the output of vdiff to determine if the error is retryable, since
			// sometimes it is observed that we get the error output as part of vdiff output.
			vtgateConn, err = lg.getVtgateConn(ctx)
			if err != nil {
				if !isQueryRetryable(err) {
					errCh <- err
					return
				}
				time.Sleep(tickInterval)
				continue
			}
			qr, err = vtgateConn.ExecuteFetch(query, 1000, false)
			vtgateConn.Close()
			if err == nil {
				qrCh <- qr
				return
			}
			if !isQueryRetryable(err) {
				errCh <- err
				return
			}
			retry = true
		}
	}()
	select {
	case qr := <-qrCh:
		return qr, nil
	case err := <-errCh:
		log.Infof("query %q failed with error %v", query, err)
		return nil, err
	}
}

func (lg *SimpleLoadGenerator) Load() error {
	lg.state = LoadGeneratorStateLoading
	defer func() { lg.state = LoadGeneratorStateStopped }()
	log.Infof("Inserting initial FK data")
	var queries = []string{
		"insert into parent values(1, 'parent1'), (2, 'parent2');",
		"insert into child values(1, 1, 'child11'), (2, 1, 'child21'), (3, 2, 'child32');",
	}
	for _, query := range queries {
		_, err := lg.exec(query)
		require.NoError(lg.vc.t, err)
	}
	log.Infof("Done inserting initial FK data")
	return nil
}

func (lg *SimpleLoadGenerator) Start() error {
	if lg.state == LoadGeneratorStateRunning {
		log.Infof("Load generator already running")
		return nil
	}
	lg.state = LoadGeneratorStateRunning
	go func() {
		defer func() {
			lg.state = LoadGeneratorStateStopped
			log.Infof("Load generator stopped")
		}()
		lg.runCtx, lg.runCtxCancel = context.WithCancel(lg.ctx)
		defer func() {
			lg.runCtx = nil
			lg.runCtxCancel = nil
		}()
		t := lg.vc.t
		var err error
		log.Infof("Load generator starting")
		for i := 0; ; i++ {
			if i%1000 == 0 {
				// Log occasionally to show that the test is still running.
				log.Infof("Load simulation iteration %d", i)
			}
			select {
			case <-lg.ctx.Done():
				log.Infof("Load generator context done")
				lg.ch <- true
				return
			case <-lg.runCtx.Done():
				log.Infof("Load generator run context done")
				lg.ch <- true
				return
			default:
			}
			op := rand.IntN(100)
			switch {
			case op < 50: // 50% chance to insert
				lg.insert()
			case op < 80: // 30% chance to update
				lg.update()
			default: // 20% chance to delete
				lg.delete()
			}
			require.NoError(t, err)
			time.Sleep(1 * time.Millisecond)
		}
	}()
	return nil
}

func (lg *SimpleLoadGenerator) Stop() error {
	if lg.state == LoadGeneratorStateStopped {
		log.Infof("Load generator already stopped")
		return nil
	}
	if lg.runCtx != nil && lg.runCtxCancel != nil {
		log.Infof("Canceling load generator")
		lg.runCtxCancel()
	}
	// Wait for ch to be closed or we hit a timeout.
	timeout := vdiffTimeout
	select {
	case <-lg.ch:
		log.Infof("Load generator stopped")
		lg.state = LoadGeneratorStateStopped
		return nil
	case <-time.After(timeout):
		log.Infof("Timed out waiting for load generator to stop")
		return fmt.Errorf("timed out waiting for load generator to stop")
	}
}

func (lg *SimpleLoadGenerator) Init(ctx context.Context, vc *VitessCluster) {
	lg.ctx = ctx
	lg.vc = vc
	lg.state = LoadGeneratorStateStopped
	lg.currentParentId = 100
	lg.currentChildId = 100
	lg.ch = make(chan bool)
	lg.tables = []string{"parent", "child"}
}

func (lg *SimpleLoadGenerator) Teardown() {
	// noop
}

func (lg *SimpleLoadGenerator) SetDBStrategy(strategy, keyspace string) {
	lg.dbStrategy = strategy
	lg.keyspace = keyspace
}

func (lg *SimpleLoadGenerator) Keyspace() string {
	return lg.keyspace
}

func (lg *SimpleLoadGenerator) DBStrategy() string {
	return lg.dbStrategy
}

func (lg *SimpleLoadGenerator) State() string {
	return lg.state
}

func isQueryCancelled(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "load generator stopped")
}

func (lg *SimpleLoadGenerator) insert() {
	t := lg.vc.t
	currentParentId++
	query := fmt.Sprintf(insertQuery, lg.keyspace, currentParentId, currentParentId)
	qr, err := lg.exec(query)
	if isQueryCancelled(err) {
		return
	}
	require.NoError(t, err)
	require.NotNil(t, qr)
	// Insert one or more children, some with valid foreign keys, some without.
	for i := 0; i < rand.IntN(4)+1; i++ {
		currentChildId++
		if i == 3 && lg.overrideConstraints {
			query = fmt.Sprintf(insertChildQueryOverrideConstraints, lg.keyspace, currentChildId, currentParentId+1000000)
			lg.exec(query)
		} else {
			query = fmt.Sprintf(insertChildQuery, lg.keyspace, currentChildId, currentParentId)
			lg.exec(query)
		}
	}
}

func (lg *SimpleLoadGenerator) getRandomId() int64 {
	t := lg.vc.t
	qr, err := lg.exec(fmt.Sprintf(getRandomIdQuery, lg.keyspace))
	if isQueryCancelled(err) {
		return 0
	}
	require.NoError(t, err)
	require.NotNil(t, qr)
	if len(qr.Rows) == 0 {
		return 0
	}
	id, err := qr.Rows[0][0].ToInt64()
	require.NoError(t, err)
	return id
}

func (lg *SimpleLoadGenerator) update() {
	id := lg.getRandomId()
	if id == 0 {
		return
	}
	updateQuery := fmt.Sprintf(updateQuery, lg.keyspace, id, id)
	_, err := lg.exec(updateQuery)
	if isQueryCancelled(err) {
		return
	}
	require.NoError(lg.vc.t, err)
}

func (lg *SimpleLoadGenerator) delete() {
	id := lg.getRandomId()
	if id == 0 {
		return
	}
	deleteQuery := fmt.Sprintf(deleteQuery, lg.keyspace, id)
	_, err := lg.exec(deleteQuery)
	if isQueryCancelled(err) {
		return
	}
	require.NoError(lg.vc.t, err)
}

// FIXME: following three functions are copied over from vtgate test utility functions in
// `go/test/endtoend/utils/utils.go`.
// We will to refactor and then reuse the same functionality from vtgate tests, in the near future.

func convertToMap(input interface{}) map[string]interface{} {
	output := input.(map[string]interface{})
	return output
}

func getTableT2Map(res *interface{}, ks, tbl string) map[string]interface{} {
	step1 := convertToMap(*res)["keyspaces"]
	step2 := convertToMap(step1)[ks]
	step3 := convertToMap(step2)["tables"]
	tblMap := convertToMap(step3)[tbl]
	return convertToMap(tblMap)
}

// waitForColumn waits for a table's column to be present in the vschema because vtgate's foreign key managed mode
// expects the column to be present in the vschema before it can be used in a foreign key constraint.
func waitForColumn(t *testing.T, vtgateProcess *cluster.VtgateProcess, ks, tbl, col string) error {
	timeout := time.After(defaultTimeout)
	for {
		select {
		case <-timeout:
			return fmt.Errorf("schema tracking did not find column '%s' in table '%s'", col, tbl)
		default:
			time.Sleep(defaultTick)
			res, err := vtgateProcess.ReadVSchema()
			require.NoError(t, err, res)
			t2Map := getTableT2Map(res, ks, tbl)
			authoritative, fieldPresent := t2Map["column_list_authoritative"]
			if !fieldPresent {
				break
			}
			authoritativeBool, isBool := authoritative.(bool)
			if !isBool || !authoritativeBool {
				break
			}
			colMap, exists := t2Map["columns"]
			if !exists {
				break
			}
			colList, isSlice := colMap.([]interface{})
			if !isSlice {
				break
			}
			for _, c := range colList {
				colDef, isMap := c.(map[string]interface{})
				if !isMap {
					break
				}
				if colName, exists := colDef["name"]; exists && colName == col {
					log.Infof("Found column '%s' in table '%s' for keyspace '%s'", col, tbl, ks)
					return nil
				}
			}
		}
	}
}
