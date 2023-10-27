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
	"math/rand"
	"os"
	"runtime/debug"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/endtoend/cluster"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
)

type ILoadGenerator interface {
	Init(ctx context.Context, vc *VitessCluster) // nme & description only for logging.
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

	WaitForAdditionalRows(count int) error // implementation will decide which table to wait for extra rows on.
	GetRowCount(table string) (int, error) // table == "", implementation will decide which table to get rows from.
	GetTables() ([]string, []int, error)   // list of tables used by implementation with number of rows.
}

var _ ILoadGenerator = (*SimpleLoadGenerator)(nil)

type LoadGenerator struct {
	ctx                 context.Context
	vc                  *VitessCluster
	state               string
	dbStrategy          string
	overrideConstraints bool
	keyspace            string
	tables              []string
	tableNumRows        []int
}

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

func (lg *SimpleLoadGenerator) GetTables() ([]string, []int, error) {
	return nil, nil, nil
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
	numRows, err := getRowCount(t, vtgateConn, table)
	require.NoError(t, err)
	return numRows
}

func (lg *SimpleLoadGenerator) WaitForAdditionalRows(count int) error {
	t := lg.vc.t
	vtgateConn := getConnection(t, vc.ClusterConfig.hostname, vc.ClusterConfig.vtgateMySQLPort)
	defer vtgateConn.Close()
	numRowsStart := lg.getNumRows(vtgateConn, "parent")
	shortCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	for {
		select {
		case <-shortCtx.Done():
			log.Infof("Timed out waiting for additional rows\n%s", debug.Stack())
			t.Fatalf("Timed out waiting for additional rows")
		default:
			numRows := lg.getNumRows(vtgateConn, "parent")
			if numRows >= numRowsStart+count {
				return nil
			}
			time.Sleep(1 * time.Second)
		}
	}
}

const queryLog = "queries.txt"

func appendToQueryLog(msg string) {
	file, err := os.OpenFile(queryLog, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Errorf("Error opening query log file: %v", err)
		return
	}
	defer file.Close()
	if _, err := file.WriteString(msg + "\n"); err != nil {
		log.Errorf("Error writing to query log file: %v", err)
	}
}

func (lg *SimpleLoadGenerator) exec(query string) (*sqltypes.Result, error) {
	switch lg.dbStrategy {
	case "direct":
		// direct is expected to be used only for unsharded keyspaces.
		primary := lg.vc.getPrimaryTablet(lg.vc.t, lg.keyspace, "0")
		qr, err := primary.QueryTablet(query, lg.keyspace, true)
		require.NoError(lg.vc.t, err)
		return qr, err
	case "vtgate":
		return lg.execQueryWithRetry(query)
	default:
		err := fmt.Errorf("Invalid dbStrategy: %v", lg.dbStrategy)
		return nil, err
	}
}

func isQueryRetryable(err error) bool {
	retriableErrorStrings := []string{
		"retry",
		"resharded",
		"VT13001",
		"Lock wait timeout exceeded",
		"errno 2003",
	}
	for _, retriableErrorString := range retriableErrorStrings {
		if strings.Contains(err.Error(), retriableErrorString) {
			return true
		}
	}
	return false
}
func (lg *SimpleLoadGenerator) execQueryWithRetry(query string) (*sqltypes.Result, error) {
	timeout := 60 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
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
				errCh <- fmt.Errorf("query %q did not succeed before the timeout of %s", query, timeout)
				return
			default:
			}
			if retry {
				time.Sleep(1 * time.Second)
			}
			vtgateConn, err = lg.getVtgateConn(ctx)
			if err != nil {
				if !isQueryRetryable(err) {
					errCh <- err
					return
				}
				time.Sleep(1 * time.Second)
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

func (lg *SimpleLoadGenerator) execQueryWithRetry2(query string) (*sqltypes.Result, error) {
	timeout := 5 * time.Second
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	var vtgateConn *mysql.Conn
	var err error
	var qr *sqltypes.Result
	retry := false
	for {
		if retry {
			log.Infof("1: Retrying query %q", query)
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)

		vtgateConn, err = lg.getVtgateConn(ctx)
		if err == nil {
			if retry {
				log.Infof("2: Retrying query %q", query)
			}
			select {
			case <-timer.C:
				cancel()
				return nil, fmt.Errorf("query %q did not succeed before the timeout of %s", query, timeout)
			default:

				qr, err = vtgateConn.ExecuteFetch(query, 1000, false)
			}

			if err == nil {
				if retry {
					log.Infof("3: Retry successful query %q", query)
				}
				appendToQueryLog(query)
				cancel()
				return qr, nil
			} else {
				retry = true
				retriableErrorStrings := []string{
					"retry",
					"resharded",
					"VT13001",
					"Lock wait timeout exceeded",
				}
				for _, retriableErrorString := range retriableErrorStrings {
					if strings.Contains(err.Error(), retriableErrorString) {
						log.Infof("found retriable error string %q in error %v, resetting timer", retriableErrorString, err)
						if !timer.Stop() {
							<-timer.C
						}
						timer.Reset(timeout)
						break
					}
				}
				log.Infof("query %q failed with error %v, retrying in %ds", query, err, int(defaultTick.Seconds()))
			}

		}
		if vtgateConn != nil {
			vtgateConn.Close()
		}
		if retry {
			log.Infof("4: Retrying query before select %q", query)
		}
		select {
		case <-timer.C:
			if !retry {
				require.FailNow(lg.vc.t, fmt.Sprintf("query %q did not succeed before the timeout of %s; last seen result: %v",
					query, timeout, qr))
			} else {
				timer.Reset(timeout)
			}
		default:
			log.Infof("query %q failed with error %v, retrying in %ds", query, err, int(defaultTick.Seconds()))
			time.Sleep(defaultTick)
		}
		if retry {
			log.Infof("5: Retrying query after select %q", query)
		}
	}
}

func (lg *SimpleLoadGenerator) Load() error {
	lg.state = "loading"
	defer func() { lg.state = "stopped" }()
	log.Infof("Inserting initial FK data")
	var queries []string = []string{
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
	if lg.state == "running" {
		log.Infof("Load generator already running")
		return nil
	}
	lg.state = "running"
	go func() {
		defer func() {
			lg.state = "stopped"
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
				// log occasionally ...
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
			op := rand.Intn(100)
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
	if lg.state == "stopped" {
		log.Infof("Load generator already stopped")
		return nil
	}
	if lg.runCtx != nil && lg.runCtxCancel != nil {
		log.Infof("Canceling load generator")
		lg.runCtxCancel()
	}
	// wait for ch to be closed with a timeout
	timeout := 30 * time.Second
	select {
	case <-lg.ch:
		log.Infof("Load generator stopped")
		return nil
	case <-time.After(timeout):
		log.Infof("Timed out waiting for load generator to stop")
		return fmt.Errorf("Timed out waiting for load generator to stop")
	}
}

func (lg *SimpleLoadGenerator) Init(ctx context.Context, vc *VitessCluster) {
	lg.ctx = ctx
	lg.vc = vc
	lg.state = "stopped"
	lg.currentParentId = 100
	lg.currentChildId = 100
	lg.ch = make(chan bool)
	lg.tables = []string{"parent", "child"}
}

func (lg *SimpleLoadGenerator) Teardown() {
	// noop
}

func (lg *SimpleLoadGenerator) SetDBStrategy(direct, keyspace string) {
	lg.dbStrategy = direct
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

const (
	getRandomIdQuery                    = "SELECT id FROM %s.parent ORDER BY RAND() LIMIT 1"
	insertQuery                         = "INSERT INTO %s.parent (id, name) VALUES (%d, 'name-%d')"
	updateQuery                         = "UPDATE %s.parent SET name = 'rename-%d' WHERE id = %d"
	deleteQuery                         = "DELETE FROM %s.parent WHERE id = %d"
	insertChildQuery                    = "INSERT INTO %s.child (id, parent_id) VALUES (%d, %d)"
	insertChildQueryOverrideConstraints = "INSERT /*+ SET_VAR(foreign_key_checks=0) */ INTO %s.child (id, parent_id) VALUES (%d, %d)"
)

func (lg *SimpleLoadGenerator) insert() {
	t := lg.vc.t
	currentParentId++
	query := fmt.Sprintf(insertQuery, lg.keyspace, currentParentId, currentParentId)
	qr, err := lg.exec(query)
	require.NoError(t, err)
	require.NotNil(t, qr)
	// insert one or more children, some with valid foreign keys, some without.
	for i := 0; i < rand.Intn(4)+1; i++ {
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
	updateQuery := fmt.Sprintf(updateQuery, lg.keyspace, id, id)
	_, err := lg.exec(updateQuery)
	require.NoError(lg.vc.t, err)
}

func (lg *SimpleLoadGenerator) delete() {
	deleteQuery := fmt.Sprintf(deleteQuery, lg.keyspace, lg.getRandomId())
	_, err := lg.exec(deleteQuery)
	require.NoError(lg.vc.t, err)
}

// FIXME: following three functions need to be refactored

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

// waitForColumn waits for a table's column to be present
func waitForColumn(t *testing.T, vtgateProcess *cluster.VtgateProcess, ks, tbl, col string) error {
	timeout := time.After(60 * time.Second)
	for {
		select {
		case <-timeout:
			return fmt.Errorf("schema tracking did not find column '%s' in table '%s'", col, tbl)
		default:
			time.Sleep(1 * time.Second)
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
