// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package worker

import (
	"fmt"
	"sync"
	"time"

	"github.com/youtube/vitess/go/vt/mysqlctl/tmutils"

	tabletmanagerdatapb "github.com/youtube/vitess/go/vt/proto/tabletmanagerdata"
)

// tableStatusList contains the status for each table of a schema.
// Functions which modify the status of a table must use the same index for
// the table which the table had in schema passed in to initialize().
type tableStatusList struct {
	// mu guards all fields in the group below.
	mu sync.Mutex
	// initialized is true when initialize() was called.
	initialized bool
	// tableStatuses is written once by initialize().
	tableStatuses []*tableStatus
	// startTime records the time initialize() was called.
	// Same as tableStatuses it's a write-once field.
	startTime time.Time
}

func (t *tableStatusList) initialize(schema *tabletmanagerdatapb.SchemaDefinition) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.initialized {
		panic(fmt.Errorf("tableStatusList is already initialized: %v", t.tableStatuses))
	}

	t.tableStatuses = make([]*tableStatus, len(schema.TableDefinitions))
	for i, td := range schema.TableDefinitions {
		t.tableStatuses[i] = newTableStatus(td.Name, td.Type != tmutils.TableBaseTable /* isView */, td.RowCount)
	}

	t.startTime = time.Now()

	t.initialized = true
}

// isInitialized returns true when initialize() was called.
func (t *tableStatusList) isInitialized() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.initialized
}

func (t *tableStatusList) setThreadCount(tableIndex, threadCount int) {
	if !t.isInitialized() {
		panic("setThreadCount() requires an initialized tableStatusList")
	}

	t.tableStatuses[tableIndex].setThreadCount(threadCount)
}

func (t *tableStatusList) threadStarted(tableIndex int) {
	if !t.isInitialized() {
		panic("threadStarted() requires an initialized tableStatusList")
	}

	t.tableStatuses[tableIndex].threadStarted()
}

func (t *tableStatusList) threadDone(tableIndex int) {
	if !t.isInitialized() {
		panic("threadDone() requires an initialized tableStatusList")
	}

	t.tableStatuses[tableIndex].threadDone()
}

func (t *tableStatusList) addCopiedRows(tableIndex, copiedRows int) {
	if !t.isInitialized() {
		panic("addCopiedRows() requires an initialized tableStatusList")
	}

	t.tableStatuses[tableIndex].addCopiedRows(copiedRows)
}

// format returns a status for each table and the overall ETA.
func (t *tableStatusList) format() ([]string, time.Time) {
	if !t.isInitialized() {
		return nil, time.Now()
	}

	copiedRows := uint64(0)
	rowCount := uint64(0)
	result := make([]string, len(t.tableStatuses))
	for i, ts := range t.tableStatuses {
		ts.mu.Lock()
		if ts.isView {
			// views are not copied
			result[i] = fmt.Sprintf("%v is a view", ts.name)
		} else if ts.threadsStarted == 0 {
			// we haven't started yet
			result[i] = fmt.Sprintf("%v: copy not started (estimating %v rows)", ts.name, ts.rowCount)
		} else if ts.threadsDone == ts.threadCount {
			// we are done with the copy
			result[i] = fmt.Sprintf("%v: copy done, processed %v rows", ts.name, ts.copiedRows)
		} else {
			// copy is running
			result[i] = fmt.Sprintf("%v: copy running using %v threads (%v/%v rows processed)", ts.name, ts.threadsStarted-ts.threadsDone, ts.copiedRows, ts.rowCount)
		}
		copiedRows += ts.copiedRows
		rowCount += ts.rowCount
		ts.mu.Unlock()
	}
	now := time.Now()
	if rowCount == 0 || copiedRows == 0 {
		return result, now
	}
	eta := now.Add(time.Duration(float64(now.Sub(t.startTime)) * float64(rowCount) / float64(copiedRows)))
	return result, eta
}

// tableStatus keeps track of the status for a given table.
type tableStatus struct {
	name   string
	isView bool

	// mu guards all fields in the group below.
	mu             sync.Mutex
	rowCount       uint64 // set to approximate value, until copy ends
	copiedRows     uint64 // actual count of copied rows
	threadCount    int    // how many concurrent threads will copy the data
	threadsStarted int    // how many threads have started
	threadsDone    int    // how many threads are done
}

func newTableStatus(name string, isView bool, rowCount uint64) *tableStatus {
	return &tableStatus{
		name:     name,
		isView:   isView,
		rowCount: rowCount,
	}
}

func (ts *tableStatus) setThreadCount(threadCount int) {
	ts.mu.Lock()
	ts.threadCount = threadCount
	ts.mu.Unlock()
}

func (ts *tableStatus) threadStarted() {
	ts.mu.Lock()
	ts.threadsStarted++
	ts.mu.Unlock()
}

func (ts *tableStatus) threadDone() {
	ts.mu.Lock()
	ts.threadsDone++
	ts.mu.Unlock()
}

func (ts *tableStatus) addCopiedRows(copiedRows int) {
	ts.mu.Lock()
	ts.copiedRows += uint64(copiedRows)
	if ts.copiedRows > ts.rowCount {
		// since rowCount is not accurate, update it if we go past it.
		ts.rowCount = ts.copiedRows
	}
	ts.mu.Unlock()
}
