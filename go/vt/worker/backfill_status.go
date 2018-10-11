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

package worker

import (
	"fmt"
	"sync"
	"time"

	"vitess.io/vitess/go/vt/proto/tabletmanagerdata"
)

// backfillStatus keeps track of the status for a given table.
type backfillStatus struct {
	name string

	// initialized is true when initialize() was called.
	initialized bool

	// startTime records the time initialize() was called.
	// it's a write-once field.
	startTime time.Time

	// mu guards all fields in the group below.
	mu             sync.Mutex
	rowCount       uint64 // set to approximate value, until copy ends
	copiedRows     uint64 // actual count of copied rows
	threadCount    int    // how many concurrent threads will copy the data
	threadsStarted int    // how many threads have started
	threadsDone    int    // how many threads are done
}

func (bs *backfillStatus) initialize(sourceTableDefinition *tabletmanagerdata.TableDefinition) {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	if bs.initialized {
		panic(fmt.Errorf("backfillStatus is already initialized: %v", bs))
	}

	bs.name = sourceTableDefinition.Name
	bs.rowCount = sourceTableDefinition.RowCount
	bs.startTime = time.Now()

	bs.initialized = true
}

func (bs *backfillStatus) setThreadCount(threadCount int) {
	bs.mu.Lock()
	bs.threadCount = threadCount
	bs.mu.Unlock()
}

func (bs *backfillStatus) threadStarted() {
	bs.mu.Lock()
	bs.threadsStarted++
	bs.mu.Unlock()
}

func (bs *backfillStatus) threadDone() {
	bs.mu.Lock()
	bs.threadsDone++
	bs.mu.Unlock()
}

func (bs *backfillStatus) addCopiedRows(copiedRows int) {
	bs.mu.Lock()
	bs.copiedRows += uint64(copiedRows)
	if bs.copiedRows > bs.rowCount {
		// since rowCount is not accurate, update it if we go past it.
		bs.rowCount = bs.copiedRows
	}
	bs.mu.Unlock()
}

// format returns a status for each table and the overall ETA.
func (bs *backfillStatus) format() (string, time.Time) {
	copiedRows := uint64(0)
	rowCount := uint64(0)
	var result string
	bs.mu.Lock()
	if bs.threadsStarted == 0 {
		// we haven'bs started yet
		result = fmt.Sprintf("%v: copy not started (estimating %v rows)", bs.name, bs.rowCount)
	} else if bs.threadsDone == bs.threadCount {
		// we are done with the copy
		result = fmt.Sprintf("%v: copy done, processed %v rows", bs.name, bs.copiedRows)
	} else {
		// copy is running
		// Display 0% if rowCount is 0 because the actual number of rows can be > 0
		// due to InnoDB's imperfect statistics.
		percentage := 0.0
		if bs.rowCount > 0 {
			percentage = float64(bs.copiedRows) / float64(bs.rowCount) * 100.0
		}
		result = fmt.Sprintf("%v: copy running using %v threads (%v/%v rows processed, %.1f%%)", bs.name, bs.threadsStarted-bs.threadsDone, bs.copiedRows, bs.rowCount, percentage)
	}
	copiedRows += bs.copiedRows
	rowCount += bs.rowCount
	bs.mu.Unlock()

	now := time.Now()
	if rowCount == 0 || copiedRows == 0 {
		return result, now
	}
	eta := now.Add(time.Duration(float64(now.Sub(bs.startTime)) * float64(rowCount) / float64(copiedRows)))
	return result, eta
}
