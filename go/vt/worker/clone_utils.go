// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package worker

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"text/template"
	"time"

	"code.google.com/p/go.net/context"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sqltypes"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/wrangler"
)

//
// This file contains utility functions for clone workers.
//

// tableStatus keeps track of the status for a given table
type tableStatus struct {
	name   string
	isView bool

	// all subsequent fields are protected by the mutex
	mu             sync.Mutex
	rowCount       uint64 // set to approximate value, until copy ends
	copiedRows     uint64 // actual count of copied rows
	threadCount    int    // how many concurrent threads will copy the data
	threadsStarted int    // how many threads have started
	threadsDone    int    // how many threads are done
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

func formatTableStatuses(tableStatuses []*tableStatus, startTime time.Time) ([]string, time.Time) {
	copiedRows := uint64(0)
	rowCount := uint64(0)
	result := make([]string, len(tableStatuses))
	for i, ts := range tableStatuses {
		ts.mu.Lock()
		if ts.isView {
			// views are not copied
			result[i] = fmt.Sprintf("%v is a view", ts.name)
		} else if ts.threadsStarted == 0 {
			// we haven't started yet
			result[i] = fmt.Sprintf("%v: copy not started (estimating %v rows)", ts.name, ts.rowCount)
		} else if ts.threadsDone == ts.threadCount {
			// we are done with the copy
			result[i] = fmt.Sprintf("%v: copy done, copied %v rows", ts.name, ts.rowCount)
		} else {
			// copy is running
			result[i] = fmt.Sprintf("%v: copy running using %v threads (%v/%v rows)", ts.name, ts.threadsStarted-ts.threadsDone, ts.copiedRows, ts.rowCount)
		}
		copiedRows += ts.copiedRows
		rowCount += ts.rowCount
		ts.mu.Unlock()
	}
	now := time.Now()
	if rowCount == 0 || copiedRows == 0 {
		return result, now
	}
	eta := now.Add(time.Duration(float64(now.Sub(startTime)) * float64(rowCount) / float64(copiedRows)))
	return result, eta
}

// fillStringTemplate returns the string template filled
func fillStringTemplate(tmpl string, vars interface{}) (string, error) {
	myTemplate := template.Must(template.New("").Parse(tmpl))
	data := new(bytes.Buffer)
	if err := myTemplate.Execute(data, vars); err != nil {
		return "", err
	}
	return data.String(), nil
}

// runSqlCommands will send the sql commands to the remote tablet.
func runSqlCommands(wr *wrangler.Wrangler, ti *topo.TabletInfo, commands []string, abort chan struct{}, disableBinLogs bool) error {
	for _, command := range commands {
		command, err := fillStringTemplate(command, map[string]string{"DatabaseName": ti.DbName()})
		if err != nil {
			return fmt.Errorf("fillStringTemplate failed: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.TODO(), 30*time.Second)
		_, err = wr.TabletManagerClient().ExecuteFetch(ctx, ti, command, 0, false, disableBinLogs)
		if err != nil {
			return err
		}
		cancel()

		// check on abort
		select {
		case <-abort:
			return nil
		default:
			break
		}
	}

	return nil
}

// findChunks returns an array of chunks to use for splitting up a table
// into multiple data chunks. It only works for tables with a primary key
// (and the primary key first column is an integer type).
// The array will always look like:
// "", "value1", "value2", ""
// A non-split tablet will just return:
// "", ""
func findChunks(wr *wrangler.Wrangler, ti *topo.TabletInfo, td *myproto.TableDefinition, minTableSizeForSplit uint64, sourceReaderCount int) ([]string, error) {
	result := []string{"", ""}

	// eliminate a few cases we don't split tables for
	if len(td.PrimaryKeyColumns) == 0 {
		// no primary key, what can we do?
		return result, nil
	}
	if td.DataLength < minTableSizeForSplit {
		// table is too small to split up
		return result, nil
	}

	// get the min and max of the leading column of the primary key
	query := fmt.Sprintf("SELECT MIN(%v), MAX(%v) FROM %v.%v", td.PrimaryKeyColumns[0], td.PrimaryKeyColumns[0], ti.DbName(), td.Name)
	ctx, cancel := context.WithTimeout(context.TODO(), 30*time.Second)
	qr, err := wr.TabletManagerClient().ExecuteFetch(ctx, ti, query, 1, true, false)
	if err != nil {
		wr.Logger().Infof("Not splitting table %v into multiple chunks: %v", td.Name, err)
		return result, nil
	}
	cancel()
	if len(qr.Rows) != 1 {
		wr.Logger().Infof("Not splitting table %v into multiple chunks, cannot get min and max", td.Name)
		return result, nil
	}
	if qr.Rows[0][0].IsNull() || qr.Rows[0][1].IsNull() {
		wr.Logger().Infof("Not splitting table %v into multiple chunks, min or max is NULL: %v %v", td.Name, qr.Rows[0][0], qr.Rows[0][1])
		return result, nil
	}
	switch qr.Fields[0].Type {
	case mproto.VT_TINY, mproto.VT_SHORT, mproto.VT_LONG, mproto.VT_LONGLONG, mproto.VT_INT24:
		minNumeric := sqltypes.MakeNumeric(qr.Rows[0][0].Raw())
		maxNumeric := sqltypes.MakeNumeric(qr.Rows[0][1].Raw())
		if qr.Rows[0][0].Raw()[0] == '-' {
			// signed values, use int64
			min, err := minNumeric.ParseInt64()
			if err != nil {
				wr.Logger().Infof("Not splitting table %v into multiple chunks, cannot convert min: %v %v", td.Name, minNumeric, err)
				return result, nil
			}
			max, err := maxNumeric.ParseInt64()
			if err != nil {
				wr.Logger().Infof("Not splitting table %v into multiple chunks, cannot convert max: %v %v", td.Name, maxNumeric, err)
				return result, nil
			}
			interval := (max - min) / int64(sourceReaderCount)
			if interval == 0 {
				wr.Logger().Infof("Not splitting table %v into multiple chunks, interval=0: %v %v", td.Name, max, min)
				return result, nil
			}

			result = make([]string, sourceReaderCount+1)
			result[0] = ""
			result[sourceReaderCount] = ""
			for i := int64(1); i < int64(sourceReaderCount); i++ {
				result[i] = fmt.Sprintf("%v", min+interval*i)
			}
			return result, nil
		}

		// unsigned values, use uint64
		min, err := minNumeric.ParseUint64()
		if err != nil {
			wr.Logger().Infof("Not splitting table %v into multiple chunks, cannot convert min: %v %v", td.Name, minNumeric, err)
			return result, nil
		}
		max, err := maxNumeric.ParseUint64()
		if err != nil {
			wr.Logger().Infof("Not splitting table %v into multiple chunks, cannot convert max: %v %v", td.Name, maxNumeric, err)
			return result, nil
		}
		interval := (max - min) / uint64(sourceReaderCount)
		if interval == 0 {
			wr.Logger().Infof("Not splitting table %v into multiple chunks, interval=0: %v %v", td.Name, max, min)
			return result, nil
		}

		result = make([]string, sourceReaderCount+1)
		result[0] = ""
		result[sourceReaderCount] = ""
		for i := uint64(1); i < uint64(sourceReaderCount); i++ {
			result[i] = fmt.Sprintf("%v", min+interval*i)
		}
		return result, nil

	case mproto.VT_FLOAT, mproto.VT_DOUBLE:
		min, err := strconv.ParseFloat(qr.Rows[0][0].String(), 64)
		if err != nil {
			wr.Logger().Infof("Not splitting table %v into multiple chunks, cannot convert min: %v %v", td.Name, qr.Rows[0][0], err)
			return result, nil
		}
		max, err := strconv.ParseFloat(qr.Rows[0][1].String(), 64)
		if err != nil {
			wr.Logger().Infof("Not splitting table %v into multiple chunks, cannot convert max: %v %v", td.Name, qr.Rows[0][1].String(), err)
			return result, nil
		}
		interval := (max - min) / float64(sourceReaderCount)
		if interval == 0 {
			wr.Logger().Infof("Not splitting table %v into multiple chunks, interval=0: %v %v", td.Name, max, min)
			return result, nil
		}

		result = make([]string, sourceReaderCount+1)
		result[0] = ""
		result[sourceReaderCount] = ""
		for i := 1; i < sourceReaderCount; i++ {
			result[i] = fmt.Sprintf("%v", min+interval*float64(i))
		}
		return result, nil
	}

	wr.Logger().Infof("Not splitting table %v into multiple chunks, primary key not numeric", td.Name)
	return result, nil
}

// buildSQLFromChunks returns the SQL command to run to insert the data
// using the chunks definitions into the provided table.
func buildSQLFromChunks(wr *wrangler.Wrangler, td *myproto.TableDefinition, chunks []string, chunkIndex int, source string) string {
	selectSQL := "SELECT " + strings.Join(td.Columns, ", ") + " FROM " + td.Name
	if chunks[chunkIndex] != "" || chunks[chunkIndex+1] != "" {
		wr.Logger().Infof("Starting to stream all data from tablet %v table %v between '%v' and '%v'", source, td.Name, chunks[chunkIndex], chunks[chunkIndex+1])
		clauses := make([]string, 0, 2)
		if chunks[chunkIndex] != "" {
			clauses = append(clauses, td.PrimaryKeyColumns[0]+">="+chunks[chunkIndex])
		}
		if chunks[chunkIndex+1] != "" {
			clauses = append(clauses, td.PrimaryKeyColumns[0]+"<"+chunks[chunkIndex+1])
		}
		selectSQL += " WHERE " + strings.Join(clauses, " AND ")
	} else {
		wr.Logger().Infof("Starting to stream all data from tablet %v table %v", source, td.Name)
	}
	if len(td.PrimaryKeyColumns) > 0 {
		selectSQL += " ORDER BY " + strings.Join(td.PrimaryKeyColumns, ", ")
	}
	return selectSQL
}

// makeValueString returns a string that contains all the passed-in rows
// as an insert SQL command's parameters.
func makeValueString(fields []mproto.Field, rows [][]sqltypes.Value) string {
	buf := bytes.Buffer{}
	for i, row := range rows {
		if i > 0 {
			buf.Write([]byte(",("))
		} else {
			buf.WriteByte('(')
		}
		for j, value := range row {
			if j > 0 {
				buf.WriteByte(',')
			}
			// convert value back to its original type
			if !value.IsNull() {
				switch fields[j].Type {
				case mproto.VT_TINY, mproto.VT_SHORT, mproto.VT_LONG, mproto.VT_LONGLONG, mproto.VT_INT24:
					value = sqltypes.MakeNumeric(value.Raw())
				case mproto.VT_FLOAT, mproto.VT_DOUBLE:
					value = sqltypes.MakeFractional(value.Raw())
				}
			}
			value.EncodeSql(&buf)
		}
		buf.WriteByte(')')
	}
	return buf.String()
}

// executeFetchLoop loops over the provided insertChannel
// and sends the commands to the provided tablet.
func executeFetchLoop(wr *wrangler.Wrangler, ti *topo.TabletInfo, insertChannel chan string, abort chan struct{}, disableBinLogs bool) error {
	for {
		select {
		case cmd, ok := <-insertChannel:
			if !ok {
				// no more to read, we're done
				return nil
			}
			cmd = "INSERT INTO `" + ti.DbName() + "`." + cmd
			ctx, cancel := context.WithTimeout(context.TODO(), 30*time.Second)
			_, err := wr.TabletManagerClient().ExecuteFetch(ctx, ti, cmd, 0, false, disableBinLogs)
			if err != nil {
				return fmt.Errorf("ExecuteFetch failed: %v", err)
			}
			cancel()
		case <-abort:
			// FIXME(alainjobart): note this select case
			// could be starved here, and we might miss
			// the abort in some corner cases.
			return nil
		}
	}
}
