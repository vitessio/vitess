// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"encoding/gob"
	"expvar"
	"fmt"
	"strings"
	"sync"
	"time"

	"code.google.com/p/vitess/go/bson"
	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/sqltypes"
	estats "code.google.com/p/vitess/go/stats" // stats is a private type defined somewhere else in this package, so it would conflict
	"code.google.com/p/vitess/go/sync2"
	"code.google.com/p/vitess/go/vt/mysqlctl"
	"code.google.com/p/vitess/go/vt/tabletserver/proto"
)

const (
	DISABLED = iota
	ENABLED
)

// Error types for rowcache invalidator.
const (
	// Fatal Errors
	FATAL_ERROR = "InvalidatorFatal"

	// Skippable errors, recorded and skipped.
	INVALID_EVENT = "InvalidatorEvent"
)

type InvalidationError struct {
	errPos  string
	errType string
	msg     string
}

func NewInvalidationError(errType, msg, pos string) *InvalidationError {
	invErr := &InvalidationError{errType: errType, msg: msg, errPos: pos}
	return invErr
}

func (err *InvalidationError) Error() string {
	return fmt.Sprintf("%v: '%v' @ '%v'", err.errType, err.msg, err.errPos)
}

func (err *InvalidationError) isFatal() bool {
	return (err.errType != INVALID_EVENT)
}

type InvalidationProcessor struct {
	currentPosition *mysqlctl.BinlogPosition
	state           sync2.AtomicUint32
	states          *estats.States
	stateLock       sync.Mutex
	inTxn           bool
	dmlBuffer       []*proto.DmlType
	receiveEvent    mysqlctl.SendUpdateStreamResponse
	encBuf          []byte
}

var CacheInvalidationProcessor *InvalidationProcessor

func NewInvalidationProcessor() *InvalidationProcessor {
	invalidator := &InvalidationProcessor{}
	invalidator.dmlBuffer = make([]*proto.DmlType, 10)

	invalidator.receiveEvent = func(response interface{}) error {
		return invalidator.invalidateEvent(response)
	}
	gob.Register(mysqlctl.BinlogPosition{})
	invalidator.encBuf = make([]byte, 0, 100)
	return invalidator
}

func RegisterCacheInvalidator() {
	if CacheInvalidationProcessor != nil {
		return
	}
	CacheInvalidationProcessor = NewInvalidationProcessor()
	CacheInvalidationProcessor.states = estats.NewStates("", []string{
		"Disabled",
		"Enabled",
	}, time.Now(), DISABLED)
	expvar.Publish("CacheInvalidationProcessor", estats.StrFunc(func() string { return CacheInvalidationProcessor.statsJSON() }))
}

func StartRowCacheInvalidation() {
	if !shouldInvalidatorRun() {
		relog.Warning("Row-cache invalidator not being enabled, criteria not met")
		CacheInvalidationProcessor.stopRowCacheInvalidation()
		return
	}

	if CacheInvalidationProcessor.isServiceEnabled() {
		relog.Warning("Row-cache invalidator service is already enabled")
		return
	}

	CacheInvalidationProcessor.stateLock.Lock()
	if shouldInvalidatorRun() {
		CacheInvalidationProcessor.setState(ENABLED)
		CacheInvalidationProcessor.stateLock.Unlock()
	} else {
		CacheInvalidationProcessor.setState(DISABLED)
		CacheInvalidationProcessor.stateLock.Unlock()
		return
	}
	relog.Info("Starting RowCacheInvalidation Service")

	CacheInvalidationProcessor.runInvalidationLoop()
}

func StopRowCacheInvalidation() {
	if !CacheInvalidationProcessor.isServiceEnabled() {
		relog.Info("Invalidator is already disabled - NOP")
		return
	}
	CacheInvalidationProcessor.stopRowCacheInvalidation()
	relog.Info("Rowcache invalidator stopped")
}

func ShouldInvalidatorRun() bool {
	return shouldInvalidatorRun()
}

func shouldInvalidatorRun() bool {
	return IsCachePoolAvailable() && mysqlctl.IsUpdateStreamEnabled()
}

func (rowCache *InvalidationProcessor) stopRowCacheInvalidation() {
	rowCache.stateLock.Lock()
	rowCache.setState(DISABLED)
	rowCache.stateLock.Unlock()
}

func (rowCache *InvalidationProcessor) setState(state uint32) {
	rowCache.state.Set(state)
	rowCache.states.SetState(int(state))
}

func (rowCache *InvalidationProcessor) statsJSON() string {
	currentPosition := ""
	if rowCache.currentPosition != nil {
		currentPosition = rowCache.currentPosition.String()
	}
	return fmt.Sprintf("{"+
		"\n \"States\": %v,"+
		"\n \"Checkpoint\": \"%v\""+
		"\n"+
		"}", rowCache.states.String(), currentPosition)
}

func (rowCache *InvalidationProcessor) isServiceEnabled() bool {
	return rowCache.state.Get() == ENABLED
}

func (rowCache *InvalidationProcessor) updateErrCounters(err *InvalidationError) {
	relog.Error(err.Error())
	if errorStats == nil {
		relog.Warning("errorStats is not initialized")
		return
	}
	errorStats.Add(err.errType, 1)
}

func (rowCache *InvalidationProcessor) invalidateEvent(response interface{}) error {
	if !shouldInvalidatorRun() || !rowCache.isServiceEnabled() {
		return NewInvalidationError(FATAL_ERROR, "Rowcache invalidator is not available", "")
	}
	updateResponse, ok := response.(*mysqlctl.UpdateResponse)
	if !ok {
		return NewInvalidationError(FATAL_ERROR, "Invalid Reponse type", "")
	}
	rowCache.currentPosition = &updateResponse.BinlogPosition
	return rowCache.processEvent(updateResponse)
}

func (rowCache *InvalidationProcessor) getCheckpoint() (*mysqlctl.BinlogPosition, bool) {
	encPosition, err := GetCurrentInvalidationPosition()
	if err != nil {
		relog.Warning("Error in getting saved position, %v", err)
		return nil, false
	}
	// At startup there will be no saved position
	if encPosition == nil {
		return nil, true
	}
	currentPosition := new(mysqlctl.BinlogPosition)
	err = bson.Unmarshal(encPosition, currentPosition)
	if err != nil {
		relog.Warning("Error in decoding saved position, %v", err)
		return nil, false
	}
	if currentPosition == nil {
		relog.Warning("Error in getting saved position, %v", err)
		return nil, false
	}
	return currentPosition, true
}

func (rowCache *InvalidationProcessor) stopCache(reason string) {
	relog.Warning("Stopping rowcache invalidation, reason: '%v'", reason)
	rowCache.stopRowCacheInvalidation()
	if IsCachePoolAvailable() {
		relog.Warning("Disallowing Query Service as row-cache invalidator cannot run")
		DisallowQueries(false)
	}
}

func (rowCache *InvalidationProcessor) runInvalidationLoop() {
	var err error
	purgeCache := false
	purgeReason := ""

	replPos, err := mysqlctl.GetReplicationPosition()
	if err != nil {
		rErr := NewInvalidationError(FATAL_ERROR, fmt.Sprintf("Cannot determine replication position %v", err), "")
		rowCache.updateErrCounters(rErr)
		rowCache.stopCache(rErr.Error())
		return
	}

	startPosition := &mysqlctl.BinlogPosition{Position: *replPos}
	checkpoint, ok := rowCache.getCheckpoint()

	// Cannot resume from last checkpoint position.
	// Purging the cache.
	if !ok {
		purgeCache = true
		purgeReason = "Error in locating invalidation checkpoint"
	} else if checkpoint == nil {
		//NOTE: not purging the cache here - since no checkpoint is found, assuming cache is empty.
		relog.Info("No saved position found, invalidation starting at current replication position.")
	} else if !isCheckpointValid(&checkpoint.Position, replPos) {
		purgeCache = true
		purgeReason = "Invalidation checkpoint too old"
	} else {
		relog.Info("Starting at saved checkpoint %v", checkpoint.String())
		startPosition = checkpoint
	}

	if purgeCache {
		PurgeRowCache()
		startPosition = &mysqlctl.BinlogPosition{Position: *replPos}
		relog.Warning("Purging cache because '%v'", purgeReason)
	}

	relog.Info("Starting @ %v", startPosition.String())
	req := &mysqlctl.UpdateStreamRequest{StartPosition: *startPosition}
	err = mysqlctl.ServeUpdateStream(req, rowCache.receiveEvent)
	if err != nil {
		relog.Error("mysqlctl.ServeUpdateStream returned err '%v'", err.Error())
		if rErr, ok := err.(*InvalidationError); ok {
			rowCache.updateErrCounters(rErr)
		}
		rowCache.stopCache(fmt.Sprintf("Unexpected or fatal error, '%v'", err.Error()))
	}
}

func isCheckpointValid(checkpoint, repl *mysqlctl.ReplicationCoordinates) bool {
	if checkpoint.MasterFilename != repl.MasterFilename {
		// FIXME(shrutip): should this be made more granular ?
		// NOTE(shrutip): this could be made more sophisticated if needed
		// later by allowing one consecutive filename, typical binlogs last > 2hrs
		// so this is good for now.
		return false
	}
	return true
}

func (rowCache *InvalidationProcessor) processEvent(event *mysqlctl.UpdateResponse) error {
	position := ""
	if event.BinlogPosition.Valid() {
		position = event.BinlogPosition.String()
	}
	if event.Error != "" {
		relog.Error("Update stream returned error '%v'", event.Error)
		// Check if update stream error is fatal, else record it and move on.
		if strings.HasPrefix(event.Error, mysqlctl.FATAL) {
			relog.Info("Returning Service Error")
			return NewInvalidationError(FATAL_ERROR, event.Error, position)
		}
		rowCache.updateErrCounters(NewInvalidationError(INVALID_EVENT, event.Error, position))
		return nil
	}

	if !event.BinlogPosition.Valid() {
		rowCache.updateErrCounters(NewInvalidationError(INVALID_EVENT, "no error, position is not set", ""))
		return nil
	}

	var err error
	switch event.EventData.SqlType {
	case mysqlctl.DDL:
		err = rowCache.handleDdlEvent(event)
		if err != nil {
			return err
		}
	case mysqlctl.BEGIN:
		rowCache.dmlBuffer = rowCache.dmlBuffer[:0]
		if rowCache.inTxn {
			rowCache.updateErrCounters(NewInvalidationError(INVALID_EVENT, "Invalid 'BEGIN' event, transaction already in progress", position))
			return nil
		}
		rowCache.inTxn = true
	case mysqlctl.COMMIT:
		if !rowCache.inTxn {
			rowCache.updateErrCounters(NewInvalidationError(INVALID_EVENT, "Invalid 'COMMIT' event for a non-transaction", position))
			return nil
		}
		err = rowCache.handleTxn(event)
		if err != nil {
			return err
		}
		rowCache.inTxn = false
		rowCache.dmlBuffer = rowCache.dmlBuffer[:0]
	case "insert", "update", "delete":
		dml, err := rowCache.buildDmlData(event)
		if err != nil {
			return err
		}
		if dml != nil {
			rowCache.dmlBuffer = append(rowCache.dmlBuffer, dml)
		}
	default:
		rowCache.updateErrCounters(NewInvalidationError(INVALID_EVENT, fmt.Sprintf("Unknown SqlType, %v %v", event.EventData.SqlType, event.EventData.Sql), position))
		//return NewInvalidationError(INVALID_EVENT, fmt.Sprintf("Unknown SqlType, %v %v", event.EventData.SqlType, event.EventData.Sql))
	}
	return nil
}

func isDmlEvent(sqlType string) bool {
	switch sqlType {
	case "insert", "update", "delete":
		return true
	}
	return false
}

func (rowCache *InvalidationProcessor) buildDmlData(event *mysqlctl.UpdateResponse) (*proto.DmlType, error) {
	if !isDmlEvent(event.SqlType) {
		rowCache.updateErrCounters(NewInvalidationError(INVALID_EVENT, fmt.Sprintf("Bad Dml type, '%v'", event.SqlType), event.BinlogPosition.String()))
		return nil, nil
	}
	dml := new(proto.DmlType)
	dml.Table = event.TableName
	dml.Keys = make([]interface{}, 0, len(event.PkValues))
	sqlTypeKeys := make([]sqltypes.Value, 0, len(event.PkColNames))
	for _, pkTuple := range event.PkValues {
		sqlTypeKeys = sqlTypeKeys[:0]
		if len(pkTuple) == 0 {
			continue
		}
		for _, pkVal := range pkTuple {
			key, err := sqltypes.BuildValue(pkVal)
			if err != nil {
				rowCache.updateErrCounters(NewInvalidationError(INVALID_EVENT, fmt.Sprintf("Error building invalidation key '%v'", err), event.BinlogPosition.String()))
				return nil, nil
			}
			sqlTypeKeys = append(sqlTypeKeys, key)
		}
		invalidateKey := buildKey(sqlTypeKeys)
		if invalidateKey != "" {
			dml.Keys = append(dml.Keys, invalidateKey)
		}
	}
	return dml, nil
}

func (rowCache *InvalidationProcessor) handleTxn(commitEvent *mysqlctl.UpdateResponse) error {
	var err error
	defer func() {
		if x := recover(); x != nil {
			if terr, ok := x.(*TabletError); ok {
				rowCache.updateErrCounters(NewInvalidationError(INVALID_EVENT, terr.Error(), commitEvent.BinlogPosition.String()))
			} else {
				err = NewInvalidationError(FATAL_ERROR, "handleTxn failed", commitEvent.BinlogPosition.String())
			}
		}
	}()

	if len(rowCache.dmlBuffer) == 0 {
		return nil
	}
	rowCache.encBuf = rowCache.encBuf[:0]
	cacheInvalidate := new(proto.CacheInvalidate)
	rowCache.encBuf, err = bson.Marshal(&commitEvent.BinlogPosition)
	if err != nil {
		return NewInvalidationError(FATAL_ERROR, fmt.Sprintf("Error in encoding position, %v", err), commitEvent.BinlogPosition.String())
	}
	cacheInvalidate.Position = rowCache.encBuf
	cacheInvalidate.Dmls = make([]proto.DmlType, 0, len(rowCache.dmlBuffer))
	for _, dml := range rowCache.dmlBuffer {
		cacheInvalidate.Dmls = append(cacheInvalidate.Dmls, *dml)
	}
	InvalidateForDml(cacheInvalidate)
	return nil
}

func (rowCache *InvalidationProcessor) handleDdlEvent(ddlEvent *mysqlctl.UpdateResponse) error {
	var err error
	defer func() {
		if x := recover(); x != nil {
			if terr, ok := x.(*TabletError); ok {
				rowCache.updateErrCounters(NewInvalidationError(INVALID_EVENT, terr.Error(), ddlEvent.BinlogPosition.String()))
			} else {
				err = NewInvalidationError(FATAL_ERROR, "ddlEvent failed", ddlEvent.BinlogPosition.String())
			}
		}
	}()

	if ddlEvent.Sql == "" {
		rowCache.updateErrCounters(NewInvalidationError(INVALID_EVENT, "Empty ddl sql", ddlEvent.BinlogPosition.String()))
		return nil
		//return NewInvalidationError(INVALID_EVENT, "Empty ddl sql", ddlEvent.BinlogPosition.String())
	}
	rowCache.encBuf = rowCache.encBuf[:0]
	ddlInvalidate := new(proto.DDLInvalidate)
	rowCache.encBuf, err = bson.Marshal(&ddlEvent.BinlogPosition)
	if err != nil {
		return NewInvalidationError(FATAL_ERROR, fmt.Sprintf("Error in encoding position, %v", err), ddlEvent.BinlogPosition.String())
	}
	ddlInvalidate.Position = rowCache.encBuf
	ddlInvalidate.DDL = ddlEvent.Sql
	InvalidateForDDL(ddlInvalidate)
	return nil
}
