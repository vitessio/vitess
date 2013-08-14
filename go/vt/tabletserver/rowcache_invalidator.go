// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"encoding/gob"
	"fmt"
	"strings"
	"sync"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/bson"
	"github.com/youtube/vitess/go/sqltypes"
	estats "github.com/youtube/vitess/go/stats" // stats is a private type defined somewhere else in this package, so it would conflict
	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	cproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
	"github.com/youtube/vitess/go/vt/tabletserver/proto"
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
	currentPosition *cproto.BinlogPosition
	state           sync2.AtomicInt64
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
	gob.Register(cproto.BinlogPosition{})
	invalidator.encBuf = make([]byte, 0, 100)
	return invalidator
}

func RegisterCacheInvalidator() {
	if CacheInvalidationProcessor != nil {
		return
	}
	CacheInvalidationProcessor = NewInvalidationProcessor()
	CacheInvalidationProcessor.states = estats.NewStates("CacheInvalidationState", []string{
		"Disabled",
		"Enabled",
	}, time.Now(), DISABLED)
	estats.Publish("CacheInvalidationCheckPoint", estats.StringFunc(func() string {
		if pos := CacheInvalidationProcessor.currentPosition; pos != nil {
			return pos.String()
		}
		return ""
	}))
}

func StartRowCacheInvalidation() {
	if !shouldInvalidatorRun() {
		log.Warningf("Row-cache invalidator not being enabled, criteria not met")
		CacheInvalidationProcessor.stopRowCacheInvalidation()
		return
	}

	if CacheInvalidationProcessor.isServiceEnabled() {
		log.Warningf("Row-cache invalidator service is already enabled")
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
	log.Infof("Starting RowCacheInvalidation Service")

	CacheInvalidationProcessor.runInvalidationLoop()
}

func StopRowCacheInvalidation() {
	if !CacheInvalidationProcessor.isServiceEnabled() {
		log.Infof("Invalidator is already disabled - NOP")
		return
	}
	CacheInvalidationProcessor.stopRowCacheInvalidation()
	log.Infof("Rowcache invalidator stopped")
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

func (rowCache *InvalidationProcessor) setState(state int64) {
	rowCache.state.Set(state)
	rowCache.states.SetState(state)
}

func (rowCache *InvalidationProcessor) isServiceEnabled() bool {
	return rowCache.state.Get() == ENABLED
}

func (rowCache *InvalidationProcessor) updateErrCounters(err *InvalidationError) {
	log.Errorf(err.Error())
	if errorStats == nil {
		log.Warningf("errorStats is not initialized")
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
	rowCache.currentPosition = &updateResponse.Coord
	return rowCache.processEvent(updateResponse)
}

func (rowCache *InvalidationProcessor) stopCache(reason string) {
	log.Warningf("Stopping rowcache invalidation, reason: '%v'", reason)
	rowCache.stopRowCacheInvalidation()
	if IsCachePoolAvailable() {
		log.Warningf("Disallowing Query Service as row-cache invalidator cannot run")
		DisallowQueries(false)
	}
}

func (rowCache *InvalidationProcessor) runInvalidationLoop() {
	var err error

	replPos, err := mysqlctl.GetReplicationPosition()
	if err != nil {
		rErr := NewInvalidationError(FATAL_ERROR, fmt.Sprintf("Cannot determine replication position %v", err), "")
		rowCache.updateErrCounters(rErr)
		rowCache.stopCache(rErr.Error())
		return
	}

	startPosition := &cproto.BinlogPosition{Position: *replPos}

	log.Infof("Starting @ %v", startPosition.String())
	req := &mysqlctl.UpdateStreamRequest{StartPosition: *startPosition}
	err = mysqlctl.ServeUpdateStream(req, rowCache.receiveEvent)
	if err != nil {
		log.Errorf("mysqlctl.ServeUpdateStream returned err '%v'", err.Error())
		if rErr, ok := err.(*InvalidationError); ok {
			rowCache.updateErrCounters(rErr)
		}
		rowCache.stopCache(fmt.Sprintf("Unexpected or fatal error, '%v'", err.Error()))
	}
}

func (rowCache *InvalidationProcessor) processEvent(event *mysqlctl.UpdateResponse) error {
	position := ""
	if event.Coord.Valid() {
		position = event.Coord.String()
	}
	if event.Error != "" {
		log.Errorf("Update stream returned error '%v'", event.Error)
		// Check if update stream error is fatal, else record it and move on.
		if strings.HasPrefix(event.Error, mysqlctl.FATAL) {
			log.Infof("Returning Service Error")
			return NewInvalidationError(FATAL_ERROR, event.Error, position)
		}
		rowCache.updateErrCounters(NewInvalidationError(INVALID_EVENT, event.Error, position))
		return nil
	}

	if !event.Coord.Valid() {
		rowCache.updateErrCounters(NewInvalidationError(INVALID_EVENT, "no error, position is not set", ""))
		return nil
	}

	var err error
	switch event.Data.SqlType {
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
		rowCache.updateErrCounters(NewInvalidationError(INVALID_EVENT, fmt.Sprintf("Unknown SqlType, %v %v", event.Data.SqlType, event.Data.Sql), position))
		//return NewInvalidationError(INVALID_EVENT, fmt.Sprintf("Unknown SqlType, %v %v", event.Data.SqlType, event.Data.Sql))
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
	if !isDmlEvent(event.Data.SqlType) {
		rowCache.updateErrCounters(NewInvalidationError(INVALID_EVENT, fmt.Sprintf("Bad Dml type, '%v'", event.Data.SqlType), event.Coord.String()))
		return nil, nil
	}
	dml := new(proto.DmlType)
	dml.Table = event.Data.TableName
	dml.Keys = make([]interface{}, 0, len(event.Data.PkValues))
	sqlTypeKeys := make([]sqltypes.Value, 0, len(event.Data.PkColNames))
	for _, pkTuple := range event.Data.PkValues {
		sqlTypeKeys = sqlTypeKeys[:0]
		if len(pkTuple) == 0 {
			continue
		}
		for _, pkVal := range pkTuple {
			key, err := sqltypes.BuildValue(pkVal)
			if err != nil {
				rowCache.updateErrCounters(NewInvalidationError(INVALID_EVENT, fmt.Sprintf("Error building invalidation key '%v'", err), event.Coord.String()))
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
				rowCache.updateErrCounters(NewInvalidationError(INVALID_EVENT, terr.Error(), commitEvent.Coord.String()))
			} else {
				err = NewInvalidationError(FATAL_ERROR, "handleTxn failed", commitEvent.Coord.String())
			}
		}
	}()

	if len(rowCache.dmlBuffer) == 0 {
		return nil
	}
	rowCache.encBuf = rowCache.encBuf[:0]
	cacheInvalidate := new(proto.CacheInvalidate)
	rowCache.encBuf, err = bson.Marshal(&commitEvent.Coord)
	if err != nil {
		return NewInvalidationError(FATAL_ERROR, fmt.Sprintf("Error in encoding position, %v", err), commitEvent.Coord.String())
	}
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
				rowCache.updateErrCounters(NewInvalidationError(INVALID_EVENT, terr.Error(), ddlEvent.Coord.String()))
			} else {
				err = NewInvalidationError(FATAL_ERROR, "ddlEvent failed", ddlEvent.Coord.String())
			}
		}
	}()

	if ddlEvent.Data.Sql == "" {
		rowCache.updateErrCounters(NewInvalidationError(INVALID_EVENT, "Empty ddl sql", ddlEvent.Coord.String()))
		return nil
		//return NewInvalidationError(INVALID_EVENT, "Empty ddl sql", ddlEvent.Coord.String())
	}
	rowCache.encBuf = rowCache.encBuf[:0]
	ddlInvalidate := new(proto.DDLInvalidate)
	rowCache.encBuf, err = bson.Marshal(&ddlEvent.Coord)
	if err != nil {
		return NewInvalidationError(FATAL_ERROR, fmt.Sprintf("Error in encoding position, %v", err), ddlEvent.Coord.String())
	}
	ddlInvalidate.DDL = ddlEvent.Data.Sql
	InvalidateForDDL(ddlInvalidate)
	return nil
}
