// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"fmt"
	"sync"
	"sync/atomic"

	"code.google.com/p/vitess/go/relog"
	"code.google.com/p/vitess/go/sqltypes"
	"code.google.com/p/vitess/go/vt/mysqlctl"
	"code.google.com/p/vitess/go/vt/tabletserver/proto"
)

type InvalidationResponse func(response interface{}) error

const (
	DISABLED = iota
	ENABLED
)

type InvalidationProcessor struct {
	currentPosition string
	state           uint32
	stateLock       sync.Mutex
	inTxn           bool
	dmlBuffer       []*proto.DmlType
	receiveEvent    mysqlctl.SendUpdateStreamResponse
}

var CacheInvalidationProcessor *InvalidationProcessor

func NewInvalidationProcessor() *InvalidationProcessor {
	invalidator := &InvalidationProcessor{}
	invalidator.dmlBuffer = make([]*proto.DmlType, 10)

	invalidator.receiveEvent = func(response interface{}) error {
		return invalidator.invalidateEvent(response)
	}
	return invalidator
}

func RegisterCacheInvalidator() {
	if CacheInvalidationProcessor != nil {
		relog.Warning("Row cache invalidation service has already been initialized.")
		return
	}
	CacheInvalidationProcessor = NewInvalidationProcessor()
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
		atomic.StoreUint32(&CacheInvalidationProcessor.state, ENABLED)
		CacheInvalidationProcessor.stateLock.Unlock()
	} else {
		atomic.StoreUint32(&CacheInvalidationProcessor.state, DISABLED)
		CacheInvalidationProcessor.stateLock.Unlock()
		return
	}
	CacheInvalidationProcessor.startInvalidation()
	relog.Info("Starting RowCacheInvalidation Service")
}

func StopRowCacheInvalidation() {
	CacheInvalidationProcessor.stopRowCacheInvalidation()
}

func (rowCache *InvalidationProcessor) stopRowCacheInvalidation() {
	rowCache.stateLock.Lock()
	atomic.StoreUint32(&rowCache.state, DISABLED)
	rowCache.stateLock.Unlock()
}

func ShouldInvalidatorRun() bool {
	return shouldInvalidatorRun()
}

func shouldInvalidatorRun() bool {
	return IsCachePoolAvailable() && mysqlctl.IsUpdateStreamEnabled() && mysqlctl.IsUpdateStreamUsingRelayLogs()
}

func (rowCache *InvalidationProcessor) isServiceEnabled() bool {
	return atomic.LoadUint32(&rowCache.state) == ENABLED
}

func (rowCache *InvalidationProcessor) invalidateEvent(response interface{}) error {
	if !shouldInvalidatorRun() || !rowCache.isServiceEnabled() {
		return fmt.Errorf("Row-cache invalidator is not available")
	}
	updateResponse, ok := response.(*mysqlctl.UpdateResponse)
	if !ok {
		return fmt.Errorf("Invalid response type")
	}
	err := rowCache.processEvent(updateResponse)
	if err != nil {
		return err
	}
	return nil
}

func (rowCache *InvalidationProcessor) getStartPosition() string {
	purgeCache := false
	currentPosition, err := GetCurrentInvalidationPosition()
	if err != nil || currentPosition == "" {
		purgeCache = true
		relog.Warning("Purging the cache and starting at current replication position - No position set")
	} else {
		replPosition, err := mysqlctl.DecodePositionToCoordinates(currentPosition)
		if err != nil || !mysqlctl.IsRelayPositionValid(replPosition) {
			purgeCache = true
			relog.Warning("Purging the cache and starting at current replication position - Invalid start position")
		}
	}

	if purgeCache {
		PurgeRowCache()
		repl, err := mysqlctl.GetCurrentReplicationPosition()
		if err != nil {
			return ""
		}
		currentPosition, err = mysqlctl.EncodeCoordinatesToPosition(repl)
		if err != nil {
			return ""
		}
	}
	return currentPosition
}

func (rowCache *InvalidationProcessor) startInvalidation() {
	startPosition := rowCache.getStartPosition()
	if startPosition == "" {
		relog.Warning("Stopping row-cache invalidation, cannot determine start position correctly")
		rowCache.stopRowCacheInvalidation()
		if IsCachePoolAvailable() {
			relog.Warning("Disallowing Query Service as row-cache invalidator cannot run")
			DisallowQueries(false)
		}
		return
	}
	req := &mysqlctl.UpdateStreamRequest{StartPosition: startPosition}
	err := mysqlctl.ServeUpdateStream(req, rowCache.receiveEvent)
	if err != nil {
		relog.Warning("Stopping row-cache invalidation, error in update stream service: %v", err)
		rowCache.stopRowCacheInvalidation()
		if IsCachePoolAvailable() {
			relog.Warning("Disallowing Query Service as row-cache invalidator cannot run")
			DisallowQueries(false)
		}
	}
}

func (rowCache *InvalidationProcessor) processEvent(event *mysqlctl.UpdateResponse) error {
	if event.Error != "" {
		if event.BinlogPosition.Position == "" {
			return fmt.Errorf("Error from update stream %v", event.Error)
		} else {
			return fmt.Errorf("Error from update stream %v @ position %v", event.Error, event.BinlogPosition.Position)
		}
	}

	if event.BinlogPosition.Position == "" {
		return fmt.Errorf("Invalid event, no error, position is not set")
	}

	switch event.EventData.SqlType {
	case mysqlctl.DDL:
		rowCache.handleDdlEvent(event)
	case mysqlctl.BEGIN:
		rowCache.dmlBuffer = rowCache.dmlBuffer[:0]
		if rowCache.inTxn {
			return fmt.Errorf("Invalid 'BEGIN' event, transaction already in progress")
		}
		rowCache.inTxn = true
	case mysqlctl.COMMIT:
		if !rowCache.inTxn {
			return fmt.Errorf("Invalid 'COMMIT' event for a non-transaction")
		}
		rowCache.handleTxn(event)
		rowCache.inTxn = false
		rowCache.dmlBuffer = rowCache.dmlBuffer[:0]
	case "insert", "update", "delete":
		dml, err := buildDmlData(event)
		if err != nil {
			return err
		}
		rowCache.dmlBuffer = append(rowCache.dmlBuffer, dml)
	default:
		return fmt.Errorf("Unknown SqlType")
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

func buildDmlData(event *mysqlctl.UpdateResponse) (*proto.DmlType, error) {
	if !isDmlEvent(event.SqlType) {
		return nil, fmt.Errorf("Invalid Dml")
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
				return nil, err
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

func (rowCache *InvalidationProcessor) handleTxn(commitEvent *mysqlctl.UpdateResponse) {
	if len(rowCache.dmlBuffer) <= 0 {
		return
	}
	cacheInvalidate := new(proto.CacheInvalidate)
	cacheInvalidate.Position = commitEvent.BinlogPosition.Position
	cacheInvalidate.Dmls = make([]proto.DmlType, 0, len(rowCache.dmlBuffer))
	for _, dml := range rowCache.dmlBuffer {
		cacheInvalidate.Dmls = append(cacheInvalidate.Dmls, *dml)
	}
	InvalidateForDml(cacheInvalidate)
}

func (rowCache *InvalidationProcessor) handleDdlEvent(ddlEvent *mysqlctl.UpdateResponse) {
	if ddlEvent.Sql == "" {
		return
	}
	ddlInvalidate := new(proto.DDLInvalidate)
	ddlInvalidate.Position = ddlEvent.BinlogPosition.Position
	ddlInvalidate.DDL = ddlEvent.Sql
	InvalidateForDDL(ddlInvalidate)
}
