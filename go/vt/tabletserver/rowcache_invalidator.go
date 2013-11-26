// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"fmt"
	"io"
	"strconv"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/tabletserver/proto"
)

const (
	RCINV_DISABLED int64 = iota
	RCINV_ENABLED
	RCINV_SHUTTING_DOWN
)

var rcinvStateNames = map[int64]string{
	RCINV_DISABLED:      "Disabled",
	RCINV_ENABLED:       "Enabled",
	RCINV_SHUTTING_DOWN: "ShuttingDown",
}

type InvalidationProcessor struct {
	currentPosition mysqlctl.BinlogPosition
	state           sync2.AtomicInt64
}

var CacheInvalidationProcessor *InvalidationProcessor

func init() {
	CacheInvalidationProcessor = new(InvalidationProcessor)
	stats.Publish("RowcacheInvalidationState", stats.StringFunc(func() string {
		return rcinvStateNames[CacheInvalidationProcessor.state.Get()]
	}))
	stats.Publish("RowcacheInvalidationCheckPoint", stats.StringFunc(func() string {
		if CacheInvalidationProcessor.currentPosition.GroupId != 0 {
			return CacheInvalidationProcessor.currentPosition.String()
		}
		return ""
	}))
}

func StartRowCacheInvalidation() {
	go CacheInvalidationProcessor.runInvalidationLoop()
}

func StopRowCacheInvalidation() {
	CacheInvalidationProcessor.stopRowCacheInvalidation()
}

func (rowCache *InvalidationProcessor) stopRowCacheInvalidation() {
	if !rowCache.state.CompareAndSwap(RCINV_ENABLED, RCINV_SHUTTING_DOWN) {
		log.Infof("Rowcache invalidator is not enabled")
	}
}

func (rowCache *InvalidationProcessor) runInvalidationLoop() {
	if !IsCachePoolAvailable() {
		log.Infof("Rowcache is not enabled. Not running invalidator.")
		return
	}
	if !rowCache.state.CompareAndSwap(RCINV_DISABLED, RCINV_ENABLED) {
		log.Infof("Rowcache invalidator already running")
		return
	}

	defer func() {
		rowCache.state.Set(RCINV_DISABLED)
		DisallowQueries()
	}()

	replPos, err := mysqlctl.GetReplicationPosition()
	if err != nil {
		log.Errorf("Rowcache invalidator could not start: cannot determine replication position: %v", err)
		return
	}

	// TODO(sougou): change GroupId to be int64
	groupid, err := strconv.Atoi(replPos.GroupId)
	if err != nil {
		log.Errorf("Rowcache invalidator could not start: could not read group id: %v", err)
		return
	}

	log.Infof("Starting rowcache invalidator")
	req := &mysqlctl.BinlogPosition{GroupId: int64(groupid)}
	err = mysqlctl.ServeUpdateStream(req, func(reply interface{}) error {
		return rowCache.processEvent(reply.(*mysqlctl.StreamEvent))
	})
	if err != nil {
		log.Errorf("mysqlctl.ServeUpdateStream returned err '%v'", err.Error())
	}
	log.Infof("Rowcache invalidator stopped")
}

func (rowCache *InvalidationProcessor) processEvent(event *mysqlctl.StreamEvent) error {
	if rowCache.state.Get() != RCINV_ENABLED {
		return io.EOF
	}
	switch event.Category {
	case "DDL":
		InvalidateForDDL(&proto.DDLInvalidate{DDL: event.Sql})
	case "DML":
		rowCache.handleDmlEvent(event)
	case "ERR":
		log.Errorf("Unrecognized: %s", event.Sql)
		errorStats.Add("Invalidation", 1)
	case "POS":
		rowCache.currentPosition.GroupId = event.GroupId
		rowCache.currentPosition.ServerId = event.ServerId
	default:
		panic(fmt.Errorf("unknown event: %#v", event))
	}
	return nil
}

func (rowCache *InvalidationProcessor) handleDmlEvent(event *mysqlctl.StreamEvent) {
	dml := new(proto.DmlType)
	dml.Table = event.TableName
	dml.Keys = make([]string, 0, len(event.PkValues))
	sqlTypeKeys := make([]sqltypes.Value, 0, len(event.PkColNames))
	for _, pkTuple := range event.PkValues {
		sqlTypeKeys = sqlTypeKeys[:0]
		for _, pkVal := range pkTuple {
			key, err := sqltypes.BuildValue(pkVal)
			if err != nil {
				log.Errorf("Error building invalidation key for %#v: '%v'", event, err)
				errorStats.Add("Invalidation", 1)
				return
			}
			sqlTypeKeys = append(sqlTypeKeys, key)
		}
		invalidateKey := buildKey(sqlTypeKeys)
		if invalidateKey != "" {
			dml.Keys = append(dml.Keys, invalidateKey)
		}
	}
	InvalidateForDml(dml)
}
