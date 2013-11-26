// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"fmt"
	"io"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/tabletserver/proto"
)

const (
	RCINV_DISABLED = iota
	RCINV_ENABLED
	RCINV_SHUTTING_DOWN
)

type InvalidationProcessor struct {
	GroupId string
	state   sync2.AtomicInt64
	states  *stats.States
}

var CacheInvalidationProcessor *InvalidationProcessor

func init() {
	CacheInvalidationProcessor = new(InvalidationProcessor)
	CacheInvalidationProcessor.states = stats.NewStates("RowcacheInvalidationState", []string{
		"Disabled",
		"Enabled",
		"ShuttingDown",
	}, time.Now(), RCINV_DISABLED)
	stats.Publish("RowcacheInvalidationCheckPoint", stats.StringFunc(func() string {
		// TODO(sougou): resolve possible data race here.
		return CacheInvalidationProcessor.GroupId
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
	rowCache.states.SetState(RCINV_SHUTTING_DOWN)
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
	rowCache.states.SetState(RCINV_ENABLED)
	defer func() {
		rowCache.state.Set(RCINV_DISABLED)
		rowCache.states.SetState(RCINV_DISABLED)
		DisallowQueries()
	}()

	replPos, err := mysqlctl.GetReplicationPosition()
	if err != nil {
		log.Errorf("Rowcache invalidator could not start: cannot determine replication position: %v", err)
		return
	}

	log.Infof("Starting rowcache invalidator")
	req := &mysqlctl.UpdateStreamRequest{GroupId: replPos.GroupId}
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
		rowCache.GroupId = event.GroupId
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
