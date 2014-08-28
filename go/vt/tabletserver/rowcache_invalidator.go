// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"fmt"
	"sync"
	"time"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/sync2"
	"github.com/youtube/vitess/go/tb"
	"github.com/youtube/vitess/go/vt/binlog"
	blproto "github.com/youtube/vitess/go/vt/binlog/proto"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	myproto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
)

// RowcacheInvalidator runs the service to invalidate
// the rowcache based on binlog events.
type RowcacheInvalidator struct {
	qe  *QueryEngine
	svm sync2.ServiceManager

	lagSeconds sync2.AtomicInt64
	gtid       myproto.GTID
	gtidMutex  sync.RWMutex
}

func (rci *RowcacheInvalidator) GetGTID() myproto.GTID {
	rci.gtidMutex.RLock()
	defer rci.gtidMutex.RUnlock()
	return rci.gtid
}

func (rci *RowcacheInvalidator) GetGTIDString() string {
	gtid := rci.GetGTID()
	if gtid == nil {
		return "<nil>"
	}
	return gtid.String()
}

func (rci *RowcacheInvalidator) SetGTID(gtid myproto.GTID) {
	rci.gtidMutex.Lock()
	defer rci.gtidMutex.Unlock()
	rci.gtid = gtid
}

// NewRowcacheInvalidator creates a new RowcacheInvalidator.
// Just like QueryEngine, this is a singleton class.
// You must call this only once.
func NewRowcacheInvalidator(qe *QueryEngine) *RowcacheInvalidator {
	rci := &RowcacheInvalidator{qe: qe}
	stats.Publish("RowcacheInvalidatorState", stats.StringFunc(rci.svm.StateName))
	stats.Publish("RowcacheInvalidatorPosition", stats.StringFunc(rci.GetGTIDString))
	stats.Publish("RowcacheInvalidatorLagSeconds", stats.IntFunc(rci.lagSeconds.Get))
	return rci
}

// Open runs the invalidation loop.
func (rci *RowcacheInvalidator) Open(dbname string, mysqld *mysqlctl.Mysqld) {
	rp, err := mysqld.MasterStatus()
	if err != nil {
		panic(NewTabletError(FATAL, "Rowcache invalidator aborting: cannot determine replication position: %v", err))
	}
	if mysqld.Cnf().BinLogPath == "" {
		panic(NewTabletError(FATAL, "Rowcache invalidator aborting: binlog path not specified"))
	}

	ok := rci.svm.Go(func(ctx *sync2.ServiceContext) error {
		rci.SetGTID(rp.MasterLogGTIDField.Value)
		rci.run(ctx, binlog.NewEventStreamer(dbname, mysqld))
		return nil
	})
	if ok {
		log.Infof("Rowcache invalidator starting, dbname: %s, path: %s, logfile: %s, position: %d", dbname, mysqld.Cnf().BinLogPath, rp.MasterLogFile, rp.MasterLogPosition)
	} else {
		log.Infof("Rowcache invalidator already running")
	}
}

// Close terminates the invalidation loop. It returns only of the
// loop has terminated.
func (rci *RowcacheInvalidator) Close() {
	rci.svm.Stop()
}

func (rci *RowcacheInvalidator) run(ctx *sync2.ServiceContext, evs *binlog.EventStreamer) {
	for {
		// We wrap this code in a func so we can catch all panics.
		// If an error is returned, we log it, wait 1 second, and retry.
		// This loop can only be stopped by calling Close.
		err := func() (inner error) {
			defer func() {
				if x := recover(); x != nil {
					inner = fmt.Errorf("%v: uncaught panic:\n%s", x, tb.Stack(4))
				}
			}()
			return evs.Stream(ctx, rci.GetGTID(), func(reply *blproto.StreamEvent) error {
				rci.processEvent(reply)
				return nil
			})
		}()
		if err == nil {
			break
		}
		log.Errorf("binlog.ServeUpdateStream returned err '%v', retrying in 1 second.", err.Error())
		internalErrors.Add("Invalidation", 1)
		time.Sleep(1 * time.Second)
	}
	log.Infof("Rowcache invalidator stopped")
}

func handleInvalidationError(event *blproto.StreamEvent) {
	if x := recover(); x != nil {
		terr, ok := x.(*TabletError)
		if !ok {
			log.Errorf("Uncaught panic for %+v:\n%v\n%s", event, x, tb.Stack(4))
			internalErrors.Add("Panic", 1)
			return
		}
		log.Errorf("%v: %+v", terr, event)
		internalErrors.Add("Invalidation", 1)
	}
}

func (rci *RowcacheInvalidator) processEvent(event *blproto.StreamEvent) {
	defer handleInvalidationError(event)
	switch event.Category {
	case "DDL":
		log.Infof("DDL invalidation: %s", event.Sql)
		rci.qe.InvalidateForDDL(event.Sql)
	case "DML":
		rci.handleDmlEvent(event)
	case "ERR":
		rci.qe.InvalidateForUnrecognized(event.Sql)
	case "POS":
		rci.SetGTID(event.GTIDField.Value)
	default:
		log.Errorf("unknown event: %#v", event)
		internalErrors.Add("Invalidation", 1)
		return
	}
	rci.lagSeconds.Set(time.Now().Unix() - event.Timestamp)
}

func (rci *RowcacheInvalidator) handleDmlEvent(event *blproto.StreamEvent) {
	table := event.TableName
	keys := make([]string, 0, len(event.PKValues))
	sqlTypeKeys := make([]sqltypes.Value, 0, len(event.PKColNames))
	for _, pkTuple := range event.PKValues {
		sqlTypeKeys = sqlTypeKeys[:0]
		for _, pkVal := range pkTuple {
			key, err := sqltypes.BuildValue(pkVal)
			if err != nil {
				log.Errorf("Error building invalidation key for %#v: '%v'", event, err)
				internalErrors.Add("Invalidation", 1)
				return
			}
			sqlTypeKeys = append(sqlTypeKeys, key)
		}
		invalidateKey := buildKey(sqlTypeKeys)
		if invalidateKey != "" {
			keys = append(keys, invalidateKey)
		}
	}
	rci.qe.InvalidateForDml(table, keys)
}
