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
	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/tabletserver/proto"
)

// RowcacheInvalidator runs the service to invalidate
// the rowcache based on binlog events.
type RowcacheInvalidator struct {
	qe  *QueryEngine
	svm sync2.ServiceManager

	// mu mainly protects access to evs by Open and Close.
	mu        sync.Mutex
	dbname    string
	mysqld    *mysqlctl.Mysqld
	evs       *binlog.EventStreamer
	Timestamp sync2.AtomicInt64
	GroupId   sync2.AtomicInt64
}

// NewRowcacheInvalidator creates a new RowcacheInvalidator.
// Just like QueryEngine, this is a singleton class.
// You must call this only once.
func NewRowcacheInvalidator(qe *QueryEngine) *RowcacheInvalidator {
	rci := &RowcacheInvalidator{qe: qe}
	stats.Publish("RowcacheInvalidatorState", stats.StringFunc(rci.svm.StateName))
	stats.Publish("RowcacheInvalidatorPosition", stats.IntFunc(rci.GroupId.Get))
	stats.Publish("RowcacheInvalidatorTimestamp", stats.IntFunc(rci.Timestamp.Get))
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

	ok := rci.svm.Go(func(_ *sync2.ServiceManager) {
		rci.mu.Lock()
		rci.dbname = dbname
		rci.mysqld = mysqld
		rci.evs = binlog.NewEventStreamer(dbname, mysqld.Cnf().BinLogPath)
		rci.GroupId.Set(rp.MasterLogGroupId)
		rci.mu.Unlock()

		rci.run()

		rci.mu.Lock()
		rci.evs = nil
		rci.mu.Unlock()
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
	rci.mu.Lock()
	if rci.evs == nil {
		log.Infof("Rowcache is not running")
		rci.mu.Unlock()
		return
	}
	// This will cause the event streamer to exit, but run
	// may still be running.
	rci.evs.Stop()
	rci.mu.Unlock()
	// Stop will wait for run and rci to shutdown, which will set
	// evs to nil. So, we need to release the lock before this.
	rci.svm.Stop()
}

func (rci *RowcacheInvalidator) run() {
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
			rp, err := rci.mysqld.BinlogInfo(rci.GroupId.Get())
			if err != nil {
				return err
			}
			return rci.evs.Stream(rp.MasterLogFile, int64(rp.MasterLogPosition), func(reply *blproto.StreamEvent) error {
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
		rci.qe.InvalidateForDDL(&proto.DDLInvalidate{DDL: event.Sql})
		rci.Timestamp.Set(event.Timestamp)
	case "DML":
		rci.handleDmlEvent(event)
		rci.Timestamp.Set(event.Timestamp)
	case "ERR":
		dbname, err := sqlparser.GetDBName(event.Sql)
		if err != nil || dbname == "" || dbname == rci.dbname {
			log.Errorf("Unrecognized: %s", event.Sql)
			internalErrors.Add("Invalidation", 1)
		}
		rci.Timestamp.Set(event.Timestamp)
	case "POS":
		rci.GroupId.Set(event.GroupId)
	default:
		log.Errorf("unknown event: %#v", event)
		internalErrors.Add("Invalidation", 1)
	}
}

func (rci *RowcacheInvalidator) handleDmlEvent(event *blproto.StreamEvent) {
	dml := new(proto.DmlType)
	dml.Table = event.TableName
	dml.Keys = make([]string, 0, len(event.PKValues))
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
			dml.Keys = append(dml.Keys, invalidateKey)
		}
	}
	rci.qe.InvalidateForDml(dml)
}
