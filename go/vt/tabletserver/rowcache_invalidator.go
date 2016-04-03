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
	"github.com/youtube/vitess/go/vt/mysqlctl"
	"github.com/youtube/vitess/go/vt/mysqlctl/replication"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/tabletserver/planbuilder"
	"golang.org/x/net/context"

	binlogdatapb "github.com/youtube/vitess/go/vt/proto/binlogdata"
)

// RowcacheInvalidator runs the service to invalidate
// the rowcache based on binlog events.
type RowcacheInvalidator struct {
	qe      *QueryEngine
	checker MySQLChecker
	dbname  string
	mysqld  mysqlctl.MysqlDaemon

	svm sync2.ServiceManager

	posMutex   sync.Mutex
	pos        replication.Position
	lagSeconds sync2.AtomicInt64
}

// AppendGTID updates the current replication position by appending a GTID to
// the set of transactions that have been processed.
func (rci *RowcacheInvalidator) AppendGTID(gtid replication.GTID) {
	rci.posMutex.Lock()
	defer rci.posMutex.Unlock()
	rci.pos = replication.AppendGTID(rci.pos, gtid)
}

// SetPosition sets the current ReplicationPosition.
func (rci *RowcacheInvalidator) SetPosition(rp replication.Position) {
	rci.posMutex.Lock()
	defer rci.posMutex.Unlock()
	rci.pos = rp
}

// Position returns the current ReplicationPosition.
func (rci *RowcacheInvalidator) Position() replication.Position {
	rci.posMutex.Lock()
	defer rci.posMutex.Unlock()
	return rci.pos
}

// PositionString returns the current ReplicationPosition as a string.
func (rci *RowcacheInvalidator) PositionString() string {
	return rci.Position().String()
}

// NewRowcacheInvalidator creates a new RowcacheInvalidator.
// Just like QueryEngine, this is a singleton class.
// You must call this only once.
func NewRowcacheInvalidator(statsPrefix string, checker MySQLChecker, qe *QueryEngine, enablePublishStats bool) *RowcacheInvalidator {
	rci := &RowcacheInvalidator{checker: checker, qe: qe}
	if enablePublishStats {
		stats.Publish(statsPrefix+"RowcacheInvalidatorState", stats.StringFunc(rci.svm.StateName))
		stats.Publish(statsPrefix+"RowcacheInvalidatorPosition", stats.StringFunc(rci.PositionString))
		stats.Publish(statsPrefix+"RowcacheInvalidatorLagSeconds", stats.IntFunc(rci.lagSeconds.Get))
	}
	return rci
}

// Open runs the invalidation loop.
func (rci *RowcacheInvalidator) Open(dbname string, mysqld mysqlctl.MysqlDaemon) {
	// Perform an early check to see if we're already running.
	if rci.svm.State() == sync2.SERVICE_RUNNING {
		return
	}
	rp, err := mysqld.MasterPosition()
	if err != nil {
		panic(NewTabletError(ErrFatal, vtrpcpb.ErrorCode_INTERNAL_ERROR, "Rowcache invalidator aborting: cannot determine replication position: %v", err))
	}
	if mysqld.Cnf().BinLogPath == "" {
		panic(NewTabletError(ErrFatal, vtrpcpb.ErrorCode_INTERNAL_ERROR, "Rowcache invalidator aborting: binlog path not specified"))
	}
	err = rci.qe.ClearRowcache(context.Background())
	if err != nil {
		panic(NewTabletError(ErrFatal, vtrpcpb.ErrorCode_INTERNAL_ERROR, "Rowcahe is not reachable"))
	}

	rci.dbname = dbname
	rci.mysqld = mysqld
	rci.SetPosition(rp)

	ok := rci.svm.Go(rci.run)
	if ok {
		log.Infof("Rowcache invalidator starting, dbname: %s, path: %s, position: %v", dbname, mysqld.Cnf().BinLogPath, rp)
	} else {
		log.Infof("Rowcache invalidator already running")
	}
}

// Close terminates the invalidation loop. It returns only of the
// loop has terminated.
func (rci *RowcacheInvalidator) Close() {
	rci.svm.Stop()
}

func (rci *RowcacheInvalidator) run(ctx *sync2.ServiceContext) error {
	for {
		evs := binlog.NewEventStreamer(rci.dbname, rci.mysqld, rci.Position(), rci.processEvent)
		// We wrap this code in a func so we can catch all panics.
		// If an error is returned, we log it, wait 1 second, and retry.
		// This loop can only be stopped by calling Close.
		err := func() (inner error) {
			defer func() {
				if x := recover(); x != nil {
					inner = fmt.Errorf("%v: uncaught panic:\n%s", x, tb.Stack(4))
				}
			}()
			return evs.Stream(ctx)
		}()
		if err == nil || !ctx.IsRunning() {
			break
		}
		if IsConnErr(err) {
			rci.checker.CheckMySQL()
		}
		log.Errorf("binlog.ServeUpdateStream returned err '%v', retrying in 1 second.", err.Error())
		rci.qe.queryServiceStats.InternalErrors.Add("Invalidation", 1)
		time.Sleep(1 * time.Second)
	}
	log.Infof("Rowcache invalidator stopped")
	return nil
}

func (rci *RowcacheInvalidator) handleInvalidationError(event *binlogdatapb.StreamEvent) {
	if x := recover(); x != nil {
		terr, ok := x.(*TabletError)
		if !ok {
			log.Errorf("Uncaught panic for %+v:\n%v\n%s", event, x, tb.Stack(4))
			rci.qe.queryServiceStats.InternalErrors.Add("Panic", 1)
			return
		}
		log.Errorf("%v: %+v", terr, event)
		rci.qe.queryServiceStats.InternalErrors.Add("Invalidation", 1)
	}
}

func (rci *RowcacheInvalidator) processEvent(event *binlogdatapb.StreamEvent) error {
	defer rci.handleInvalidationError(event)
	switch event.Category {
	case binlogdatapb.StreamEvent_SE_DDL:
		log.Infof("DDL invalidation: %s", event.Sql)
		rci.handleDDLEvent(event.Sql)
	case binlogdatapb.StreamEvent_SE_DML:
		rci.handleDMLEvent(event)
	case binlogdatapb.StreamEvent_SE_ERR:
		rci.handleUnrecognizedEvent(event.Sql)
	case binlogdatapb.StreamEvent_SE_POS:
		gtid, err := replication.DecodeGTID(event.TransactionId)
		if err != nil {
			return err
		}
		rci.AppendGTID(gtid)
	default:
		log.Errorf("unknown event: %#v", event)
		rci.qe.queryServiceStats.InternalErrors.Add("Invalidation", 1)
		return nil
	}
	rci.lagSeconds.Set(time.Now().Unix() - event.Timestamp)
	return nil
}

func (rci *RowcacheInvalidator) handleDMLEvent(event *binlogdatapb.StreamEvent) {
	invalidations := int64(0)
	tableInfo := rci.qe.schemaInfo.GetTable(event.TableName)
	if tableInfo == nil {
		panic(NewTabletError(ErrFail, vtrpcpb.ErrorCode_BAD_INPUT, "Table %s not found", event.TableName))
	}
	if !tableInfo.IsCached() {
		return
	}

	for _, pkTuple := range event.PrimaryKeyValues {
		// We can trust values coming from EventStreamer.
		row := sqltypes.MakeRowTrusted(event.PrimaryKeyFields, pkTuple)
		tableInfo.Cache.Delete(context.Background(), buildKey(row))
		invalidations++
	}
	tableInfo.invalidations.Add(invalidations)
}

func (rci *RowcacheInvalidator) handleDDLEvent(ddl string) {
	ddlPlan := planbuilder.DDLParse(ddl)
	if ddlPlan.Action == "" {
		panic(NewTabletError(ErrFail, vtrpcpb.ErrorCode_BAD_INPUT, "DDL is not understood"))
	}
	if ddlPlan.TableName != "" && ddlPlan.TableName != ddlPlan.NewName {
		// It's a drop or rename.
		rci.qe.schemaInfo.DropTable(ddlPlan.TableName)
	}
	if ddlPlan.NewName != "" {
		rci.qe.schemaInfo.CreateOrUpdateTable(context.Background(), ddlPlan.NewName)
	}
}

func (rci *RowcacheInvalidator) handleUnrecognizedEvent(sql string) {
	statement, err := sqlparser.Parse(sql)
	if err != nil {
		log.Errorf("Error: %v: %s", err, sql)
		rci.qe.queryServiceStats.InternalErrors.Add("Invalidation", 1)
		return
	}
	var table *sqlparser.TableName
	switch stmt := statement.(type) {
	case *sqlparser.Insert:
		// Inserts don't affect rowcache.
		return
	case *sqlparser.Update:
		table = stmt.Table
	case *sqlparser.Delete:
		table = stmt.Table
	default:
		log.Errorf("Unrecognized: %s", sql)
		rci.qe.queryServiceStats.InternalErrors.Add("Invalidation", 1)
		return
	}

	// Ignore cross-db statements.
	if table.Qualifier != "" && string(table.Qualifier) != rci.qe.dbconfigs.App.DbName {
		return
	}

	// Ignore if it's an uncached table.
	tableName := string(table.Name)
	tableInfo := rci.qe.schemaInfo.GetTable(tableName)
	if tableInfo == nil {
		log.Errorf("Table %s not found: %s", tableName, sql)
		rci.qe.queryServiceStats.InternalErrors.Add("Invalidation", 1)
		return
	}
	if !tableInfo.IsCached() {
		return
	}

	// Treat the statement as a DDL.
	// It will conservatively invalidate all rows of the table.
	log.Warningf("Treating '%s' as DDL for table %s", sql, tableName)
	rci.qe.schemaInfo.CreateOrUpdateTable(context.Background(), tableName)
}
