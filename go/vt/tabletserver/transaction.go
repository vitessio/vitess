package tabletserver

import (
	"time"

	"github.com/youtube/vitess/go/vt/dbconnpool"
)

// Begin begins a transaction.
func Begin(logStats *SQLQueryStats, qe *QueryEngine) int64 {
	defer queryStats.Record("BEGIN", time.Now())

	conn, err := qe.txPool.TryGet()
	if err == dbconnpool.CONN_POOL_CLOSED_ERR {
		panic(connPoolClosedErr)
	}
	if err != nil {
		panic(NewTabletErrorSql(FATAL, err))
	}
	if conn == nil {
		qe.activeTxPool.LogActive()
		panic(NewTabletError(TX_POOL_FULL, "Transaction pool connection limit exceeded"))
	}
	transactionID, err := qe.activeTxPool.SafeBegin(conn)
	if err != nil {
		conn.Recycle()
		panic(err)
	}
	return transactionID
}

// Commit commits the specified transaction.
func Commit(logStats *SQLQueryStats, qe *QueryEngine, transactionID int64) {
	defer queryStats.Record("COMMIT", time.Now())
	dirtyTables, err := qe.activeTxPool.SafeCommit(transactionID)
	for tableName, invalidList := range dirtyTables {
		tableInfo := qe.schemaInfo.GetTable(tableName)
		if tableInfo == nil {
			continue
		}
		invalidations := int64(0)
		for key := range invalidList {
			tableInfo.Cache.Delete(key)
			invalidations++
		}
		logStats.CacheInvalidations += invalidations
		tableInfo.invalidations.Add(invalidations)
	}
	if err != nil {
		panic(err)
	}
}

// Rollback rolls back the specified transaction.
func Rollback(logStats *SQLQueryStats, qe *QueryEngine, transactionID int64) {
	defer queryStats.Record("ROLLBACK", time.Now())
	qe.activeTxPool.Rollback(transactionID)
}
