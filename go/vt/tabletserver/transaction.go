package tabletserver

import "time"

// Commit commits the specified transaction.
func Commit(logStats *SQLQueryStats, qe *QueryEngine, transactionID int64) {
	defer queryStats.Record("COMMIT", time.Now())
	dirtyTables, err := qe.txPool.SafeCommit(transactionID)
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
