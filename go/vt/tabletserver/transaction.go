// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import "golang.org/x/net/context"

// Commit commits the specified transaction.
func Commit(ctx context.Context, logStats *SQLQueryStats, qe *QueryEngine, transactionID int64) {
	dirtyTables, err := qe.txPool.SafeCommit(ctx, transactionID)
	for tableName, invalidList := range dirtyTables {
		tableInfo := qe.schemaInfo.GetTable(tableName)
		if tableInfo == nil {
			continue
		}
		invalidations := int64(0)
		for key := range invalidList {
			// Use context.Background, becaause we don't want to fail
			// these deletes.
			tableInfo.Cache.Delete(context.Background(), key)
			invalidations++
		}
		logStats.CacheInvalidations += invalidations
		tableInfo.invalidations.Add(invalidations)
	}
	if err != nil {
		panic(err)
	}
}
