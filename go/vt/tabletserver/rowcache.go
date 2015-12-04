// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"encoding/binary"
	"strconv"
	"time"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/stats"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
	"golang.org/x/net/context"
)

var cacheStats = stats.NewTimings("Rowcache")

var pack = binary.BigEndian

const (
	rcDeleted = 1

	// maxKeyLen is a value less than memcache's limit of 250.
	maxKeyLen = 200

	// maxDataLen prevents large rows from being inserted in rowcache.
	maxDataLen = 8000
)

// RowCache gives a table-level view into the rowcache.
type RowCache struct {
	tableInfo *TableInfo
	prefix    string
	cachePool *CachePool
}

// RCResult represents the result of a cache multi-fetch.
type RCResult struct {
	Row []sqltypes.Value
	Cas uint64
}

// NewRowCache creates a new RowCache.
func NewRowCache(tableInfo *TableInfo, cachePool *CachePool) *RowCache {
	prefix := strconv.FormatInt(cachePool.maxPrefix.Add(1), 36) + "."
	return &RowCache{tableInfo, prefix, cachePool}
}

// Get fetches the values for the specified keys.
func (rc *RowCache) Get(ctx context.Context, keys []string) (results map[string]RCResult) {
	mkeys := make([]string, 0, len(keys))
	for _, key := range keys {
		if len(key) > maxKeyLen {
			continue
		}
		mkeys = append(mkeys, rc.prefix+key)
	}
	prefixlen := len(rc.prefix)
	conn := rc.cachePool.Get(ctx)
	// This is not the same as defer rc.cachePool.Put(conn)
	defer func() { rc.cachePool.Put(conn) }()

	defer cacheStats.Record("Exec", time.Now())
	mcresults, err := conn.Gets(mkeys...)
	if err != nil {
		conn.Close()
		conn = nil
		panic(NewTabletError(ErrFatal, vtrpcpb.ErrorCode_INTERNAL_ERROR, "%s", err))
	}
	results = make(map[string]RCResult, len(mkeys))
	for _, mcresult := range mcresults {
		if mcresult.Flags == rcDeleted {
			// The row was recently invalidated.
			// If the caller reads the row from db, they can update it
			// back as long as it's not updated again.
			results[mcresult.Key[prefixlen:]] = RCResult{Cas: mcresult.Cas}
			continue
		}
		row := rc.decodeRow(mcresult.Value)
		if row == nil {
			panic(NewTabletError(ErrFatal, vtrpcpb.ErrorCode_INTERNAL_ERROR, "Corrupt data for %s", mcresult.Key))
		}
		results[mcresult.Key[prefixlen:]] = RCResult{Row: row, Cas: mcresult.Cas}
	}
	return
}

// Set pushes the specified row into the rowcache.
func (rc *RowCache) Set(ctx context.Context, key string, row []sqltypes.Value, cas uint64) {
	if len(key) > maxKeyLen {
		return
	}
	b := rc.encodeRow(row)
	if b == nil {
		return
	}
	conn := rc.cachePool.Get(ctx)
	defer func() { rc.cachePool.Put(conn) }()
	mkey := rc.prefix + key

	var err error
	if cas == 0 {
		// Either caller didn't find the value at all
		// or they didn't look for it in the first place.
		_, err = conn.Add(mkey, 0, 0, b)
	} else {
		// Caller is trying to update a row that recently changed.
		_, err = conn.Cas(mkey, 0, 0, b, cas)
	}
	if err != nil {
		conn.Close()
		conn = nil
		panic(NewTabletError(ErrFatal, vtrpcpb.ErrorCode_INTERNAL_ERROR, "%s", err))
	}
}

// Delete marks the row as deleted.
func (rc *RowCache) Delete(ctx context.Context, key string) {
	if len(key) > maxKeyLen {
		return
	}
	conn := rc.cachePool.Get(ctx)
	defer func() { rc.cachePool.Put(conn) }()
	mkey := rc.prefix + key

	_, err := conn.Set(mkey, rcDeleted, 0, nil)
	if err != nil {
		conn.Close()
		conn = nil
		panic(NewTabletError(ErrFatal, vtrpcpb.ErrorCode_INTERNAL_ERROR, "%s", err))
	}
}

func (rc *RowCache) encodeRow(row []sqltypes.Value) (b []byte) {
	length := 0
	for _, v := range row {
		length += v.Len()
		if length > maxDataLen {
			return nil
		}
	}
	datastart := 4 + len(row)*4
	b = make([]byte, datastart+length)
	data := b[datastart:datastart]
	pack.PutUint32(b, uint32(len(row)))
	for i, v := range row {
		if v.IsNull() {
			pack.PutUint32(b[4+i*4:], 0xFFFFFFFF)
			continue
		}
		data = append(data, v.Raw()...)
		pack.PutUint32(b[4+i*4:], uint32(v.Len()))
	}
	return b
}

func (rc *RowCache) decodeRow(b []byte) (row []sqltypes.Value) {
	rowlen := pack.Uint32(b)
	data := b[4+rowlen*4:]
	row = make([]sqltypes.Value, rowlen)
	for i := range row {
		length := pack.Uint32(b[4+i*4:])
		if length == 0xFFFFFFFF {
			continue
		}
		if length > uint32(len(data)) {
			// Corrupt data
			return nil
		}
		// rowcache values are trusted.
		row[i] = sqltypes.MakeTrusted(rc.tableInfo.Columns[i].Type, data[:length])
		data = data[length:]
	}
	return row
}
