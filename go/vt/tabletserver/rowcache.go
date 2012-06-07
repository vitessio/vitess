// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"encoding/binary"
)

var pack = binary.BigEndian

const (
	RC_DELETED = 1
)

type RowCache struct {
	prefix    string
	cachePool *CachePool
}

func NewRowCache(tableName string, hash string, cachePool *CachePool) *RowCache {
	prefix := hash + "."
	return &RowCache{prefix, cachePool}
}

func (self *RowCache) Get(key string) (row []interface{}, cas uint64) {
	conn := self.cachePool.Get()
	defer conn.Recycle()
	mkey := self.prefix + key

	b, f, cas, err := conn.Gets(mkey)
	if err != nil {
		conn.Close()
		panic(NewTabletError(FATAL, "%s", err))
	}
	if b == nil {
		return nil, 0
	}
	if f == RC_DELETED {
		// The row was recently invalidated.
		// If the caller reads the row from db, they can update it
		// back as long as it's not updated again.
		return nil, cas
	}
	if row = decodeRow(b); row == nil {
		panic(NewTabletError(FAIL, "Corrupt data for %s", key))
	}
	// No cas. If you've read the row, we don't expect you to update it back.
	return row, 0
}

func (self *RowCache) Set(key string, row []interface{}, cas uint64) {
	// This value is hardcoded for now.
	// We're assuming it's not worth caching rows that are too large.
	b := encodeRow(row, 8000)
	if b == nil {
		return
	}
	conn := self.cachePool.Get()
	defer conn.Recycle()
	mkey := self.prefix + key

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
		panic(NewTabletError(FATAL, "%s", err))
	}
}

func (self *RowCache) Delete(key string) {
	conn := self.cachePool.Get()
	defer conn.Recycle()
	mkey := self.prefix + key

	_, err := conn.Set(mkey, RC_DELETED, self.cachePool.DeleteExpiry, nil)
	if err != nil {
		conn.Close()
		panic(NewTabletError(FATAL, "%s", err))
	}
}

func rowLen(row []interface{}) int {
	length := 0
	for _, v := range row {
		if v == nil {
			continue
		}
		switch col := v.(type) {
		case string:
			length += len(col)
		case []byte:
			length += len(col)
		}
	}
	return length
}

func encodeRow(row []interface{}, max int) (b []byte) {
	length := 0
	for _, v := range row {
		if v == nil {
			continue
		}
		switch col := v.(type) {
		case string:
			length += len(col)
		case []byte:
			length += len(col)
		}
		if length > max {
			return nil
		}
	}
	datastart := 4 + len(row)*4
	b = make([]byte, datastart+length)
	data := b[datastart:datastart]
	pack.PutUint32(b, uint32(len(row)))
	for i, v := range row {
		if v == nil {
			pack.PutUint32(b[4+i*4:], 0xFFFFFFFF)
			continue
		}
		var clen int
		switch col := v.(type) {
		case string:
			clen = len(col)
			data = append(data, col...)
		case []byte:
			clen = len(col)
			data = append(data, col...)
		}
		pack.PutUint32(b[4+i*4:], uint32(clen))
	}
	return b
}

func decodeRow(b []byte) (row []interface{}) {
	rowlen := pack.Uint32(b)
	data := b[4+rowlen*4:]
	row = make([]interface{}, rowlen)
	for i, _ := range row {
		length := pack.Uint32(b[4+i*4:])
		if length == 0xFFFFFFFF {
			continue
		}
		if length > uint32(len(data)) {
			// Corrupt data
			return nil
		}
		row[i] = data[:length]
		data = data[length:]
	}
	return row
}
