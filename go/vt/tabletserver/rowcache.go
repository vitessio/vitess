/*
Copyright 2012, Google Inc.
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

    * Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above
copyright notice, this list of conditions and the following disclaimer
in the documentation and/or other materials provided with the
distribution.
    * Neither the name of Google Inc. nor the names of its
contributors may be used to endorse or promote products derived from
this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

package tabletserver

import (
	"code.google.com/p/vitess/go/bson"
)

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
	err = bson.Unmarshal(b, &row)
	if err != nil {
		panic(NewTabletError(FATAL, "%s", err))
	}
	// No cas. If you've read the row, we don't expect you to update it back.
	return row, 0
}

func (self *RowCache) Set(key string, row []interface{}, cas uint64) {
	// This value is hardcoded for now.
	// We're assuming it's not worth caching rows that are too large.
	if rowLen(row) > 8000 {
		return
	}
	conn := self.cachePool.Get()
	defer conn.Recycle()
	mkey := self.prefix + key

	b, err := bson.Marshal(row)
	if err != nil {
		panic(NewTabletError(FATAL, "%s", err))
	}

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
