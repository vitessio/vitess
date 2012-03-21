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
	"strconv"
	"time"
)

const (
	RC_DELETED = 1
)

type RowCache struct {
	prefix    string
	cachePool *CachePool
}

func NewRowCache(tableName string, createTime time.Time, cachePool *CachePool) *RowCache {
	prefix := tableName + "." + strconv.FormatInt(createTime.Unix(), 10) + "."
	return &RowCache{prefix, cachePool}
}

func (self *RowCache) Get(key string) (row []interface{}) {
	conn := self.cachePool.Get()
	defer conn.Recycle()
	mkey := self.prefix + key

	b, f, err := conn.Get(mkey)
	if err != nil {
		conn.Close()
		panic(NewTabletError(FATAL, "%s", err))
	}
	if b == nil {
		return nil
	}
	if f == RC_DELETED {
		return nil
	}
	err = bson.Unmarshal(b, &row)
	if err != nil {
		panic(NewTabletError(FATAL, "%s", err))
	}
	return row
}

func (self *RowCache) Set(key string, row []interface{}, readTime time.Time) {
	conn := self.cachePool.Get()
	defer conn.Recycle()
	mkey := self.prefix + key

	b, f, err := conn.Get(mkey)
	if err != nil {
		conn.Close()
		panic(NewTabletError(FATAL, "%s", err))
	}
	if f == RC_DELETED {
		ut, err := strconv.ParseUint(string(b), 10, 64)
		if err != nil {
			panic(NewTabletError(FATAL, "%s", err))
		}
		if readTime.UnixNano() <= int64(ut+1e8) {
			return
		}
	}

	b, err = bson.Marshal(row)
	if err != nil {
		panic(NewTabletError(FATAL, "%s", err))
	}
	_, err = conn.Set(mkey, 0, b)
	if err != nil {
		conn.Close()
		panic(NewTabletError(FATAL, "%s", err))
	}
}

func (self *RowCache) Delete(key string) {
	conn := self.cachePool.Get()
	defer conn.Recycle()
	mkey := self.prefix + key

	b := strconv.AppendInt(nil, time.Now().UnixNano(), 10)
	_, err := conn.Set(mkey, RC_DELETED, b)
	if err != nil {
		conn.Close()
		panic(NewTabletError(FATAL, "%s", err))
	}
}
