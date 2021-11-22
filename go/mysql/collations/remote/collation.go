/*
Copyright 2021 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package remote

import (
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"strings"
	"sync"

	"vitess.io/vitess/go/bytes2"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/collations/internal/charset"
	"vitess.io/vitess/go/sqltypes"
)

// Collation is a generic implementation of the Collation interface
// that supports any collation in MySQL by performing the collation
// operation directly on a remote `mysqld` instance. It is not particularly
// efficient compared to the native Collation implementations in Vitess,
// but it offers authoritative results for all collation types and can be
// used as a fallback or as a way to test our native implementations.
type Collation struct {
	name string
	id   collations.ID

	charset string
	prefix  string
	suffix  string

	mu   sync.Mutex
	conn *mysql.Conn
	sql  bytes2.Buffer
	hex  io.Writer
	err  error
}

var _ collations.Collation = (*Collation)(nil)

func makeRemoteCollation(conn *mysql.Conn, collid collations.ID, collname string) *Collation {
	charset := collname
	if idx := strings.IndexByte(collname, '_'); idx >= 0 {
		charset = collname[:idx]
	}

	coll := &Collation{
		name:    collname,
		id:      collid,
		conn:    conn,
		charset: charset,
	}

	coll.prefix = fmt.Sprintf("_%s X'", charset)
	coll.suffix = fmt.Sprintf("' COLLATE %q", collname)
	coll.hex = hex.NewEncoder(&coll.sql)
	return coll
}

func NewCollation(conn *mysql.Conn, collname string) *Collation {
	return makeRemoteCollation(conn, collations.Unknown, collname)
}

func (c *Collation) LastError() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

func (c *Collation) Init() {}

func (c *Collation) ID() collations.ID {
	return c.id
}

func (c *Collation) IsBinary() bool {
	return false
}

func (c *Collation) Name() string {
	return c.name
}

func (c *Collation) Charset() charset.Charset {
	return makeRemoteCharset(c.conn, &c.mu, c.charset)
}

func (c *Collation) Collate(left, right []byte, isPrefix bool) int {
	if isPrefix {
		panic("unsupported: isPrefix with remote.Collation")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.sql.Reset()
	c.sql.WriteString("SELECT STRCMP(")
	c.sql.WriteString(c.prefix)
	c.hex.Write(left)
	c.sql.WriteString(c.suffix)
	c.sql.WriteString(", ")
	c.sql.WriteString(c.prefix)
	c.hex.Write(right)
	c.sql.WriteString(c.suffix)
	c.sql.WriteString(")")

	var cmp int64
	if result := c.performRemoteQuery(); result != nil {
		cmp, c.err = result[0].ToInt64()
	}
	return int(cmp)
}

func (c *Collation) performRemoteQuery() []sqltypes.Value {
	res, err := c.conn.ExecuteFetch(c.sql.StringUnsafe(), 1, false)
	if err != nil {
		c.err = err
		return nil
	}
	if len(res.Rows) != 1 {
		c.err = fmt.Errorf("unexpected result from MySQL: %d rows returned", len(res.Rows))
		return nil
	}
	c.err = nil
	return res.Rows[0]
}

func (c *Collation) WeightString(dst, src []byte, numCodepoints int) []byte {
	if numCodepoints == math.MaxInt32 {
		panic("unsupported: PadToMax with remote.Collation")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.sql.Reset()
	c.sql.WriteString("SELECT WEIGHT_STRING(")
	c.sql.WriteString(c.prefix)
	c.hex.Write(src)
	c.sql.WriteString(c.suffix)
	if numCodepoints > 0 {
		fmt.Fprintf(&c.sql, " AS CHAR(%d)", numCodepoints)
	}
	c.sql.WriteString(")")

	if result := c.performRemoteQuery(); result != nil {
		if dst == nil {
			dst = result[0].ToBytes()
		} else {
			dst = append(dst, result[0].ToBytes()...)
		}
	}
	return dst
}

func (c *Collation) Hash(_ []byte, _ int) collations.HashCode {
	panic("unsupported: Hash for remote collations")
}

func (c *Collation) WeightStringLen(_ int) int {
	return 0
}
