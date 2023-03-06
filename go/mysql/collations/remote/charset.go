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
	"sync"

	"vitess.io/vitess/go/bytes2"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/collations/internal/charset"
)

type Charset struct {
	name string

	mu   *sync.Mutex
	conn *mysql.Conn
	sql  bytes2.Buffer
	hex  io.Writer
}

var _ charset.Charset = (*Charset)(nil)

func makeRemoteCharset(conn *mysql.Conn, mu *sync.Mutex, csname string) *Charset {
	cs := &Charset{
		name: csname,
		mu:   mu,
		conn: conn,
	}
	cs.hex = hex.NewEncoder(&cs.sql)
	return cs
}

func NewCharset(conn *mysql.Conn, csname string) *Charset {
	return makeRemoteCharset(conn, &sync.Mutex{}, csname)
}

func (c *Charset) Name() string {
	return c.name
}

func (c *Charset) IsSuperset(_ charset.Charset) bool {
	return false
}

func (c *Charset) SupportsSupplementaryChars() bool {
	return true
}

func (c *Charset) EncodeRune(dst []byte, r rune) int {
	panic("unsupported: EncodeRune in remote.Charset (use Charset.Convert directly)")
}

func (c *Charset) DecodeRune(bytes []byte) (rune, int) {
	panic("unsupported: DecodeRune in remote.Charset (use Charset.Convert directly)")
}

func (c *Charset) performConversion(dst []byte, dstCharset string, src []byte, srcCharset string) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.sql.Reset()
	c.sql.WriteString("SELECT CAST(CONVERT(_")
	c.sql.WriteString(srcCharset)
	c.sql.WriteString(" X'")
	c.hex.Write(src)
	c.sql.WriteString("' USING ")
	c.sql.WriteString(dstCharset)
	c.sql.WriteString(") AS binary)")

	res, err := c.conn.ExecuteFetch(c.sql.StringUnsafe(), 1, false)
	if err != nil {
		return nil, err
	}
	if len(res.Rows) != 1 {
		return nil, fmt.Errorf("unexpected result from MySQL: %d rows returned", len(res.Rows))
	}
	result, err := res.Rows[0][0].ToBytes()
	if err != nil {
		return nil, err
	}
	if dst != nil {
		return append(dst, result...), nil
	}
	return result, nil
}

func (c *Charset) EncodeFromUTF8(dst, src []byte) ([]byte, error) {
	return c.performConversion(dst, c.name, src, "utf8mb4")
}

func (c *Charset) DecodeToUTF8(dst, src []byte) ([]byte, error) {
	return c.performConversion(dst, "utf8mb4", src, c.name)
}

func (c *Charset) Convert(dst, src []byte, srcCharset charset.Charset) ([]byte, error) {
	return c.performConversion(dst, c.name, src, srcCharset.Name())
}

func (c *Charset) MaxWidth() int {
	return 1
}
