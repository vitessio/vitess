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
	"fmt"
	"sync"

	"vitess.io/vitess/go/mysql"
)

type Charset struct {
	name string

	mu   *sync.Mutex
	conn *mysql.Conn
}

func (c *Charset) Name() string {
	return c.name
}

func (c *Charset) SupportsSupplementaryChars() bool {
	return true
}

func (c *Charset) DecodeRune(bytes []byte) (rune, int) {
	panic("unsupported: DecodeRune in remote.Charset")
}

func (c *Charset) EncodeFromUTF8(in []byte) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	query := fmt.Sprintf("SELECT CAST(CONVERT(_utf8mb4 X'%x' USING %s) AS binary)", in, c.name)
	res, err := c.conn.ExecuteFetch(query, 1, false)
	if err != nil {
		return nil, err
	}
	if len(res.Rows) != 1 {
		return nil, fmt.Errorf("unexpected result from MySQL: %d rows returned", len(res.Rows))
	}
	return res.Rows[0][0].ToBytes(), nil
}
