/*
Copyright 2026 The Vitess Authors.

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

package topo

import "time"

// SetCellInfoCacheTTL sets how long ts.ConnForCell reuses a cell's connection without re-reading CellInfo.
func (ts *Server) SetCellInfoCacheTTL(ttl time.Duration) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.cellInfoCacheTTL = ttl
}

// ExpireCellInfo makes the next ts.ConnForCell for cell re-read its CellInfo.
func (ts *Server) ExpireCellInfo(cell string) {
	ts.expireCellInfo(cell)
}

// CellReadConn returns the connection that conn, as returned by ConnForCell, sends reads to.
func CellReadConn(conn Conn) Conn {
	if c, ok := conn.(*cachedCellConn); ok {
		return c.readConn()
	}
	return conn
}
