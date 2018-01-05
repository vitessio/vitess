/*
Copyright 2017 Google Inc.

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

package dbconnpool

// PooledDBConnection re-exposes DBConnection to be used by ConnectionPool.
type PooledDBConnection struct {
	*DBConnection
	pool *ConnectionPool
}

// Recycle should be called to return the PooledDBConnection to the pool.
func (pc *PooledDBConnection) Recycle() {
	if pc.IsClosed() {
		pc.pool.Put(nil)
	} else {
		pc.pool.Put(pc)
	}
}

// Reconnect replaces the existing underlying connection with a new one,
// if possible. Recycle should still be called afterwards.
func (pc *PooledDBConnection) Reconnect() error {
	pc.DBConnection.Close()
	newConn, err := NewDBConnection(pc.pool.info, pc.mysqlStats)
	if err != nil {
		return err
	}
	pc.DBConnection = newConn
	return nil
}
