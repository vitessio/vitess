/*
Copyright 2019 The Vitess Authors.

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

package tabletserver

import (
	"errors"
	"fmt"
	"sync"
)

var (
	errPrepCommitting = errors.New("committing")
	errPrepFailed     = errors.New("failed")
)

// TxPreparedPool manages connections for prepared transactions.
// The Prepare functionality and associated orchestration
// is done by TxPool.
type TxPreparedPool struct {
	mu       sync.Mutex
	conns    map[string]*TxConnection
	reserved map[string]error
	capacity int
}

// NewTxPreparedPool creates a new TxPreparedPool.
func NewTxPreparedPool(capacity int) *TxPreparedPool {
	if capacity < 0 {
		// If capacity is 0 all prepares will fail.
		capacity = 0
	}
	return &TxPreparedPool{
		conns:    make(map[string]*TxConnection, capacity),
		reserved: make(map[string]error),
		capacity: capacity,
	}
}

// Put adds the connection to the pool. It returns an error
// if the pool is full or on duplicate key.
func (pp *TxPreparedPool) Put(c *TxConnection, dtid string) error {
	pp.mu.Lock()
	defer pp.mu.Unlock()
	if _, ok := pp.reserved[dtid]; ok {
		return errors.New("duplicate DTID in Prepare: " + dtid)
	}
	if _, ok := pp.conns[dtid]; ok {
		return errors.New("duplicate DTID in Prepare: " + dtid)
	}
	if len(pp.conns) >= pp.capacity {
		return fmt.Errorf("prepared transactions exceeded limit: %d", pp.capacity)
	}
	pp.conns[dtid] = c
	return nil
}

// FetchForRollback returns the connection and removes it from the pool.
// If the connection is not found, it returns nil. If the dtid
// is in the reserved list, it means that an operator is trying
// to resolve a previously failed commit. So, it removes the entry
// and returns nil.
func (pp *TxPreparedPool) FetchForRollback(dtid string) *TxConnection {
	pp.mu.Lock()
	defer pp.mu.Unlock()
	if _, ok := pp.reserved[dtid]; ok {
		delete(pp.reserved, dtid)
		return nil
	}
	c := pp.conns[dtid]
	delete(pp.conns, dtid)
	return c
}

// FetchForCommit returns the connection for commit. Before returning,
// it remembers the dtid in its reserved list as "committing". If
// the dtid is already in the reserved list, it returns an error.
// If the commit is successful, the dtid can be removed from the
// reserved list by calling Forget. If the commit failed, SetFailed
// must be called. This will inform future retries that the previous
// commit failed.
func (pp *TxPreparedPool) FetchForCommit(dtid string) (*TxConnection, error) {
	pp.mu.Lock()
	defer pp.mu.Unlock()
	if err, ok := pp.reserved[dtid]; ok {
		return nil, err
	}
	c, ok := pp.conns[dtid]
	if ok {
		delete(pp.conns, dtid)
		pp.reserved[dtid] = errPrepCommitting
	}
	return c, nil
}

// SetFailed marks the reserved dtid as failed.
// If there was no previous entry, one is created.
func (pp *TxPreparedPool) SetFailed(dtid string) {
	pp.mu.Lock()
	defer pp.mu.Unlock()
	pp.reserved[dtid] = errPrepFailed
}

// Forget removes the dtid from the reserved list.
func (pp *TxPreparedPool) Forget(dtid string) {
	pp.mu.Lock()
	defer pp.mu.Unlock()
	delete(pp.reserved, dtid)
}

// FetchAll removes all connections and returns them as a list.
// It also forgets all reserved dtids.
func (pp *TxPreparedPool) FetchAll() []*TxConnection {
	pp.mu.Lock()
	defer pp.mu.Unlock()
	conns := make([]*TxConnection, 0, len(pp.conns))
	for _, c := range pp.conns {
		conns = append(conns, c)
	}
	pp.conns = make(map[string]*TxConnection, pp.capacity)
	pp.reserved = make(map[string]error)
	return conns
}
