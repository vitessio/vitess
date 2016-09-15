// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"errors"
	"sync"
)

// TxPreparedPool manages connections for prepared transactions.
// The Prepare functionality and associated orchestration
// is done by TxPool.
type TxPreparedPool struct {
	mu    sync.Mutex
	conns map[string]*preparedConn
}

type preparedConn struct {
	*DBConn
	// purpose will contain a non-empty purpose if the conn is in use.
	purpose string
}

// NewTxPreparedPool creates a new TxPreparedPool.
func NewTxPreparedPool() *TxPreparedPool {
	return &TxPreparedPool{conns: make(map[string]*preparedConn)}
}

// Register adds the connection as a prepared transaction.
func (pp *TxPreparedPool) Register(c *DBConn, dtid string) {
	pp.mu.Lock()
	defer pp.mu.Unlock()
	if _, ok := pp.conns[dtid]; ok {
		// This should never happen.
		panic("duplicate DTID in Prepare: " + dtid)
	}
	pp.conns[dtid] = &preparedConn{DBConn: c}
}

// Unregister forgets the connection associdated with the dtid.
// To avoid race conditions, you have to perform a Get before
// calling Unregister.
func (pp *TxPreparedPool) Unregister(dtid string) {
	pp.mu.Lock()
	defer pp.mu.Unlock()
	delete(pp.conns, dtid)
}

// Get locks and returns the requested conn. If it's already
// in use, it returns a "in use: purpose" error. The lock
// is released by a Put.
func (pp *TxPreparedPool) Get(dtid, purpose string) (*DBConn, error) {
	if purpose == "" {
		panic("empty purpose not allowed")
	}
	pp.mu.Lock()
	defer pp.mu.Unlock()
	pc, ok := pp.conns[dtid]
	if !ok {
		return nil, errors.New("not found")
	}
	if pc.purpose != "" {
		return nil, errors.New("in use: " + pc.purpose)
	}
	pc.purpose = purpose
	return pc.DBConn, nil
}

// Put unlocks the connection for someone else to use.
func (pp *TxPreparedPool) Put(dtid string) {
	pp.mu.Lock()
	defer pp.mu.Unlock()
	pc, ok := pp.conns[dtid]
	if !ok {
		// This should never happen.
		panic("DTID not found while trying to Put: " + dtid)
	}
	pc.purpose = ""
}
