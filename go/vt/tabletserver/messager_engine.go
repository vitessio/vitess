// Copyright 2017, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"sync"

	"github.com/youtube/vitess/go/vt/schema"
)

// MessagerEngine is the engine for handling messages.
type MessagerEngine struct {
	isOpen bool
	wg     sync.WaitGroup

	qe *QueryEngine

	mu        sync.Mutex
	msgTables []string
}

// NewMessagerEngine creates a new MessagerEngine.
func NewMessagerEngine(qe *QueryEngine) *MessagerEngine {
	return &MessagerEngine{
		qe: qe,
	}
}

// Open starts the MessagerEngine service.
func (me *MessagerEngine) Open() error {
	if me.isOpen {
		return nil
	}
	me.qe.schemaInfo.RegisterNotifier("messages", me.schemaChanged)
	me.isOpen = true
	return nil
}

// Close closes the MessagerEngine service.
func (me *MessagerEngine) Close() {
	if !me.isOpen {
		return
	}
	me.wg.Wait()
	me.qe.schemaInfo.UnregisterNotifier("messages")
	me.isOpen = false
}

func (me *MessagerEngine) schemaChanged(tables map[string]*schema.Table) {
	me.mu.Lock()
	defer me.mu.Unlock()
mainLoop:
	for name, t := range tables {
		if t.Type != schema.Message {
			continue
		}
		for _, meName := range me.msgTables {
			if meName == name {
				continue mainLoop
			}
		}
		me.msgTables = append(me.msgTables, name)
	}
}
