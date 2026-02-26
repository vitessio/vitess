/*
Copyright 2025 The Vitess Authors.

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
	"context"
	"fmt"
	"sync"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vterrors"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

type binlogDumpStream struct {
	cancel context.CancelFunc
}

var _ subComponent = (*BinlogDumpEngine)(nil)

// BinlogDumpEngine tracks active BinlogDump/BinlogDumpGTID streams
// and cancels them on shutdown. This follows the VStreamer Engine pattern.
//
// Open and Close must not be called concurrently. The state manager
// serializes all state transitions, so this is guaranteed in practice.
type BinlogDumpEngine struct {
	wg sync.WaitGroup
	mu sync.Mutex

	isOpen bool

	streamIdx int
	streams   map[int]*binlogDumpStream
}

// NewBinlogDumpEngine creates a new BinlogDumpEngine.
func NewBinlogDumpEngine() *BinlogDumpEngine {
	return &BinlogDumpEngine{
		streams: make(map[int]*binlogDumpStream),
	}
}

// Open marks the engine as open.
func (e *BinlogDumpEngine) Open() {
	e.mu.Lock()
	defer e.mu.Unlock()

	log.Info("BinlogDumpEngine: opening")
	e.isOpen = true
}

func (e *BinlogDumpEngine) IsOpen() bool {
	e.mu.Lock()
	defer e.mu.Unlock()

	return e.isOpen
}

// Close cancels all active streams and waits for them to finish.
func (e *BinlogDumpEngine) Close() {
	func() {
		e.mu.Lock()
		defer e.mu.Unlock()

		if !e.isOpen {
			return
		}
		e.isOpen = false

		for _, s := range e.streams {
			s.cancel()
		}
	}()

	// Wait only after releasing the lock because Unregister needs it.
	// This is safe because isOpen is already false, so no new Register
	// calls can succeed. Open must not be called concurrently with Close;
	// the state manager guarantees this.
	e.wg.Wait()
	log.Info("BinlogDumpEngine: closed")
}

// Register creates a derived context for a new stream and tracks it.
// The caller must call Unregister when the stream ends.
func (e *BinlogDumpEngine) Register(ctx context.Context) (context.Context, int, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if !e.isOpen {
		return nil, 0, vterrors.Errorf(vtrpcpb.Code_UNAVAILABLE, "BinlogDumpEngine is closed")
	}

	ctx, cancel := context.WithCancel(ctx)
	e.streamIdx++
	idx := e.streamIdx
	e.streams[idx] = &binlogDumpStream{cancel: cancel}
	e.wg.Add(1)

	log.Info(fmt.Sprintf("BinlogDumpEngine: registered stream %d", idx))
	return ctx, idx, nil
}

// Unregister removes a stream from tracking and signals completion.
func (e *BinlogDumpEngine) Unregister(idx int) {
	e.mu.Lock()
	defer e.mu.Unlock()

	delete(e.streams, idx)
	e.wg.Done()

	log.Info(fmt.Sprintf("BinlogDumpEngine: unregistered stream %d", idx))
}
