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

package mysqltopo

import (
	"context"
	"fmt"

	"vitess.io/vitess/go/vt/topo"
)

// Watch is part of the topo.Conn interface.
func (s *Server) Watch(ctx context.Context, filePath string) (current *topo.WatchData, changes <-chan *topo.WatchData, err error) {
	if err := s.checkClosed(); err != nil {
		return nil, nil, convertError(err, filePath)
	}

	fullPath := s.fullPath(filePath)

	// Get the current value
	data, version, err := s.Get(ctx, filePath)
	if err != nil {
		// If the file doesn't exist, return the error directly (not in WatchData)
		return nil, nil, err
	}

	current = &topo.WatchData{
		Contents: data,
		Version:  version,
	}

	// Get the notification system - this is required for watches to work
	ns, err := s.getNotificationSystemForServer()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to initialize watch: %v", err)
	}

	// Create the watcher
	watchCtx, cancel := context.WithCancel(ctx)
	changes_chan := make(chan *topo.WatchData, 10) // Buffered channel

	w := &watcher{
		path:    fullPath,
		changes: changes_chan,
		ctx:     watchCtx,
		cancel:  cancel,
	}

	// Add to notification system
	ns.addWatcher(w)

	// Start a goroutine to handle cleanup when context is cancelled
	go func() {
		<-watchCtx.Done()
		ns.removeWatcher(w)

		// Check if this watcher was cancelled due to deletion
		w.deletedMu.Lock()
		wasDeleted := w.deleted
		w.deletedMu.Unlock()

		// Only send interrupted error if not cancelled due to deletion
		if !wasDeleted {
			select {
			case changes_chan <- &topo.WatchData{Err: topo.NewError(topo.Interrupted, fullPath)}:
			default:
			}
		}
		close(changes_chan)
	}()

	return current, changes_chan, nil
}

// WatchRecursive is part of the topo.Conn interface.
func (s *Server) WatchRecursive(ctx context.Context, pathPrefix string) ([]*topo.WatchDataRecursive, <-chan *topo.WatchDataRecursive, error) {
	if err := s.checkClosed(); err != nil {
		return nil, nil, convertError(err, pathPrefix)
	}

	fullPathPrefix := s.fullPath(pathPrefix)

	// Get current values
	kvInfos, err := s.List(ctx, pathPrefix)
	if err != nil && !topo.IsErrType(err, topo.NoNode) {
		return nil, nil, err
	}

	var current []*topo.WatchDataRecursive
	for _, kvInfo := range kvInfos {
		current = append(current, &topo.WatchDataRecursive{
			Path: string(kvInfo.Key),
			WatchData: topo.WatchData{
				Contents: kvInfo.Value,
				Version:  kvInfo.Version,
			},
		})
	}

	// Get the notification system - this is required for watches to work
	ns, err := s.getNotificationSystemForServer()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to initialize recursive watch: %v", err)
	}

	// Create the recursive watcher
	watchCtx, cancel := context.WithCancel(ctx)
	changes_chan := make(chan *topo.WatchDataRecursive, 10) // Buffered channel

	w := &recursiveWatcher{
		pathPrefix: fullPathPrefix,
		changes:    changes_chan,
		ctx:        watchCtx,
		cancel:     cancel,
	}

	// Add to notification system
	ns.addRecursiveWatcher(w)

	// Start a goroutine to handle cleanup when context is cancelled
	go func() {
		<-watchCtx.Done()
		ns.removeRecursiveWatcher(w)

		// Send final interrupted error and close channel
		select {
		case changes_chan <- &topo.WatchDataRecursive{
			Path:      fullPathPrefix,
			WatchData: topo.WatchData{Err: topo.NewError(topo.Interrupted, fullPathPrefix)},
		}:
		default:
		}
		close(changes_chan)
	}()

	return current, changes_chan, nil
}
