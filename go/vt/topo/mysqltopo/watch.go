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
	"log/slog"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
)

// Watch is part of the topo.Conn interface.
//
// The watcher is registered with the notification system BEFORE the current
// value is read. Registering after the read would open a gap: a change
// committed and scanned between the read and the registration updates the
// notification system's global knownKeys, so it would never be delivered to
// this watcher — the caller would hold the pre-change value forever. The
// price of register-first is that a change landing in the gap can be
// observed twice (in the returned current value AND as a change event);
// topo.Watch consumers must tolerate redundant updates, which Vitess's do.
func (s *Server) Watch(ctx context.Context, filePath string) (current *topo.WatchData, changes <-chan *topo.WatchData, err error) {
	if err := s.checkClosed(); err != nil {
		return nil, nil, convertError(err, filePath)
	}

	fullPath := s.resolvePath(filePath)

	// Get the notification system - this is required for watches to work
	ns, err := s.getNotificationSystemForServer()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to initialize watch: %v", err)
	}

	// Create the watcher and register it before reading the current value.
	watchCtx, cancel := context.WithCancel(ctx)
	changesChan := make(chan *topo.WatchData, 10) // Buffered channel

	w := &watcher{
		path:    fullPath,
		changes: changesChan,
		ctx:     watchCtx,
		cancel:  cancel,
	}

	// Add to notification system. Registration is refused if the system
	// died between getNotificationSystemForServer and here — its
	// cancellation sweep has already run, so a watcher added now would
	// starve forever. Fail fast: the caller's retry goes back through
	// getNotificationSystemForServer, which replaces a dead system.
	if !ns.addWatcher(w) {
		cancel()
		return nil, nil, fmt.Errorf("failed to initialize watch for %s: notification system is dead; retry to get a fresh one", fullPath)
	}
	log.Info("MySQL topo: registered watch", slog.String("path", fullPath))

	// Get the current value
	data, version, err := s.Get(ctx, filePath)
	if err != nil {
		// If the file doesn't exist, return the error directly (not in
		// WatchData). The watcher was never handed to the caller, so tear
		// it down without sending anything on the channel.
		ns.removeWatcher(w)
		cancel()
		return nil, nil, err
	}

	current = &topo.WatchData{
		Contents: data,
		Version:  version,
	}

	// Start a goroutine to handle cleanup when context is cancelled
	go func() {
		<-watchCtx.Done()
		ns.removeWatcher(w)
		log.Info("MySQL topo: deregistered watch", slog.String("path", fullPath))

		// Check if this watcher was cancelled due to deletion
		wasDeleted := w.deleted.Load()

		// Only send interrupted error if not cancelled due to deletion
		if !wasDeleted {
			select {
			case changesChan <- &topo.WatchData{Err: topo.NewError(topo.Interrupted, fullPath)}:
			default:
			}
		}
		close(changesChan)
	}()

	return current, changesChan, nil
}

// WatchRecursive is part of the topo.Conn interface.
//
// Like Watch, the recursive watcher is registered BEFORE the current values
// are listed, so a change landing between the two is delivered as an event
// rather than lost to the notification system's version dedup. Consumers
// must tolerate a change appearing both in the returned snapshot and as an
// event.
func (s *Server) WatchRecursive(ctx context.Context, pathPrefix string) ([]*topo.WatchDataRecursive, <-chan *topo.WatchDataRecursive, error) {
	if err := s.checkClosed(); err != nil {
		return nil, nil, convertError(err, pathPrefix)
	}

	fullPathPrefix := s.resolvePath(pathPrefix)

	// Get the notification system - this is required for watches to work
	ns, err := s.getNotificationSystemForServer()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to initialize recursive watch: %v", err)
	}

	// Create the recursive watcher and register it before listing.
	watchCtx, cancel := context.WithCancel(ctx)
	changesChan := make(chan *topo.WatchDataRecursive, 10) // Buffered channel

	w := &recursiveWatcher{
		pathPrefix: fullPathPrefix,
		changes:    changesChan,
		ctx:        watchCtx,
		cancel:     cancel,
	}

	// Add to notification system. See Watch for why a refused registration
	// (dead system) must fail fast instead of silently starving.
	if !ns.addRecursiveWatcher(w) {
		cancel()
		return nil, nil, fmt.Errorf("failed to initialize recursive watch for %s: notification system is dead; retry to get a fresh one", fullPathPrefix)
	}
	log.Info("MySQL topo: registered recursive watch", slog.String("prefix", fullPathPrefix))

	// Get current values
	kvInfos, err := s.List(ctx, pathPrefix)
	if err != nil && !topo.IsErrType(err, topo.NoNode) {
		// Tear the watcher down without sending anything: it was never
		// handed to the caller.
		ns.removeRecursiveWatcher(w)
		cancel()
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

	// Start a goroutine to handle cleanup when context is cancelled
	go func() {
		<-watchCtx.Done()
		ns.removeRecursiveWatcher(w)
		log.Info("MySQL topo: deregistered recursive watch", slog.String("prefix", fullPathPrefix))

		// Send final interrupted error and close channel
		select {
		case changesChan <- &topo.WatchDataRecursive{
			Path:      fullPathPrefix,
			WatchData: topo.WatchData{Err: topo.NewError(topo.Interrupted, fullPathPrefix)},
		}:
		default:
		}
		close(changesChan)
	}()

	return current, changesChan, nil
}
