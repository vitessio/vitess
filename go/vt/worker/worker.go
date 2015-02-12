// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/*
Package worker contains the framework, utility methods and core
functions for long running actions. 'vtworker' binary will use these.
*/
package worker

import (
	"html/template"

	"github.com/youtube/vitess/go/vt/topo"
)

// Worker is the base interface for all long running workers.
type Worker interface {
	// StatusAsHTML returns the current worker status in HTML
	StatusAsHTML() template.HTML

	// StatusAsText returns the current worker status in plain text
	StatusAsText() string

	// Run is the main entry point for the worker. It will be called
	// in a go routine.
	// When Cancel() is called, Run should exit as soon as possible.
	Run()

	// Cancel should attempt to force the Worker to exit as soon as possible.
	// Note that cleanup actions may still run after cancellation.
	Cancel()

	// Error returns the error status of the job, if any.
	// It will only be called after Run() has completed.
	Error() error
}

// Resolver is an interface that should be implemented by any workers that need to
// resolve the topology.
type Resolver interface {
	// ResolveDestinationMasters forces the worker to (re)resolve the topology and update
	// the destination masters that it knows about.
	ResolveDestinationMasters() error

	// GetDestinationMaster returns the most recently resolved destination master for a particular shard.
	GetDestinationMaster(shardName string) (*topo.TabletInfo, error)
}
