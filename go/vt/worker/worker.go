// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/*
'worker' package contains the framework, utility methods and core
functions for long running actions. 'vtworker' binary will use these.
*/
package worker

import ()

// Worker is the base interface for all long running workers.
type Worker interface {
	// StatusAsHTML returns the current worker status in HTML
	StatusAsHTML() string

	// StatusAsText returns the current worker status in plain text
	StatusAsText() string

	// Run is the main entry point for the worker. It will be called
	// in a go routine.
	// When the SignalInterrupt() is called, Run should exit as soon as
	// possible.
	Run()
}

// signal handling
var interrupted = make(chan struct{})

func SignalInterrupt() {
	close(interrupted)
}
