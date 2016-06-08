// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package throttler

// Module specifies the API for a Decision Module which can tell the throttler
// to dynamically increase or decrease the current rate limit.
type Module interface {
	// Start can be implemented to e.g. start own Go routines and initialize the
	// module.
	// rateUpdateChan must be used to notify the Throttler when the module's
	// maximum rate limit has changed and a subsequent MaxRate() call would return
	// a different value.
	Start(rateUpdateChan chan<- struct{})
	// Stop will free all resources and be called by Throttler.Close().
	Stop()

	// MaxRate returns the maximum allowed rate determined by the module.
	MaxRate() int64
}
