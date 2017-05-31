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
