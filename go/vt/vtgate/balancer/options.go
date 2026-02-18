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

package balancer

// pickOptions configure a Pick call. pickOptions are set by the PickOption
// values passed to the Pick function.
type pickOptions struct {
	sessionUUID string
}

// PickOption configures how we perform the pick operation.
type PickOption interface {
	apply(*pickOptions)
}

// funcPickOption wraps a function that modifies pickOptions into an
// implementation of the PickOption interface.
type funcPickOption struct {
	f func(*pickOptions)
}

func (fpo *funcPickOption) apply(po *pickOptions) {
	fpo.f(po)
}

func newFuncPickOption(f func(*pickOptions)) *funcPickOption {
	return &funcPickOption{
		f: f,
	}
}

// WithSessionUUID allows you to specify the session UUID for the session balancer.
func WithSessionUUID(sessionUUID string) PickOption {
	return newFuncPickOption(func(o *pickOptions) {
		o.sessionUUID = sessionUUID
	})
}

// getOptions applies the given options to a new pickOptions struct
// and returns it.
func getOptions(opts []PickOption) *pickOptions {
	options := &pickOptions{}
	for _, opt := range opts {
		opt.apply(options)
	}

	return options
}
