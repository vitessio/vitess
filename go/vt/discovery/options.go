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

package discovery

import (
	"vitess.io/vitess/go/vt/logutil"
)

// Options configure a discovery components. Options are set by the Option
// values passed to the component constructors.
type Options struct {
	logger logutil.Logger
}

// Option configures how we perform certain operations.
type Option interface {
	apply(*Options)
}

// funcOption wraps a function that modifies options into an implementation of
// the Option interface.
type funcOption struct {
	f func(*Options)
}

func defaultOptions() Options {
	return Options{
		logger: logutil.NewConsoleLogger(),
	}
}

func withOptions(dos ...Option) Options {
	os := defaultOptions()
	for _, do := range dos {
		do.apply(&os)
	}
	return os
}

func (fhco *funcOption) apply(dos *Options) {
	fhco.f(dos)
}

func newFuncOption(f func(*Options)) *funcOption {
	return &funcOption{
		f: f,
	}
}

// WithLogger accepts a custom logger to use in a discovery component. If this
// option is not provided then the default system logger will be used.
func WithLogger(l logutil.Logger) Option {
	return newFuncOption(func(o *Options) {
		o.logger = l
	})
}
