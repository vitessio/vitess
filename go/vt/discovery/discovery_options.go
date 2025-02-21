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

// discoveryOptions configure a discovery components. discoveryOptions are set
// by the DiscoveryOption values passed to the component constructors.
type discoveryOptions struct {
	logger logutil.Logger
}

// DiscoveryOption configures how we perform certain operations.
type DiscoveryOption interface {
	apply(*discoveryOptions)
}

// funcDiscoveryOption wraps a function that modifies discoveryOptions into
// an implementation of the DiscoveryOption interface.
type funcDiscoveryOption struct {
	f func(*discoveryOptions)
}

func defaultOptions() discoveryOptions {
	return discoveryOptions{
		logger: logutil.NewConsoleLogger(),
	}
}

func withOptions(dos ...DiscoveryOption) discoveryOptions {
	os := defaultOptions()
	for _, do := range dos {
		do.apply(&os)
	}
	return os
}

func (fhco *funcDiscoveryOption) apply(dos *discoveryOptions) {
	fhco.f(dos)
}

func newFuncDiscoveryOption(f func(*discoveryOptions)) *funcDiscoveryOption {
	return &funcDiscoveryOption{
		f: f,
	}
}

// WithLogger accepts a custom logger to use in a discovery component. If this
// option is not provided then the default system logger will be used.
func WithLogger(l logutil.Logger) DiscoveryOption {
	return newFuncDiscoveryOption(func(o *discoveryOptions) {
		o.logger = l
	})
}
