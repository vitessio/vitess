/*
Copyright 2023 The Vitess Authors.

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

package viperutil

import (
	"github.com/spf13/pflag"

	"vitess.io/vitess/go/viperutil/internal/value"
)

var (
	_ Value[int] = (*value.Static[int])(nil)
	_ Value[int] = (*value.Dynamic[int])(nil)
)

// Value represents the public API to access viper-backed config values.
//
// N.B. the embedded value.Registerable interface is necessary only for
// BindFlags and other mechanisms of binding Values to the internal registries
// to work. Users of Value objects should only need to call Get(), Set(v T), and
// Default().
type Value[T any] interface {
	value.Registerable

	// Get returns the current value. For static implementations, this will
	// never change after the initial config load. For dynamic implementations,
	// this may change throughout the lifetime of the vitess process.
	Get() T
	// Set sets the underlying value. For both static and dynamic
	// implementations, this is reflected in subsequent calls to Get.
	//
	// If a config file was loaded, changes to dynamic values will be persisted
	// back to the config file in the background, governed by the behavior of
	// the --config-persistence-min-interval flag.
	Set(v T)
	// Default returns the default value configured for this Value. For both
	// static and dynamic implementations, it should never change.
	Default() T
}

// BindFlags binds a set of Registerable values to the given flag set.
//
// This function will panic if any of the values was configured to map to a flag
// which is not defined on the flag set. Therefore, this function should usually
// be called in an OnParse or OnParseFor hook after defining the flags for the
// values in question.
func BindFlags(fs *pflag.FlagSet, values ...value.Registerable) {
	value.BindFlags(fs, values...)
}
