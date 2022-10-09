/*
Copyright 2022 The Vitess Authors.

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

// Package viperutil provides additional functions for Vitess's use of viper for
// managing configuration of various components.
package viperutil

import (
	"fmt"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

type Option[T any] func(val *Value[T])

func WithAliases[T any](aliases ...string) Option[T] {
	return func(val *Value[T]) {
		val.aliases = append(val.aliases, aliases...)
	}
}

func WithDefault[T any](t T) Option[T] {
	return func(val *Value[T]) {
		val.hasDefault, val.value = true, t
	}
}

func WithEnvVars[T any](envvars ...string) Option[T] {
	return func(val *Value[T]) {
		val.envvars = append(val.envvars, envvars...)
	}
}

func WithFlags[T any](flags ...string) Option[T] {
	return func(val *Value[T]) {
		val.flagNames = append(val.flagNames, flags...)
	}
}

type Value[T any] struct {
	key   string
	value T

	aliases    []string
	flagNames  []string
	envvars    []string
	hasDefault bool

	resolve func(string) T
	loaded  bool
}

// NewValue returns a viper-backed value with the given key, lookup function,
// and bind options.
func NewValue[T any](key string, getFunc func(string) T, opts ...Option[T]) *Value[T] {
	val := &Value[T]{
		key:     key,
		resolve: getFunc,
	}

	for _, opt := range opts {
		opt(val)
	}

	return val
}

// Bind binds the value to flags, aliases, and envvars, depending on the Options
// the value was created with.
//
// If the passed-in viper is nil, the global viper instance is used.
//
// If the passed-in flag set is nil, flag binding is skipped. Otherwise, if any
// flag names do not match the value's canonical key, they are registered as
// additional aliases for the value's key. This function panics if a flag name
// is specified that is not defined on the flag set.
func (val *Value[T]) Bind(v *viper.Viper, fs *pflag.FlagSet) {
	if v == nil {
		v = viper.GetViper()
	}

	aliases := val.aliases

	// Bind any flags, additionally aliasing flag names to the canonical key for
	// this value.
	if fs != nil {
		for _, name := range val.flagNames {
			f := fs.Lookup(name)
			if f == nil {
				// TODO: Don't love "configuration error" here as it might make
				// users think _they_ made a config error, but really it is us,
				// the Vitess authors, that have misconfigured a flag binding in
				// the source code somewhere.
				panic(fmt.Sprintf("configuration error: attempted to bind %s to unspecified flag %s", val.key, name))
			}

			if f.Name != val.key {
				aliases = append(aliases, f.Name)
			}

			// We are deliberately ignoring the error value here because it
			// only occurs when `f == nil` and we check for that ourselves.
			_ = v.BindPFlag(val.key, f)
		}
	}

	// Bind any aliases.
	for _, alias := range aliases {
		v.RegisterAlias(alias, val.key)
	}

	// Bind any envvars.
	if len(val.envvars) > 0 {
		vars := append([]string{val.key}, val.envvars...)
		// Again, ignoring the error value here, which is non-nil only when
		// `len(vars) == 0`.
		_ = v.BindEnv(vars...)
	}

	// Set default value.
	if val.hasDefault {
		v.SetDefault(val.key, val.value)
	}
}

// Fetch returns the underlying value from the backing viper.
func (val *Value[T]) Fetch() T {
	val.value, val.loaded = val.resolve(val.key), true
	return val.value
}

// Get returns the underlying value, loading from the backing viper only on
// first use. For dynamic reloading, use Fetch.
func (val *Value[T]) Get() T {
	if !val.loaded {
		val.Fetch()
	}

	return val.value
}

// Value returns the current underlying value. If a value has not been
// previously loaded (either via Fetch or Get), and the value was not created
// with a WithDefault option, the return value is whatever the zero value for
// the type T is.
func (val *Value[T]) Value() T {
	return val.value
}

/*
old api

func (v *Value[T]) BindFlag(f *pflag.Flag, aliases ...string) {
	BindFlagWithAliases(f, v.key, aliases...)
}

func (v *Value[T]) BindEnv(envvars ...string) {
	viper.BindEnv(append([]string{v.key}, envvars...)...)
}

func BindFlagWithAliases(f *pflag.Flag, canonicalKey string, aliases ...string) {
	viper.BindPFlag(canonicalKey, f)
	if canonicalKey != f.Name {
		aliases = append(aliases, f.Name)
	}

	for _, alias := range aliases {
		viper.RegisterAlias(alias, canonicalKey)
	}
}

*/
