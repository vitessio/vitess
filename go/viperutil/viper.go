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

/*
Package viperutil provides additional functions for Vitess's use of viper for
managing configuration of various components.

Example usage, without dynamic reload:

	var (
		tracingServer = viperutil.NewValue(
			"trace.service",
			viper.GetString,
			viperutil.WithFlags[string]("tracer"),
			viperutil.Withdefault("noop"),
		)
	)

	func init() {
		servenv.OnParse(func(fs *pflag.FlagSet) {
			fs.String("tracer", tracingServer.Value(), "tracing service to use")
			tracingServer.Bind(nil, fs)
		})
	}

	func StartTracing(serviceName string) io.Closer {
		backend := tracingServer.Get()
		factory, ok := tracingBackendFactories[backend]
		...
	}

Example usage, with dynamic reload:

	var (
		syncedViper = vipersync.New()
		syncedGetDuration = vipersync.AdaptGetter(syncedViper, func(v *viper.Viper) func(key string) time.Duration {
			return v.GetDuration
		})
		syncedGetInt = vipersync.AdaptGetter(syncedViper, func(v *viper.Viper) func(key string) int {
			return v.GetInt
		})

		// Convenience to remove need to repeat this module's prefix constantly.
		keyPrefix = viperutil.KeyPrefix("grpc.client")

		keepaliveTime = viperutil.NewValue(
			keyPrefix("keepalive.time"),
			syncedGetDuration,
			viperutil.WithFlags[time.Duration]("grpc_keepalive_time"),
			viperutil.WithDefault(10 * time.Second),
		)
		keepaliveTimeout = viperutil.NewValue(
			keyPrefix("keepalive.timeout"),
			syncedGetDuration,
			viperutil.WithFlags[time.Duration]("grpc_keepalive_timeout"),
			viperutil.WithDefault(10 * time.Second),
		)

		initialConnWindowSize = viperutil.NewValue(
			keyPrefix("initial_conn_window_size"),
			syncedGetInt,
			viperutil.WithFlags[int]("grpc_initial_conn_window_size"),
		)
	)

	func init() {
		binaries := []string{...}
		for _, cmd := range binaries {
			servenv.OnParseFor(cmd, func(fs *pflag.FlagSet) {

			})
		}

		servenv.OnConfigLoad(func() {
			// Watch the config file for changes to our synced variables.
			syncedViper.Watch(viper.ConfigFileUsed())
		})
	}

	func DialContext(ctx context.Context, target string, failFast FailFast, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
		...

		// Load our dynamic variables, thread-safely.
		kaTime := keepaliveTime.Fetch()
		kaTimeout := keepaliveTimeout.Fetch()
		if kaTime != 0 || kaTimeout != 0 {
			kp := keepalive.ClientParameters{
				Time: kaTime,
				Timeout: kaTimeout,
				PermitWithoutStream: true,
			}
			newopts = append(newopts, grpc.WithKeepaliveParams(kp))
		}

		if size := initialConnWindowSize.Fetch(); size != 0 {
			newopts = append(newopts, grpc.WithInitialConnWindowSize(int32(size)))
		}

		// and so on ...

		return grpc.DialContext(ctx, target, newopts...)
	}
*/
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
	bound   chan struct{}
	loaded  chan struct{}
}

// NewValue returns a viper-backed value with the given key, lookup function,
// and bind options.
func NewValue[T any](key string, getFunc func(string) T, opts ...Option[T]) *Value[T] {
	val := &Value[T]{
		key:     key,
		resolve: getFunc,
		bound:   make(chan struct{}, 1),
		loaded:  make(chan struct{}, 1),
	}

	for _, opt := range opts {
		opt(val)
	}

	val.bound <- struct{}{}
	val.loaded <- struct{}{}

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
//
// Bind is not safe to call concurrently with other calls to Bind, Fetch, Get,
// or Value. It is recommended to call Bind at least once before calling those
// other 3 methods; otherwise the default or zero value for T will be returned.
func (val *Value[T]) Bind(v *viper.Viper, fs *pflag.FlagSet) {
	select {
	case _, ok := <-val.bound:
		if ok {
			defer func() { val.bound <- struct{}{} }()
		}
	}

	val.bind(v, fs)
}

func (val *Value[T]) tryBind(v *viper.Viper, fs *pflag.FlagSet) {
	select {
	case _, ok := <-val.bound:
		if ok {
			val.bind(v, fs)
			close(val.bound)
		}
	}
}

func (val *Value[T]) bind(v *viper.Viper, fs *pflag.FlagSet) {
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
	// Prior to fetching anything, bind the value to the global viper if it
	// hasn't been bound to some viper already.
	val.tryBind(nil, nil)
	return val.resolve(val.key)
}

// Get returns the underlying value, loading from the backing viper only on
// first use. For dynamic reloading, use Fetch.
func (val *Value[T]) Get() T {
	val.tryBind(nil, nil)
	select {
	case _, ok := <-val.loaded:
		if ok {
			// We're the first to load; resolve the key and set the value.
			val.value = val.resolve(val.key)
			close(val.loaded)
		}
	}

	return val.value
}

// Value returns the current underlying value. If a value has not been
// previously loaded (either via Fetch or Get), and the value was not created
// with a WithDefault option, the return value is whatever the zero value for
// the type T is.
func (val *Value[T]) Value() (value T) {
	select {
	case _, ok := <-val.loaded:
		if ok {
			// No one has loaded yet, and no one is loading.
			// Return the value passed to us in the constructor, and
			// put the sentinel value back so another Get call can proceed
			// to actually load from the backing viper.
			value = val.value
			val.loaded <- struct{}{}
			return value
		}
	}

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
